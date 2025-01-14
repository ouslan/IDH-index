from sqlmodel import create_engine
from json import JSONDecodeError
from datetime import datetime
import world_bank_data as wb
from tqdm import tqdm
import pandas as pd
import polars as pl
import requests
import ibis
import os

class DataPull:
    """
    A class to pull and process data from various sources and save it to local files.

    Parameters
    ----------
    end_year : int
        The end year for data collection.
    debug : bool, optional
        If True, enables debug messages. The default is False.
    """

    def __init__(self, database_url:str='sqlite:///db.sqlite', saving_dir:str='data/',
                        update:bool=False, debug:bool=False, dev:bool=False) -> None:
        """
        Parameters
        ----------
        saving_dir : str
            The directory where the data will be saved.
        Returns
        -------
        None
        """

        self.database_url = database_url
        self.engine = create_engine(self.database_url)
        self.saving_dir = saving_dir
        self.debug = debug
        self.dev = dev
        self.update = update

        if self.database_url.startswith("sqlite"):
            self.conn = ibis.sqlite.connect(self.database_url.replace("sqlite:///", ""))
        elif self.database_url.startswith("postgres"):
            self.conn = ibis.postgres.connect(
                user=self.database_url.split("://")[1].split(":")[0],
                password=self.database_url.split("://")[1].split(":")[1].split("@")[0],
                host=self.database_url.split("://")[1].split(":")[1].split("@")[1],
                port=self.database_url.split("://")[1].split(":")[2].split("/")[0],
                database=self.database_url.split("://")[1].split(":")[2].split("/")[1])
        else:
            raise Exception("Database url is not supported")

        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

    def pull_acs(self) -> ibis.expr.types.relations.Table:
        df = self.conn.table("pumstable")
        for _year in range(2012, 2024):
            if df.filter(df.year == _year).to_pandas().empty and _year != 2020:
                try:
                    self.conn.insert("pumstable", self.pull_request(params=['AGEP', 'SCH', 'SCHL', 'HINCP', 'PWGTP'], year=_year))
                except JSONDecodeError:
                    print("\033[0;36mNOTICE: \033[0m" + f"The ACS for {_year} is not availabel")
                    continue
            else:
                continue
        return self.conn.table("pumstable")

    def pull_request(self, params:list, year:int) -> pl.DataFrame:
        param = ",".join(params)
        base = "https://api.census.gov/data/"
        flow = "/acs/acs1/pumspr"
        url = f'{base}{year}{flow}?get={param}'
        r = requests.get(url).json()  
        df = pl.DataFrame(r)
        names = df.select(pl.col("column_0")).transpose()
        names = names.to_dicts().pop()
        names = dict((k, v.lower()) for k,v in names.items())
        df = df.drop("column_0").transpose()
        return df.rename(names).with_columns(year=pl.lit(year)).cast(pl.Int64)

    def pull_wb(self) -> ibis.expr.types.relations.Table:
        # Get the list of years in db
        df = self.conn.table("gnitable")
        list_year = df[["year"]].distinct().to_polars().to_numpy().transpose()[0]

        # Pull data from the WB that are not in the database
        for _year in range(2012, datetime.now().year):
            if not _year in list_year:
                capita_df = wb.get_series('NY.GNP.PCAP.PP.CD', country='PR', simplify_index=True, date="2028")
                constant_df = wb.get_series('NY.GNP.PCAP.PP.KD', country='PR', simplify_index=True, date="2028")

                capita_df = pl.from_pandas(capita_df.reset_index())
                constant_df = pl.from_pandas(constant_df.reset_index())

                capita_df = capita_df.rename({"NY.GNP.PCAP.PP.CD": "capita", "Year": "year"})
                constant_df = constant_df.rename({"NY.GNP.PCAP.PP.KD": "constant", "Year": "year"})

                capita_df = capita_df.with_columns(pl.col("year").cast(pl.Int64))
                constant_df = constant_df.with_columns(pl.col("year").cast(pl.Int64))

                df = capita_df.join(constant_df, on="year", how="inner", validate="1:1")
                self.conn.insert("gnitable", df)

                # Logging
                self.debug_log(message=f"inserted wb data for {_year}")

            else:
                self.debug_log(message="Data is in the database")
                continue
        return self.conn.table("gnitable")

    def life_exp(self) -> None:
        """
        Downloads and processes life expectancy data and saves it as a Parquet file.

        Returns
        -------
        None
        """
        if not os.path.exists('data/raw/life_exp.parquet'):
            life_exp = pl.from_pandas(
                pd.DataFrame(wb.get_series('SP.DYN.LE00.IN', country='PR', simplify_index=True).reset_index())
            )
            life_exp = life_exp.rename({"SP.DYN.LE00.IN": "life_exp", "Year": "year"})
            life_exp = life_exp.with_columns(pl.col("year").cast(pl.Int64))
            life_exp.write_parquet('data/raw/life_exp.parquet')

            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"Life expectancy data downloaded")
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File for life expectancy data already exists")


    def pull_file(self, url:str, filename:str, verify:bool=True) -> None:
        """
        Pulls a file from a URL and saves it in the filename. Used by the class to pull external files.

        Parameters
        ----------
        url: str
            The URL to pull the file from.
        filename: str
            The filename to save the file to.
        verify: bool
            If True, verifies the SSL certificate. If False, does not verify the SSL certificate.

        Returns
        -------
        None
        """
        chunk_size = 10 * 1024 * 1024

        with requests.get(url, stream=True, verify=verify) as response:
            total_size = int(response.headers.get('content-length', 0))

            with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024, desc='Downloading') as bar:
                with open(filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))  # Update the progress bar with the size of the chunk

    def debug_log(self, message:str) -> None:
        if self.debug:
            print(f"\033[0;36mINFO: \033[0m {message}")

