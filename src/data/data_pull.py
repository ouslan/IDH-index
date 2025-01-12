from sqlmodel import create_engine
from tqdm import tqdm
import pandas as pd
import polars as pl
import requests
import zipfile
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

    def pull_data(self, data_path: str, end_year: int) -> None:
        """
        Downloads and extracts data files from a specified URL for a range of years.

        Parameters
        ----------
        data_path : str
            The path to the zip file on the server.
        end_year : int
            The end year for data collection.

        Returns
        -------
        None
        """
        for year in range(2012, end_year + 1):
            csv_file_path = f'data/raw/data_{data_path[4:7]}_{year}_raw.csv'
            if os.path.exists(csv_file_path):
                continue
            url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/{data_path}'
            file_name = f'data/raw/raw_ppr_{year}.zip'
            try:
                urlretrieve(url, file_name)
            except URLError:
                print("\033[0;31mERROR: \033[0m" + f"Could not download {year} data")
                continue
            if self.debug:
                print("\033[0;36mINFO: \033[0m" + f"Downloading {year} data")

            # Unzip the file
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                zip_ref.extractall('data/raw/')

            # Remove the zip file and pdf
            for file in os.listdir('data/raw/'):
                if file.endswith('.pdf') or file.endswith('.zip'):
                    os.remove(f'data/raw/{file}')
                elif file.endswith('.csv') and not file.startswith('data'):
                    os.rename(f'data/raw/{file}', csv_file_path)
                else:
                    continue

    def pull_gni_capita(self) -> None:
        """
        Downloads and processes GNI per capita data and saves it as a Parquet file.

        Returns
        -------
        None
        """
        if not os.path.exists('data/raw/gni_capita.parquet'):
            capita_df = pl.from_pandas(
                pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.CD', country='PR', simplify_index=True).reset_index())
            )
            capita_df = capita_df.rename({"NY.GNP.PCAP.PP.CD": "capita", "Year": "year"}).drop_nulls()
            capita_df = capita_df.with_columns(
                pl.col("year").cast(pl.Int64),
                pl.col("capita").cast(pl.Int64)
            )
            capita_df.write_parquet('data/raw/gni_capita.parquet')
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"GNI capita data downloaded")
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File for GNI capita data already exists")

    def pull_gni_constant(self) -> None:
        """
        Downloads and processes GNI constant data and saves it as a Parquet file.

        Returns
        -------
        None
        """
        if not os.path.exists('data/raw/gni_constant.parquet'):
            constant_df = pl.from_pandas(
                pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.KD', country='PR', simplify_index=True).reset_index())
            )
            constant_df = constant_df.rename({'NY.GNP.PCAP.PP.KD': 'constant', "Year": "year"})
            constant_df = constant_df.with_columns(pl.col("year").cast(pl.Int64))

            constant_df.write_parquet('data/raw/gni_constant.parquet')
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"GNI constant data downloaded")
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File for GNI constant data already exists")

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

if __name__ == '__main__':
    DataPull(end_year=2023, debug=True)
