from urllib.request import urlretrieve
import world_bank_data as wb
import pandas as pd
import polars as pl
import zipfile
import os

class DataPull:

    def __init__(self, end_year:int=2022, debug:bool=False):
        self.debug = debug
        self.pull_data(data_path='csv_ppr.zip', end_year=end_year)
        self.pull_data(data_path='csv_hpr.zip', end_year=end_year)
        self.pull_gni_capita()
        self.pull_gni_constant()
        self.life_exp()

    def pull_data(self, data_path:str, end_year:int) -> None:
        for year in range(2012, end_year + 1):
            if os.path.exists(f'data/raw/data_{data_path[4:7]}_{year}_raw.csv'):
                continue
            else:
                url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/{data_path}'
                file_name = f'data/raw/raw_ppr_{year}.zip'
            try:
                urlretrieve(url, file_name)
            except:
                print("\033[0;31mERROR: \033[0m" + f"Could not download {year} data")
                continue
            if self.debug:
                print("\033[0;36mINFO: \033[0m" + f"Downloading {year} data")

            # unzip the file
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                zip_ref.extractall('data/raw/')

            # remove the zip file and pdf
            for file in os.listdir('data/raw/'):
                if file.endswith('.pdf'):
                    os.remove(f'data/raw/{file}')
                elif file.endswith('.zip'):
                    os.remove(f'data/raw/{file}')
                elif file.endswith('.csv') and not file.startswith('data'):
                    os.rename(f'data/raw/{file}', f'data/raw/data_{data_path[4:7]}_{year}_raw.csv')
                else:
                    continue

    def pull_gni_capita(self) -> None:
        
        if not os.path.exists('data/raw/gni_capita.parquet'):
            capita_df = pl.from_pandas(pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.CD', country='PR', simplify_index=True).reset_index()))
            capita_df = capita_df.rename({"NY.GNP.PCAP.PP.CD": "capita", "Year":"year"}).drop_nulls()
            capita_df = capita_df.with_columns(
                                               pl.col("year").cast(pl.Int64),
                                               pl.col("capita").cast(pl.Int64))
            capita_df.write_parquet('data/raw/gni_capita.parquet')
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"GNI capita data downloaded")
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File for GNI capita data already exists")

    def pull_gni_constant(self) -> None:

        if not os.path.exists('data/raw/gni_constant.parquet'):
            constant_df = pl.from_pandas(pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.KD', country='PR', simplify_index=True).reset_index()))
            constant_df = constant_df.rename({'NY.GNP.PCAP.PP.KD': 'constant', "Year":"year"})
            constant_df = constant_df.with_columns(pl.col("year").cast(pl.Int64))

            constant_df.write_parquet('data/raw/gni_constant.parquet')
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"GNI constant data downloaded")
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File for GNI constant data already exists")



    def life_exp(self) -> None:
        
        if not os.path.exists('data/raw/life_exp.parquet'):
            life_exp = pd.DataFrame(wb.get_series('SP.DYN.LE00.IN', country='PR', simplify_index=True))
            life_exp = pl.from_pandas(life_exp)    
            life_exp = life_exp.rename({"SP.DYN.LE00.IN":"life_exp"})
            life_exp.write_parquet('data/raw/life_exp.parquet')
            
            if self.debug:
                print("\033[0;32mINFO: \033[0m" + f"Life expectancy data downloaded")
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File for life expectancy data already exists")

if __name__ == '__main__':
    DataPull()
