import pandas as pd
import zipfile
from tqdm import tqdm
from src.data.IDH import IndexIDH
import requests 
from urllib.request import urlretrieve
from yaspin import yaspin
import os
from rich import print
from rich.console import Console

console = Console()
def get_data(range_years, data_file):
    
    for year in range(int(range_years[0]), int(range_years[1])+1):
        spinner = yaspin(text='Downloading PPR data from the Census Bureau...', color='blue', spinner='dots')
        if os.path.exists(f'data/raw/data_{data_file[4:7]}_{year}_raw.csv') or os.path.exists(f'data/processed/edu_index.csv'):
            continue
        else:
            url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/{data_file}'
            file_name = f'data/raw/raw_ppr_{year}.zip'
            
            # Download progress bar
            spinner = yaspin(text='Downloading PPR data from the Census Bureau...', color='blue', spinner='dots')
            urlretrieve(url, file_name)

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
                    os.rename(f'data/raw/{file}', f'data/raw/data_{data_file[4:7]}_{year}_raw.csv')
                else:
                    continue

       

def calculate(range_years):
    spinner = yaspin(text='Downloading PPR data from the Census Bureau...', color='blue', spinner='dots')
    spinner.start()
    get_data(range_years, data_file='csv_ppr.zip')
    spinner.stop()
    spinner = yaspin(text='Downloading HPR data from the Census Bureau...', color='green', spinner='dots')
    spinner.start()
    get_data(range_years, data_file='csv_hpr.zip')
    spinner.stop()
    spinner = yaspin(text='Calculating IDH...', color='red', spinner='dots')
    spinner.start()
    IDH = IndexIDH()
    IDH.edu_index('data/raw/')
    IDH.income_index()
    IDH.health_index()
    IDH.idh_index()
    spinner.stop()
    
    # for file in os.listdir('data/raw/'):
    #     if file.endswith('raw.csv'):
    #         os.remove(f'data/raw/{file}')
    #     else:
    #         continue

if __name__ == '__main__':
    get_data(['2012','2021'])