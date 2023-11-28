import pandas as pd
import zipfile
from tqdm import tqdm
from src.data.IDH import IndexIDH
import requests 
import os
from rich import print
from rich.console import Console

console = Console()
def get_data(range_years, data_file):
    console.log("Downloading PPR data from the Census Bureau...", style='bold cyan')
    for year in range(int(range_years[0]), int(range_years[1])+1):
        if os.path.exists(f'data/raw/data_{data_file[4:7]}_{year}_raw.csv') or os.path.exists(f'data/processed/edu_index.csv'):
            continue
        else:
            url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/{data_file}'
            file_name = f'data/raw/raw_ppr_{year}.zip'

            # Download progress bar
            r = requests.get(url, stream=True)
            total_size = int(r.headers.get('content-length'))
            block_size = 1024
            progress = tqdm(unit='iB', total=total_size, unit_scale=True, unit_divisor=1024, desc=f'Downloading {year}_{data_file[4:7]} data')
            with open(file_name, 'wb') as f:
                for data in r.iter_content(block_size):
                    progress.update(len(data))
                    f.write(data)
            progress.close()

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
        console.log(f'Done downloading {year} data', style='bold cyan')

       

def calculate(range_years):
    get_data(range_years, data_file='csv_ppr.zip')
    get_data(range_years, data_file='csv_hpr.zip')
    console.log("Calculating all indexes...", style='bold cyan')
    IDH = IndexIDH()
    IDH.edu_index('data/raw/')
    IDH.income_index()
    IDH.health_index()
    IDH.idh_index()
    
    for file in os.listdir('data/raw/'):
        if file.endswith('raw.csv'):
            os.remove(f'data/raw/{file}')
        else:
            continue

if __name__ == '__main__':
    get_data(['2012','2021'])