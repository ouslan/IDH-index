import pandas as pd
import zipfile
from tqdm import tqdm
from termcolor import colored
from IDH import IndexIDH
from urllib.request import urlretrieve
import os

def get_data(range_years):
        print((colored('Education Data', 'blue')).center(50))
        print('|------------------------------------|')
        # get data from ACS PUMS 5-year
        for year in range(int(range_years[0]), int(range_years[1])+1):
            if os.path.exists(f'data/raw/data_{year}_raw.csv') or os.path.exists(f'data/processed/edu_index.csv'):
                continue
            else:
                url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/csv_ppr.zip'
                file_name = f'data/raw/raw_{year}.zip'

            # progress bar
                print(colored(f'Downloading {year} data', 'yellow'))
                with tqdm(unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1]) as t:
                    urlretrieve(url, file_name, reporthook=lambda blocknum, blocksize, total: t.update(blocknum * blocksize - t.n))

            # unzip the file
                with zipfile.ZipFile(file_name, 'r') as zip_ref:
                    zip_ref.extractall('data/raw')

            # remove the zip file and pdf
                for file in os.listdir('data/raw/'):
                    if file.endswith('.pdf'):
                        os.remove(f'data/raw/{file}')
                    elif file.endswith('.zip'):
                        os.remove(f'data/raw/{file}')
                    elif file.endswith('.csv') and not file.startswith('data') or file.startswith('pnb'):
                        os.rename(f'data/raw/{file}', f'data/raw/data_{year}_raw.csv')
                    else:
                        continue
        IDH = IndexIDH()
        IDH.edu_index('data/raw/')
        # # remove the raw data
        # for file in os.listdir('data/raw/'):
        #     if file.endswith('_raw.csv'):
        #         os.remove(f'data/raw/{file}')
        #     else:
        #         continue

if __name__ == '__main__':
    get_data(['2012','2021'])