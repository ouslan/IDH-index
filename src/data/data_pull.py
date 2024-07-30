from urllib.request import urlretrieve
import zipfile
import os

class DataPull:

    def __init__(self, end_year:int=2022, debug:bool=False):
        self.debug = debug
        self.get_data(data_path='csv_ppr.zip', end_year=end_year, debug=self.debug)
        self.get_data(data_path='csv_hpr.zip', end_year=end_year, debug=self.debug)

    def get_data(self, data_path:str, end_year:int, debug:bool):
        for year in range(2012, end_year + 1):
            if os.path.exists(f'data/raw/data_{data_path[4:7]}_{year}_raw.csv') or os.path.exists('data/processed/edu_index.csv'):
                continue
            else:
                url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/{data_path}'
                file_name = f'data/raw/raw_ppr_{year}.zip'
            try:
                urlretrieve(url, file_name)
            except:
                print(f'Error: File for {year} not found')
                continue
            if debug:
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

if __name__ == '__main__':
    DataPull()
