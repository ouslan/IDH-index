from urllib.request import urlretrieve
import zipfile
import os
from tqdm import tqdm

# Download the file from `url` and save it locally under `file_name`: add progress bar for download
for year in range(2009, 2021):
    url = f'https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/csv_ppr.zip'
    file_name = f'Data/raw_{year}.zip'

    # progress bar
    with tqdm(unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1]) as t:
        urlretrieve(url, file_name, reporthook=lambda blocknum, blocksize, total: t.update(blocknum * blocksize - t.n))

    # unzip the file
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall('Data')

    # remove the zip file and pdf
    for file in os.listdir('Data'):
        if file.endswith('.pdf'):
            os.remove(f'Data/{file}')
        elif file.endswith('.zip'):
            os.remove(f'Data/{file}')
        elif file.endswith('.csv') and not file.startswith('data'):
            os.rename(f'Data/{file}', f'Data/data_{year}.csv')
        else:
            continue