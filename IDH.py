import pandas as pd
import numpy as np
from urllib.request import urlretrieve
from tqdm import tqdm
import zipfile
import os

class IDH:

    def get_data(self, range_years):
        # get data from ACS PUMS 5-year
        for year in range(int(range_years[0]), int(range_years[1])+1):
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

    def health_index(self):
        return self.data['Health'].mean()

    def income_index(self):
        return self.data['Income'].mean()
    
    def edu_index(self, year):
        data = f'Data/data_{year}.csv'
        self.df = pd.read_csv(data, low_memory=False)
        self.df = self.df[['AGEP', 'SCH', 'SCHL']]
        # calcualte the mean of years of schooling
        self.df2 = self.df[self.df['AGEP'] > 25].copy()
        self.df2['scholing'] =  self.df2['SCHL']
        self.df2.reset_index(inplace=True)
        self.df2['scholing'].replace({3:1, 4:2, 5:3, 6:4, 7:5, 8:6, 9:7, 10:8, 11:9, 
                                12:10, 13:11, 14:12, 15:13, 15:13, 16:13, 17:13, 
                                18:13.5, 19:14, 20:15, 21:17, 22:19, 23:19, 24:23}, inplace=True)
        self.df2['enroled'] = np.where(self.df2['scholing'] > 1, 1, 0)
        value1 = self.df2['scholing'].mean()

        # calculate the expected years of schooling
        self.df3 = self.df[self.df['AGEP'] < 25].copy()
        self.df3['enrolled'] = self.df3['SCH'].apply(lambda x: 1 if x > 1 else 0)
        self.df_age = self.df3.groupby(['AGEP'])[['AGEP','enrolled']].count()
        self.df_age['enrolled'] = self.df3.groupby(['AGEP'])['enrolled'].sum()
        self.df_age['enrollment_rate'] = self.df_age['enrolled'] / self.df_age['AGEP']
        self.df_age = self.df_age.rename (columns = {'AGEP': 'count'})
        self.df_age = self.df_age.reset_index()
        self.df_age.drop([0,1,2,3,4], inplace=True)
        value2 = self.df_age['enrollment_rate'].sum()

        # calculate index
        edu_index = (value1/15 + value2/18) / 2
        return edu_index       
    
    def idh_index(self):
        return self.data['IDH'].mean()

if __name__ == "__main__":
    idh = IDH()
    print('Education Index:', idh.edu_index(2009))