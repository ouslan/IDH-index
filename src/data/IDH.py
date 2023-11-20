import pandas as pd
import numpy as np
import world_bank_data as wb
import os

class IndexIDH:

    def health_index(self, debug=False):
        # get Life expectancy at birth, total (years) for Puerto Rico
        pr_health = pd.DataFrame(wb.get_series('SP.DYN.LE00.IN', country='PR', simplify_index=True))
        pr_health.reset_index(inplace=True)
        pr_health.rename(columns={'SP.DYN.LE00.IN': 'index'}, inplace=True)
        pr_health['index'] = pr_health['index'].apply(lambda x: (x-20)/(85-20))
        pr_health['index'] = pr_health['index'].astype(float)
        pr_health['Year'] = pr_health['Year'].astype(int)
        # save in csv
        pr_health.to_csv('data/processed/health_index.csv', index=False)

    def income_index(self, debug=False):
        # get atlas df from WB
        atlas_df = pd.DataFrame(wb.get_series('NY.GNP.PCAP.CD', country='PR', simplify_index=True))
        atlas_df.reset_index(inplace=True)
        atlas_df.rename(columns={'NY.GNP.PCAP.CD': 'atlas'}, inplace=True)
        atlas_df['atlas'] = atlas_df['atlas'].astype(float)

        # get gni constant df from WB
        gni_df = pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.KD', country='PR', simplify_index=True))
        gni_df.reset_index(inplace=True)
        gni_df.rename(columns={'NY.GNP.PCAP.PP.KD': 'gni'}, inplace=True)
        gni_df['gni'] = gni_df['gni'].astype(float)

        # merge the two dataframes
        inc_df = atlas_df.merge(gni_df, on='Year')
        inc_df['income_ratio'] = inc_df['gni'] / inc_df['atlas']
        inc_df['income_ratio'] = inc_df['income_ratio'].astype(float)
        inc_df['Year'] = inc_df['Year'].astype(int)

        # merge the income index with the pnb.csv file
        pnb = pd.read_csv('pnb.csv')
        merge_df = inc_df.merge(pnb, on='Year', how='left')
        merge_df = merge_df.dropna()
        merge_df.reset_index(inplace=True)
        merge_df.drop(['index'], axis=1, inplace=True)
        merge_df['index_temp'] = merge_df['income_ratio'] * merge_df['pnb']
        merge_df['index'] = (np.log(merge_df['index_temp']) - np.log(100)) / (np.log(70000)-np.log(100))
        merge_df = merge_df[['Year', 'index']]
        merge_df.to_csv('Data/income_index.csv', index=False)
    
    def edu_index(self, folder_path='data/raw/', debug=False):

        self.df = pd.read_csv(folder_path + file, low_memory=False)
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
        edu_value = (value1/15 + value2/18) / 2
        year = file.split('_')[1]
        edu_index = pd.concat([edu_index, pd.DataFrame([[year, edu_value]], columns=['Year', 'edu_index'])])
        edu_index = edu_index.sort_values(by='Year', ascending=True)
        edu_index.to_csv('data/processed/edu_index.csv', index=False)
 
    def idh_index(self, debug=False):

        # get & calculate the health index
        health = pd.read_csv('data/processed/health_index.csv')
        health.rename(columns={'index': 'health_index'}, inplace=True)
        income = pd.read_csv('data/processed/income_index.csv')
        income.rename(columns={'index': 'income_index'}, inplace=True)
        edu = pd.read_csv('data/processed/edu_index.csv')
        edu.rename(columns={'index': 'edu_index'}, inplace=True)
        
        # calculate the index & save in csv
        df = health.merge(income, on='Year', how='left')
        df = df.merge(edu, on='Year', how='left')
        df['index'] = (df['health_index'] * df['income_index'] * df['edu_index']) ** (1/3)
        df.drop(['enrollment_mean', 'schooling_mean'], axis=1, inplace=True)
        df.dropna(inplace=True)
        df.to_csv('data/processed/idh_index.csv', index=False)

if __name__ == "__main__":
    # # generate csv file for 2009-2020 for the education index
    idh = IndexIDH()
    print(idh.health_index(2019))