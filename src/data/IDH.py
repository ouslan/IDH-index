import pandas as pd
import numpy as np
from scipy.stats import gmean
import world_bank_data as wb
import os

class IndexIDH:

    def health_index(self, debug=False):
        """
        Calculate the health index for PR

        Parameters
        ----------
        debug : <bool>, optional
            If True, returns the dataframe of the health index and does not save to csv. The default is False.
        
        Returns
        -------
        pd.DataFrame
            The dataframe of the health index.
        """
        # get the health index
        pr_health = pd.DataFrame(wb.get_series('SP.DYN.LE00.IN', country='PR', simplify_index=True))
        pr_health.reset_index(inplace=True)
        pr_health.rename(columns={'SP.DYN.LE00.IN': 'index'}, inplace=True)
        pr_health['index'] = pr_health['index'].apply(lambda x: (x-20)/(85-20))
        pr_health['index'] = pr_health['index'].astype(float)
        pr_health['Year'] = pr_health['Year'].astype(int)
        pr_health['health_index_ajusted'] = pr_health['index'] * (1-0.08)
        pr_health['atkinson'] = 0.08
        
        if debug:
            return pr_health
        else:
            pr_health.to_csv('data/processed/health_index.csv', index=False)

    def income_index(self, debug=False):
        """
        Calculate the income index for PR

        Parameters
        ----------
        debug : <bool>, optional
            If True, returns the dataframe of the income index and does not save to csv. The default is False.
        
        Returns
        -------
        pd.DataFrame
            The dataframe of the income index.
        """
        
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

        # ajust the index
        ajusted_df = pd.DataFrame([], columns=['Year', 'coef', 'atkinson'])
        for file in os.listdir('data/raw/'):
            if file.startswith('data_hpr'):
                ajust_df = pd.read_csv('data/raw/' + file, low_memory=False)
                ajust_df = ajust_df['HINCP'] # use HINCS
                ajust_df = ajust_df.sort_values(ascending=True)
                ajust_df = ajust_df[ajust_df > 0]
                ajust_df = ajust_df.dropna()
                bottom_5 = ajust_df[ajust_df <= ajust_df.quantile(0.005)]
                bottom_max = bottom_5.max()
                # replace bottom 5% with the max value
                ajust_df = ajust_df.apply(lambda x: x if x > bottom_max else bottom_max)
                # remove top 0.5%
                ajust_df = ajust_df[ajust_df <= ajust_df.quantile(0.995)]
                ajust_df = ajust_df.dropna()
                # get coefficient of ajustment
                coef, amean, gemetric, atkinson = self.adjust(ajust_df)
                ajusted_df = pd.concat([
                    ajusted_df if not ajusted_df.empty else None,
                    pd.DataFrame([[int(file.split('_')[2]), coef, atkinson]], columns=['Year', 'coef', 'atkinson'])])
            else:
                continue
        # merge the two dataframes
        inc_df = atlas_df.merge(gni_df, on='Year')
        inc_df['income_ratio'] = inc_df['gni'] / inc_df['atlas']
        inc_df['income_ratio'] = inc_df['income_ratio'].astype(float)
        inc_df['Year'] = inc_df['Year'].astype(int)

        # merge the income index with the pnb.csv file
        pnb = pd.read_csv('data/external/pnb.csv')
        merge_df = inc_df.merge(pnb, on='Year', how='left')
        merge_df = merge_df.dropna()
        merge_df.reset_index(inplace=True)
        merge_df.drop(['index'], axis=1, inplace=True)
        
        # calculate the index
        merge_df['index_temp'] = merge_df['income_ratio'] * merge_df['pnb']
        merge_df['index'] = (np.log(merge_df['index_temp']) - np.log(100)) / (np.log(70000)-np.log(100))
        merge_df = merge_df[['Year', 'index']]
        merge_df = merge_df.sort_values(by='Year', ascending=True)
        merge_df = merge_df.merge(ajusted_df, on='Year', how='left')
        merge_df['income_index_ajusted'] = merge_df['coef'] * merge_df['index']
        merge_df.drop(['coef'], axis=1, inplace=True)
        
        if debug:
            return merge_df
        else:
            merge_df.to_csv('data/processed/income_index.csv', index=False)
    
    def edu_index(self, folder_path='data/raw/', debug=False):
        """
        Calculate the edu index

        Parameters
        ----------
        folder_path : <str>
            path to the folder where the data is stored. Assumes the data is formatted as data_ppr_YYYY.csv
        debug : <bool>, optional
            debug mode, if True, the function will return the dataframe with the index. The default is False.

        Returns
        -------
        pd.DataFrame
            dataframe with the index of the edu index
        """

        # create the dataframe and loop through the files
        edu_index = pd.DataFrame([],columns=['Year', 'edu_index', 'edu_index_ajusted'])
        for file in os.listdir(folder_path):
            if file.startswith('data_ppr'):
                df = pd.read_csv(folder_path + file, low_memory=False)
                df = df[['AGEP', 'SCH', 'SCHL']]
                
                # calcualte the mean of years of schooling
                edu_sch = df[df['AGEP'] >= 25].copy()
                edu_sch['scholing'] =  edu_sch['SCHL']
                edu_sch.reset_index(inplace=True)
                edu_sch['scholing'].replace({3:1, 4:2, 5:3, 6:4, 7:5, 8:6, 9:7, 10:8, 11:9, 
                                        12:10, 13:11, 14:12, 15:13, 15:13, 16:13, 17:13, 
                                        18:13.5, 19:14, 20:15, 21:17, 22:19, 23:19, 24:23}, inplace=True)
                edu_sch['enroled'] = np.where(edu_sch['scholing'] > 1, 1, 0)
                mean_sch = edu_sch['scholing'].mean()

                # get coeficient of ajustment
                edu_sch['no_zero_schooling'] = 1 + edu_sch['scholing']
                coef, amean, gemetric, atkinson = self.adjust(edu_sch['no_zero_schooling'])

                # calculate the expected years of schooling
                edu_exp = df[df['AGEP'] < 25].copy()
                edu_exp['enrolled'] = edu_exp['SCH'].apply(lambda x: 1 if x > 1 else 0)
                edu_age = edu_exp.groupby(['AGEP'])[['AGEP','enrolled']].count()
                edu_age['enrolled'] = edu_exp.groupby(['AGEP'])['enrolled'].sum()
                edu_age['enrollment_rate'] = edu_age['enrolled'] / edu_age['AGEP']
                edu_age = edu_age.rename (columns = {'AGEP': 'count'})
                edu_age = edu_age.reset_index()
                edu_age.drop([0,1,2,3,4], inplace=True)
                exp_sch = edu_age['enrollment_rate'].sum()

                # calculate index
                edu_value = (mean_sch/15 + exp_sch/18) / 2
                edu_value_ajusted = coef * edu_value
                year = file.split('_')[2]
                edu_index = pd.concat([
                    edu_index if not edu_index.empty else None,
                    pd.DataFrame([[year, edu_value, edu_value_ajusted, atkinson ]], columns=['Year', 'edu_index', 'edu_index_ajusted', 'atkinson'])])
                edu_index = edu_index.sort_values(by='Year', ascending=True)
            else:
                continue

        if debug:
            return edu_index    
        else:
            edu_index.to_csv('data/processed/edu_index.csv', index=False)
 
    def idh_index(self, debug=False):
        """
        Calculate the idh index

        Parameters
        ----------
        debug : <bool>, optional
            debug mode, if True, the function will return the dataframe with the index. The default is False.

        Returns
        -------
        pd.DataFrame
            dataframe with the index of the idh index
        """

        # get & calculate the health index
        health = pd.read_csv('data/processed/health_index.csv')
        health.rename(columns={'index': 'health_index'}, inplace=True)
        health = health[['Year', 'health_index', 'health_index_ajusted']]
        income = pd.read_csv('data/processed/income_index.csv')
        income.rename(columns={'index': 'income_index'}, inplace=True)
        income = income[['Year', 'income_index', 'income_index_ajusted']]
        edu = pd.read_csv('data/processed/edu_index.csv')
        edu.rename(columns={'index': 'edu_index'}, inplace=True)
        edu = edu[['Year', 'edu_index', 'edu_index_ajusted']]
        
        # calculate the index & save in csv
        df = health.merge(income, on='Year', how='left')
        df = df.merge(edu, on='Year', how='left')
        df['index'] = (df['health_index'] * df['income_index'] * df['edu_index']) ** (1/3)
        df['index_ajusted'] = (df['health_index_ajusted'] * df['income_index_ajusted'] * df['edu_index_ajusted']) ** (1/3)
        df.dropna(inplace=True)
        if debug:
            return df
        else:
            df.to_csv('data/processed/idh_index.csv', index=False)

    def adjust(self, df):
        """
        Function for calculating the adjustment coefficient using the atkinson's method

        Parameters
        ----------
        df : <pd.DataFrame>
            dataframe with the index of the idh index

        Returns
        -------
        <float>
            coefficient of the adjustment
        <float>
            mean of the index
        <float>
            geometric mean of the index
        <float>
            atkinson's coefficient of the index
        """
        gemetric = gmean(df)
        amean = df.mean()
        atkinson = 1 - gemetric/amean
        coef = 1 - atkinson
        return coef, amean, gemetric, atkinson

if __name__ == "__main__":
    # # generate csv file for 2009-2020 for the education index
    idh = IndexIDH()
    print(idh.health_index(2019))