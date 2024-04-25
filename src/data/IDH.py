import pandas as pd
import polars as pl
import numpy as np
from scipy.stats import gmean
import world_bank_data as wb
import os
import wbpy

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
        df = pd.DataFrame(wb.get_series('SP.DYN.LE00.IN', country='PR', simplify_index=True))
        df = pl.from_pandas(df.reset_index())
        
        df = df.rename({"SP.DYN.LE00.IN":"life_exp"}).fill_null(strategy="forward")
        df = df.with_columns(
            pl.col("Year").cast(pl.Int32),
            ((pl.col("life_exp") - 20) / (85-20)).alias("health_index"),
            (((pl.col("life_exp") - 20) / (85-20)) * (1-0.08)).alias("health_index_adjusted"),
            arkinson=0.08
            )
        df = df.with_columns(
            (pl.col("health_index").pct_change() * 100).alias("health_index_pct_change"),
            (pl.col("health_index_adjusted").pct_change() * 100).alias("health_index_adjusted_pct_change")
            )
        df
        
        if debug:
            return df
        else:
            # round to 2 decimal places
            # pr_health = pr_health.round(2)
            df.write_csv('data/processed/health_index.csv')

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
        
        # get atlas df from WB (change names)
        atlas_df = pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.CD', country='PR', simplify_index=True))
        atlas_df.reset_index(inplace=True)
        atlas_df.rename(columns={'NY.GNP.PCAP.PP.CD': 'atlas'}, inplace=True)
        atlas_df['atlas'] = atlas_df['atlas'].astype(float)

        # get gni constant df from WB
        gni_df = pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.KD', country='PR', simplify_index=True))
        gni_df.reset_index(inplace=True)
        gni_df.rename(columns={'NY.GNP.PCAP.PP.KD': 'gni'}, inplace=True)
        gni_df['gni'] = gni_df['gni'].astype(float)
        # replace value 20

        # ajust the index
        ajusted_df = pd.DataFrame([], columns=['Year', 'coef', 'atkinson'])
        for file in os.listdir('data/raw/'):
            if file.startswith('data_hpr'):
                ajust_df = pd.read_csv('data/raw/' + file, engine="pyarrow")
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
                ajusted_df = pd.concat([ajusted_df if not ajusted_df.empty else None,
                                        pd.DataFrame([[int(file.split('_')[2]), coef, atkinson]], 
                                                        columns=['Year', 'coef', 'atkinson'])])
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
        #merge_df['index_temp'] =  merge_df['pnb']
        # replace the value of the year 2021 with 0
        # merge_df.loc[merge_df['Year'] == 2021, 'index_temp'] = 22342.18055
        merge_df['index'] = (np.log(merge_df['pnb']) - np.log(100)) / (np.log(75000)-np.log(100))
        merge_df = merge_df[['Year', 'index']]
        merge_df = merge_df.sort_values(by='Year', ascending=True)
        merge_df = merge_df.merge(ajusted_df, on='Year', how='left')
        merge_df['income_index_ajusted'] = merge_df['coef'] * merge_df['index']
        merge_df.drop(['coef'], axis=1, inplace=True)
        # growth rate for income index and adjusted income index
        merge_df['growth_rate_income_index'] = merge_df['index'].pct_change() * 100
        merge_df['growth_rate_income_index_ajusted'] = merge_df['income_index_ajusted'].pct_change() * 100

        if debug:
            return merge_df
        else:
            # round to 2 decimals
            # merge_df = merge_df.round(2)
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
                df = pd.read_csv(folder_path + file, engine="pyarrow")
                df = df[['AGEP', 'SCH', 'SCHL']]
                
                # calcualte the mean of years of schooling
                edu_sch = df[df['AGEP'] >= 25].copy()
                edu_sch['scholing'] = edu_sch['SCHL']
                edu_sch.reset_index(inplace=True)
                edu_sch['scholing'] = edu_sch['scholing'].apply(lambda x: self.to_category(x))
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
                    pd.DataFrame([[year, edu_value, edu_value_ajusted, atkinson, mean_sch, exp_sch]], columns=['Year', 'edu_index', 'edu_index_ajusted', 'atkinson', "Mean years of schooling", "Expected years of schooling"])], ignore_index=True)
                edu_index = edu_index.sort_values(by='Year', ascending=True)
            else:
                continue
        # growth rate for edu index & edu index ajusted
        edu_index['growth_rate'] = edu_index['edu_index'].pct_change() * 100
        edu_index['growth_rate_ajusted'] = edu_index['edu_index_ajusted'].pct_change() * 100
        if debug:
            return edu_index    
        else:
            # round to 2 decimals
            # edu_index = edu_index.round(2)
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
        #  groth rate for idh index & idh index ajusted
        df['growth_rate'] = df['index'].pct_change() * 100
        df['growth_rate_ajusted'] = df['index_ajusted'].pct_change() * 100
        if debug:
            return df
        else:
            # round to 2 decimals
            # df = df.round(2)
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

    def to_category(self, x):
        mapping = {4:1, 5:2, 6:3, 7:4, 8:5, 
                   9:6, 10:7, 11:8, 2:9, 13:10,
                   14: 11,15: 11, 16:12, 17:12, 
                   18:12.5, 19:13, 20:24, 21:16,
        }
        return mapping.get(x, 0) if x <= 21 else 18
    
if __name__ == "__main__":
    # # generate csv file for 2009-2020 for the education index
    idh = IndexIDH()
    print(idh.health_index(2019))