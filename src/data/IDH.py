import pandas as pd
import polars as pl
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
        df = pd.DataFrame(wb.get_series('SP.DYN.LE00.IN', country='PR', simplify_index=True))
        df = pl.from_pandas(df.reset_index())
        
        df = df.rename({"SP.DYN.LE00.IN":"life_exp"}).fill_null(strategy="forward")
        df = df.with_columns(
            pl.col("Year").cast(pl.Int64),
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
        atlas_df = pl.from_pandas(pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.CD', country='PR', simplify_index=True).reset_index()))
        atlas_df = atlas_df.rename({"NY.GNP.PCAP.PP.CD": "atlas"}).drop_nulls()
        atlas_df = atlas_df.with_columns(
            pl.col("Year").cast(pl.Int64),
            pl.col("atlas").cast(pl.Int64))

        # get gni constant df from WB
        gni_df = pl.from_pandas(pd.DataFrame(wb.get_series('NY.GNP.PCAP.PP.KD', country='PR', simplify_index=True).reset_index()))
        gni_df = gni_df.rename({'NY.GNP.PCAP.PP.KD': 'gni'})
        gni_df = gni_df.with_columns(pl.col("Year").cast(pl.Int64))

        # adjust the income index
        adjusted_df = pl.DataFrame({"Year": [1],"coef": [1.1],"atkinson": [1.1],}).clear()

        for file in os.listdir('data/raw/'):
            if file.startswith('data_hpr'):
                adjust_df = pl.read_csv("data/raw/data_hpr_2012_raw.csv")
                adjust_df = adjust_df.select(pl.col("HINCP").drop_nulls())
                adjust_df = adjust_df.sort("HINCP")
                adjust_df = adjust_df.filter(pl.col("HINCP") > 0)

                # replace bottom 0.5% 
                bottom_max = adjust_df.select(pl.col("HINCP").quantile(0.005))
                adjust_df = adjust_df.select(
                    pl.when(pl.col("HINCP") < bottom_max)
                    .then(bottom_max)
                    .otherwise(pl.col("HINCP")).alias("HINCP"))

                # drop top 0.5%
                adjust_df = adjust_df.filter(
                    pl.col("HINCP") <= pl.col("HINCP").quantile(0.995))

                # get coefficient of adjustmet
                coef, amean, gemetric, atkinson = self.adjust(adjust_df)
                tmp_df = pl.DataFrame({
                    "Year": int(file.split('_')[2]),
                    "coef": coef[0][0],
                    "atkinson": atkinson[0][0]})

                adjusted_df = pl.concat([adjusted_df, tmp_df], how="vertical")
        
        # merge the two dataframes
        inc_df = atlas_df.join(gni_df, on='Year')
        inc_df = inc_df.with_columns(
            (pl.col("gni") / pl.col("atlas")).alias("income_ratio"))

        # merge the income index with the pnb.csv file
        pnb = pl.read_csv('data/external/pnb.csv')
        merge_df = inc_df.join(pnb, on='Year', how='left').drop_nulls()
        merge_df = merge_df.join(adjusted_df, on='Year', how='left')
        
        # calculate the index
        merge_df =  merge_df.with_columns(
            ((np.log(pl.col('pnb')) - np.log(100)) / (np.log(75000)-np.log(100))).alias('index'))
        merge_df = merge_df.with_columns(
            (pl.col("index") * pl.col("coef")).alias("income_index_ajusted"))
        merge_df = merge_df.select(pl.col("Year", "index", "income_index_ajusted")).drop_nulls()
        
        if debug:
            return merge_df
        else:
            # round to 2 decimals
            # merge_df = merge_df.round(2)
            merge_df.write_csv('data/processed/income_index.csv')
    
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
        health = health[['Year', 'health_index', 'health_index_adjusted']]
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
        df['index_ajusted'] = (df['health_index_adjusted'] * df['income_index_ajusted'] * df['edu_index_ajusted']) ** (1/3)
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
                   9:6, 10:7, 11:8, 12:9, 13:10,
                   14:11 ,15:11, 16:12, 17:12, 
                   18:12.5, 19:13, 20:14, 21:16,
        }
        return mapping.get(x, 0) if x <= 21 else 18
    
if __name__ == "__main__":
    # # generate csv file for 2009-2020 for the education index
    idh = IndexIDH()
    print(idh.health_index(2019))