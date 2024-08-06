from src.data.data_pull import DataPull
from scipy.stats import gmean
import world_bank_data as wb
import pandas as pd
import polars as pl
import numpy as np
import os

class DataProcess(DataPull):
    """
    A class to process various indices from raw data and save them to processed files.

    Parameters
    ----------
    end_year : int
        The end year for data processing.
    file_remove : bool, optional
        If True, removes the raw data files after processing. The default is False.
    debug : bool, optional
        If True, enables debug messages. The default is False.
    """

    def __init__(self, end_year: int, file_remove: bool = False, debug: bool = False) -> None:
        """
        Initializes the DataProcess instance and processes the data for specified years.

        Parameters
        ----------
        end_year : int
            The end year for data processing.
        file_remove : bool, optional
            If True, removes the raw data files after processing. The default is False.
        debug : bool, optional
            If True, enables debug messages. The default is False.
        """
        super().__init__(end_year=end_year, debug=debug)
        self.health_index()
        self.income_index()
        self.edu_index()
        self.idh_index()
        if file_remove:
            self.remove_data()

    def health_index(self) -> None:
        """
        Processes the health index based on life expectancy data and saves it to a CSV file.

        Returns
        -------
        None
        """
        df = pl.read_parquet('data/raw/life_exp.parquet')
        df = df.fill_null(strategy="forward")
        df = df.with_columns(
            pl.col("year").cast(pl.Int64),
            ((pl.col("life_exp") - 20) / (85 - 20)).alias("health_index"),
            (((pl.col("life_exp") - 20) / (85 - 20)) * (1 - 0.08)).alias("health_index_adjusted"),
            arkinson=0.08
        )
        df = df.with_columns(
            (pl.col("health_index").pct_change() * 100).alias("health_index_pct_change"),
            (pl.col("health_index_adjusted").pct_change() * 100).alias("health_index_adjusted_pct_change")
        )
        
        df.write_csv('data/processed/health_index.csv')

        if self.debug:
            print("\033[0;32mINFO: \033[0m" + "Health index data is processed")

    def income_index(self, debug: bool = False) -> None:
        """
        Processes the income index and saves it to a CSV file.

        Parameters
        ----------
        debug : bool, optional
            If True, enables debug messages. The default is False.

        Returns
        -------
        None
        """
        # Adjust the income index
        empty = [
            pl.Series("year", [], dtype=pl.Int64), 
            pl.Series("coef", [], dtype=pl.Float64),
            pl.Series("atkinson", [], dtype=pl.Float64)
        ]
        adjusted_df = pl.DataFrame(empty)
        capita_df = pl.read_parquet("data/raw/gni_capita.parquet")
        constant_df = pl.read_parquet("data/raw/gni_constant.parquet")

        for file in os.listdir('data/raw/'):
            if file.startswith('data_hpr'):
                adjust_df = pl.read_csv("data/raw/data_hpr_2012_raw.csv")
                adjust_df = adjust_df.select(pl.col("HINCP").drop_nulls())
                adjust_df = adjust_df.sort("HINCP")
                adjust_df = adjust_df.filter(pl.col("HINCP") > 0)

                # Replace bottom 0.5%
                bottom_max = adjust_df.select(pl.col("HINCP").quantile(0.005))
                adjust_df = adjust_df.select(
                    pl.when(pl.col("HINCP") < bottom_max)
                      .then(bottom_max)
                      .otherwise(pl.col("HINCP")).alias("HINCP")
                )

                # Drop top 0.5%
                adjust_df = adjust_df.filter(
                    pl.col("HINCP") <= pl.col("HINCP").quantile(0.995)
                )

                # Get coefficient of adjustment
                coef, amean, gemetric, atkinson = self.adjust(adjust_df)
                tmp_df = pl.DataFrame({
                    "year": int(file.split('_')[2]),
                    "coef": coef[0][0],
                    "atkinson": atkinson[0][0]
                })

                adjusted_df = pl.concat([adjusted_df, tmp_df], how="vertical")
        
        # Merge the two dataframes
        inc_df = capita_df.join(constant_df, on='year')
        inc_df = inc_df.with_columns(
            (pl.col("constant") / pl.col("capita")).alias("income_ratio")
        )

        # Merge the income index with the pnb.csv file
        pnb = pl.read_csv('data/external/pnb.csv')
        merge_df = inc_df.join(pnb, on='year', how='left').drop_nulls()
        merge_df = merge_df.join(adjusted_df, on='year', how='left')
        
        # Calculate the index
        merge_df = merge_df.with_columns(
            ((np.log(pl.col('pnb')) - np.log(100)) / (np.log(75000) - np.log(100))).alias('index')
        )
        merge_df = merge_df.with_columns(
            (pl.col("index") * pl.col("coef")).alias("income_index_adjusted")
        )
        merge_df = merge_df.select(pl.col("year", "index", "income_index_adjusted")).drop_nulls()
        
        merge_df.write_csv('data/processed/income_index.csv')

        if self.debug:
            print("\033[0;32mINFO: \033[0m" + "Income index data is processed")
    
    def edu_index(self, folder_path: str = 'data/raw/', debug: bool = False) -> None:
        """
        Processes the education index and saves it to a CSV file.

        Parameters
        ----------
        folder_path : str, optional
            The path to the folder containing raw data files. The default is 'data/raw/'.
        debug : bool, optional
            If True, enables debug messages. The default is False.

        Returns
        -------
        None
        """
        edu_index = pd.DataFrame([], columns=['year', 'edu_index', 'edu_index_adjusted'])
        for file in os.listdir(folder_path):
            if file.startswith('data_ppr'):
                df = pd.read_csv(folder_path + file, engine="pyarrow")
                df = df[['AGEP', 'SCH', 'SCHL']]
                
                # Calculate the mean of years of schooling
                edu_sch = df[df['AGEP'] >= 25].copy()
                edu_sch['schooling'] = edu_sch['SCHL']
                edu_sch.reset_index(inplace=True)
                edu_sch['schooling'] = edu_sch['schooling'].apply(lambda x: self.to_category(x))
                edu_sch['enrolled'] = np.where(edu_sch['schooling'] > 1, 1, 0)
                mean_sch = edu_sch['schooling'].mean()

                # Get coefficient of adjustment
                edu_sch['no_zero_schooling'] = 1 + edu_sch['schooling']
                coef, amean, gemetric, atkinson = self.adjust(edu_sch['no_zero_schooling'])

                # Calculate the expected years of schooling
                edu_exp = df[df['AGEP'] < 25].copy()
                edu_exp['enrolled'] = edu_exp['SCH'].apply(lambda x: 1 if x > 1 else 0)
                edu_age = edu_exp.groupby(['AGEP'])[['AGEP', 'enrolled']].count()
                edu_age['enrolled'] = edu_exp.groupby(['AGEP'])['enrolled'].sum()
                edu_age['enrollment_rate'] = edu_age['enrolled'] / edu_age['AGEP']
                edu_age = edu_age.rename(columns={'AGEP': 'count'})
                edu_age = edu_age.reset_index()
                edu_age.drop([0, 1, 2, 3, 4], inplace=True)
                exp_sch = edu_age['enrollment_rate'].sum()

                # Calculate index
                edu_value = (mean_sch / 15 + exp_sch / 18) / 2
                edu_value_adjusted = coef * edu_value
                year = file.split('_')[2]
                edu_index = pd.concat([
                    edu_index if not edu_index.empty else None,
                    pd.DataFrame([[year, edu_value, edu_value_adjusted, atkinson, mean_sch, exp_sch]], columns=['year', 'edu_index', 'edu_index_adjusted', 'atkinson', "Mean years of schooling", "Expected years of schooling"])], ignore_index=True)
                edu_index = edu_index.sort_values(by='year', ascending=True)
            else:
                continue
        # Growth rate for edu index & edu index adjusted
        edu_index['growth_rate'] = edu_index['edu_index'].pct_change() * 100
        edu_index['growth_rate_adjusted'] = edu_index['edu_index_adjusted'].pct_change() * 100

        edu_index.to_csv('data/processed/edu_index.csv', index=False)

        if self.debug:
            print("\033[0;32mINFO: \033[0m" + "Education index data is processed")

    def idh_index(self, debug: bool = False) -> None:
        """
        Calculates the Human Development Index (HDI) from processed indices and saves it to a CSV file.

        Parameters
        ----------
        debug : bool, optional
            If True, enables debug messages. The default is False.

        Returns
        -------
        None
        """
        # Get & calculate the health index
        health = pd.read_csv('data/processed/health_index.csv')
        health.rename(columns={'index': 'health_index'}, inplace=True)
        health = health[['year', 'health_index', 'health_index_adjusted']]
        income = pd.read_csv('data/processed/income_index.csv')
        income.rename(columns={'index': 'income_index'}, inplace=True)
        income = income[['year', 'income_index', 'income_index_adjusted']]
        edu = pd.read_csv('data/processed/edu_index.csv')
        edu.rename(columns={'index': 'edu_index'}, inplace=True)
        edu = edu[['year', 'edu_index', 'edu_index_adjusted']]
        
        # Calculate the index & save in CSV
        df = health.merge(income, on='year', how='left')
        df = df.merge(edu, on='year', how='left')
        df['index'] = (df['health_index'] * df['income_index'] * df['edu_index']) ** (1/3)
        df['index_adjusted'] = (df['health_index_adjusted'] * df['income_index_adjusted'] * df['edu_index_adjusted']) ** (1/3)
        df.dropna(inplace=True)
        # Growth rate for HDI index & HDI index adjusted
        df['growth_rate'] = df['index'].pct_change() * 100
        df['growth_rate_adjusted'] = df['index_adjusted'].pct_change() * 100
        
        df.to_csv('data/processed/idh_index.csv', index=False)

        if self.debug:
            print("\033[0;32mINFO: \033[0m" + "HDI index data is processed")

    def adjust(self, df: pl.DataFrame) -> tuple:
        """
        Calculates adjustment coefficients for income or education data.

        Parameters
        ----------
        df : pl.DataFrame
            The dataframe of data to adjust.

        Returns
        -------
        tuple
            A tuple containing coefficients: (coef, amean, gemetric, atkinson).
        """
        gemetric = gmean(df)
        amean = df.mean()
        atkinson = 1 - gemetric / amean
        coef = 1 - atkinson
        return coef, amean, gemetric, atkinson

    def to_category(self, x: int) -> float:
        """
        Maps schooling years to categories.

        Parameters
        ----------
        x : int
            The number of years of schooling.

        Returns
        -------
        float
            The categorized years of schooling.
        """
        mapping = {4: 1, 5: 2, 6: 3, 7: 4, 8: 5, 
                   9: 6, 10: 7, 11: 8, 12: 9, 13: 10,
                   14: 11, 15: 11, 16: 12, 17: 12, 
                   18: 12.5, 19: 13, 20: 14, 21: 16}
        return mapping.get(x, 0) if x <= 21 else 18
    
    def remove_data(self) -> None:
        """
        Removes raw and processed data files.

        Returns
        -------
        None
        """
        for file in os.listdir("data/raw/"):
            if file.endswith(".csv") or file.endswith(".parquet"):
                os.remove("data/raw/" + file)
        for file in os.listdir("data/processed/"):
            if file.endswith(".csv") and not file.startswith('idh_index'):
                os.remove("data/processed/" + file)

if __name__ == "__main__":
    DataProcess(end_year=2023, debug=True)
