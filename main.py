from src.data.IDH import IndexIDH
from src.data.fetch import download
import pandas as pd
from time import sleep
import numpy as np
import os
from simple_term_menu import TerminalMenu
from rich.console import Console
from rich.table import Table

def main():
    options = ["Download IDH data", "Show Health Index", "Show Education Index", "Show Income Index","IDH Index", "Graph", "Exit"]
    terminal_menu = TerminalMenu(options)
    menu_entry_index = terminal_menu.show()
    # downlad the data
    if menu_entry_index == 0:
        download(['2012','2021'])
    # show the health index
    elif menu_entry_index == 1:
        if os.path.isfile("data/processed/idh_index.csv"):
            df = pd.read_csv("data/processed/idh_index.csv")
            df = df[['Year','health_index','health_index_ajusted']]
            table = Table(show_header=True, title="Health Index")
            table.add_column("Year")
            table.add_column("Health Index")
            table.add_column("Health Index Ajusted")
            for i in range(len(df)):
                table.add_row(str(np.round(df.iloc[i,0], decimals=2)),
                              str(np.round(df.iloc[i,1],decimals=2)),
                              str(np.round(df.iloc[i,2], decimals=2)))
            console = Console()
            console.print(table)
        else:
            print("No data found, please download the data first")
    elif menu_entry_index == 2:
        if os.path.isfile("data/processed/idh_index.csv"):
            df = pd.read_csv("data/processed/idh_index.csv")
            df = df[['Year','edu_index','edu_index_ajusted']]
            table = Table(show_header=True, title="Education Index")
            table.add_column("Year")
            table.add_column("Education Index")
            table.add_column("Education Index Ajusted")
            for i in range(len(df)):
                table.add_row(str(np.round(df.iloc[i,0], decimals=2)), 
                              str(np.round(df.iloc[i,1],decimals=2)), 
                              str(np.round(df.iloc[i,2], decimals=2)))
            console = Console()
            console.print(table)
        else:
            print("No data found, please download the data first")
    elif menu_entry_index == 3:
        if os.path.isfile("data/processed/idh_index.csv"):
            df = pd.read_csv("data/processed/idh_index.csv")
            df = df[['Year','income_index','income_index_ajusted']]
            table = Table(show_header=True, title="Income Index")
            table.add_column("Year")
            table.add_column("Income Index")
            table.add_column("Income Index Ajusted")
            for i in range(len(df)):
                table.add_row(str(np.round(df.iloc[i,0], decimals=2)), 
                              str(np.round(df.iloc[i,1],decimals=2)), 
                              str(np.round(df.iloc[i,2], decimals=2)))
            console = Console()
            console.print(table)
        else:
            print("No data found, please download the data first")
    elif menu_entry_index == 4:
        if os.path.isfile("data/processed/idh_index.csv"):
            df = pd.read_csv("data/processed/idh_index.csv")
            df = df[['Year','index','index_ajusted']]
            table = Table(show_header=True, title="IDH Index")
            table.add_column("Year")
            table.add_column("IDH Index")
            table.add_column("IDH Index Ajusted")
            for i in range(len(df)):
                table.add_row(str(np.round(df.iloc[i,0], decimals=2)), 
                              str(np.round(df.iloc[i,1],decimals=2)),
                              str(np.round(df.iloc[i,2], decimals=2)))
            console = Console()
            console.print(table)
        else:
            print("No data found, please download the data first")

if __name__ == "__main__":
    main()
