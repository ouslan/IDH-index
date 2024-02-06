from src.data.fetch import download
import pandas as pd
import numpy as np
import os
from simple_term_menu import TerminalMenu
from rich.console import Console
from rich.table import Table

def main():
    main_menu_title = "  Main Menu.\n  Press Q or Esc to quit. \n"
    main_menu_items = ["Download IDH data", "Show Health Index", "Show Education Index", "Show Income Index", "IDH Index", "Graph", "Quit"]
    main_menu_cursor = "> "
    main_menu_cursor_style = ("fg_red", "bold")
    main_menu_style = ("bg_red", "fg_yellow")
    main_menu_exit = False

    main_menu = TerminalMenu(
        menu_entries=main_menu_items,
        title=main_menu_title,
        menu_cursor=main_menu_cursor,
        menu_cursor_style=main_menu_cursor_style,
        menu_highlight_style=main_menu_style,
        cycle_cursor=True,
        clear_screen=True,
    )
    # graph menu WIP

    while not main_menu_exit:
        main_sel = main_menu.show()
        if main_sel == 0:
            download(['2012','2022'])
        # show the health index
        elif main_sel == 1:
            if os.path.isfile("data/processed/idh_index.csv"):
                df = pd.read_csv("data/processed/idh_index.csv")
                df = df[['Year','health_index','health_index_ajusted']]
                table = Table(show_header=True, title="Health Index")
                table.add_column("Year")
                table.add_column("Health Index")
                table.add_column("Health Index Ajusted")
                for i in range(len(df)):
                    table.add_row(str(np.round(df.iloc[i,0], decimals=4)),
                                str(np.round(df.iloc[i,1],decimals=4)),
                                str(np.round(df.iloc[i,2], decimals=4)))
                console = Console()
                console.print(table)
                input("Press Enter to continue...")
            else:
                print("No data found, please download the data first")
        elif main_sel == 2:
            if os.path.isfile("data/processed/idh_index.csv"):
                df = pd.read_csv("data/processed/idh_index.csv")
                df = df[['Year','edu_index','edu_index_ajusted']]
                table = Table(show_header=True, title="Education Index")
                table.add_column("Year")
                table.add_column("Education Index")
                table.add_column("Education Index Ajusted")
                for i in range(len(df)):
                    table.add_row(str(np.round(df.iloc[i,0], decimals=4)), 
                                str(np.round(df.iloc[i,1],decimals=4)), 
                                str(np.round(df.iloc[i,2], decimals=4)))
                console = Console()
                console.print(table)
                input("Press Enter to continue...")

            else:
                print("No data found, please download the data first")
        elif main_sel == 3:
            if os.path.isfile("data/processed/idh_index.csv"):
                df = pd.read_csv("data/processed/idh_index.csv")
                df = df[['Year','income_index','income_index_ajusted']]
                table = Table(show_header=True, title="Income Index")
                table.add_column("Year")
                table.add_column("Income Index")
                table.add_column("Income Index Ajusted")
                for i in range(len(df)):
                    table.add_row(str(np.round(df.iloc[i,0], decimals=4)), 
                                str(np.round(df.iloc[i,1],decimals=4)), 
                                str(np.round(df.iloc[i,2], decimals=4)))
                console = Console()
                console.print(table)
                input("Press Enter to continue...")
            else:
                print("No data found, please download the data first")
        elif main_sel == 4:
            if os.path.isfile("data/processed/idh_index.csv"):
                df = pd.read_csv("data/processed/idh_index.csv")
                df = df[['Year','index','index_ajusted']]
                table = Table(show_header=True, title="IDH Index")
                table.add_column("Year")
                table.add_column("IDH Index")
                table.add_column("IDH Index Ajusted")
                for i in range(len(df)):
                    table.add_row(str(np.round(df.iloc[i,0], decimals=4)), 
                                str(np.round(df.iloc[i,1],decimals=4)),
                                str(np.round(df.iloc[i,2], decimals=4)))
                console = Console()
                console.print(table)
                input("Press Enter to continue...")
            else:
                print("No data found, please download the data first")
        elif main_sel == 6 or main_sel == None:
            main_menu_exit = True

if __name__ == "__main__":
    main()
