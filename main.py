from src.data.fetch import download
from src.visualization.visualize import Graphs
import pandas as pd
import numpy as np
import os

from simple_term_menu import TerminalMenu
from rich.console import Console
from rich.table import Table

def main():
    download(['2012','2022'])

if __name__ == "__main__":
    main()