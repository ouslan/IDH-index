from src.data.IDH import IndexIDH
import numpy as np
from time import sleep
from tqdm import tqdm
from termcolor import colored
import os

# While loop to choose option
while True:
    print("1. Download the data")
    print("2. Show the data")
    print("3. Show the health index")
    print("4. Show the income index")
    print("5. Show the education index")
    print("6. Show the IDH index")
    print("7. Exit")
    option = int(input("Choose an option: "))
    if option == 1:
        idh = IndexIDH()
        idh.get_data(['2012','2021'])
        print("Data downloaded")
        os.system('clear')
        sleep(2)
    elif option == 2:
        idh = IndexIDH()
        idh.show_data()
        sleep(2)
        os.system('clear')
    # health index
    elif option == 3:
        idh = IndexIDH()
        year = int(input("Enter a year: "))
        os.system('clear')
        print("Calculating the health index")
        sleep(2)
        if idh.health_index(year) is np.nan:
            print(colored(f"ERROR: The year {year} is outside the range. Please try again",'red'))
        else:
            print(colored(f"The health index is {np.round(idh.health_index(year), decimals=2)}", 'blue'))
        sleep(5)
        os.system('clear')
    # income index
    elif option == 4:
        idh = IndexIDH()
        os.system('clear')
        year = int(input("Enter a year: "))
        os.system('clear')
        print("Calculating the education index")
        sleep(2)
        if idh.income_index(year) is np.nan:
            print(colored(f"ERROR: The year {year} is outside the 2009-2021 range. Please try again",'red'))
        else:
            print(colored(f"The education index is {np.round(idh.income_index(year),decimals=2)}", 'blue'))
        sleep(2)
        os.system('clear')
    # Education index
    elif option == 5:
        idh = IndexIDH()
        os.system('clear')
        year = int(input("Enter a year: "))
        os.system('clear')
        print("Calculating the education index")
        sleep(2)
        if idh.edu_index(year) is np.nan:
            print(colored(f"ERROR: The year {year} is outside the 2009-2021 range. Please try again",'red'))
        else:
            print(colored(f"The education index is {np.round(idh.edu_index(year), decimals=2)}", 'blue'))
        sleep(2)
        os.system('clear')
    # IDH index
    elif option == 6:
        idh = IndexIDH()
        os.system('clear')
        year = int(input("Enter a year: "))
        print("Calculating the IDH index")
        sleep(2)
        print(colored(f"The IDH index is {np.round(idh.idh_index(year), decimals=2)}", 'blue'))
        sleep(2)
        os.system('clear')
    elif option == 7:
        print("Good bye")
        break
    else:
        print("Invalid option")
        sleep(2)