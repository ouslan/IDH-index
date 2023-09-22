from IDH import IndexIDH
from time import sleep
from tqdm import tqdm
from termcolor import colored

# While loop to choose uption
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
        idh = IDH()
        idh.get_data(['2009','2021'])
        print("Data downloaded")
        os.system('clear')
        sleep(2)
    elif option == 2:
        idh = IDH()
        idh.show_data()
        sleep(2)
        os.system('clear')
    elif option == 3:
        idh = IDH()
        print("Calculating the health index")
        sleep(2)
        print(colored(f"The health index is {idh.health_index()}", 'green'))
        sleep(2)
        os.system('clear')
    elif option == 4:
        idh = IDH()
        print("Calculating the income index")
        sleep(2)
        os.system('clear')
        print(f"The income index is {idh.income_index()}")
        sleep(2)
 
    elif option == 5:
        idh = IDH()
        year = int(input("Enter a year: "))
        os.system('clear')
        print("Calculating the education index")
        sleep(2)
        # print with color
        print(colored(f"The education index is {idh.edu_index(year)}", 'blue'))
        sleep(2)
    elif option == 6:
        idh = IDH()
        print("Calculating the IDH index")
        sleep(2)
        print(f"The IDH index is {idh.idh_index()}")
        sleep(2)
    elif option == 7:
        print("Good bye")
        break
    else:
        print("Invalid option")
        sleep(2)