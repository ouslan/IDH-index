from src.data.data_pull import DataPull
from src.data.data_process import DataProcess
def main():
    DataPull(end_year=2022, debug=True)
    DataProcess()


if __name__ == "__main__":
    main()
