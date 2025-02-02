from src.data.data_pull import DataPull


def main() -> None:
    dp = DataPull()
    # print(dp.pull_pumspr().execute())
    print(dp.pull_wb().execute())


if __name__ == "__main__":
    main()
