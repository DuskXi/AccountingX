from database import Db
import data


def main():
    """

    :return:
    """
    # Db(["test.json"]).read("test.json")
    data.get_all_files(r"testDataDir/accounts")


if __name__ == '__main__':
    main()
