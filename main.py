from database import Db
from data import Data


def main():
    """

    :return:
    """
    # Db(["test.json"]).read("test.json")
    data = Data("testDataDir/accounts")
    data.load()
    data.pre_process()
    data.query(account_number='test')


if __name__ == '__main__':
    main()
