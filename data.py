import re
import numpy as np
from database import Db
import pandas as pd
import os
from pandas import Series, DataFrame
import datetime


def get_all_files(path):
    g = os.walk(path)
    files = []
    for path, dir_list, file_list in g:
        for file_name in file_list:
            files.append((file_name, os.path.join(path, file_name)))
    return files


class Data:
    def __init__(self, data_path):
        """

        :param data_path:
        """
        self.accounts_pandas = {}
        files = get_all_files(data_path)
        new_files = []
        for file in files:
            if re.match(r".*\.[xX]$", file[1]):
                new_files.append(file)
        files = new_files
        self.files = files
        self.db = Db(np.array(self.files)[:, 1])
        self.datasets: list[list[Db.StructureAccount]] = []
        self.isLoad = False

    def load(self):
        self.datasets = self.db.load()
        self.isLoad = True

    @staticmethod
    def create_data(array):
        data = {"date": [], "text": [], "name_transaction_party": [], "account_transaction_party": [], "amount": [],
                "income": [], "currency": [], "balance": []}
        for element in array:
            # data["date"].append(datetime.datetime.strptime(element.date, "%Y-%m-%d %H:%M:%S.%f"))
            data["date"].append(pd.to_datetime(element.date))
            data["text"].append(element.text)
            data["name_transaction_party"].append(element.name_transaction_party)
            data["account_transaction_party"].append(element.account_transaction_party)
            data["amount"].append(element.amount)
            data["income"].append(element.income)
            data["currency"].append(element.currency)
            data["balance"].append(element.balance)
        return data

    def pre_process(self):
        accounts_pandas = {}
        for accounts in self.datasets:
            for account in accounts:
                data = self.create_data(account.details)
                if account.account_number not in accounts_pandas.keys():
                    accounts_pandas[account.account_number] = DataFrame(data)
                else:
                    buffer = pd.concat([accounts_pandas[account.account_number], DataFrame(data)], axis=1)
                    accounts_pandas[account.account_number] = buffer
        self.accounts_pandas = accounts_pandas

    def query(self, account_number: str, time_start: datetime.datetime = None, time_end: datetime.datetime = None,
              currency: str = None, income_type: bool = None, max_amount: float = None, min_amount: float = None,
              text: str = None):
        """

        :param text:
        :param min_amount:
        :param max_amount:
        :param income_type:
        :param currency:
        :param account_number:
        :param time_start:
        :param time_end:
        :return:
        """
        if account_number is None:
            raise Exception("账户号为空")
        if time_start is None:
            time_start = datetime.datetime(1970, 1, 1, 0, 0, 0)
        if time_end is None:
            time_end = datetime.datetime.now()
        if min_amount is None:
            min_amount = -1
        in_time_result = self.accounts_pandas[account_number][
            (self.accounts_pandas[account_number]["date"] >= time_start) & (
                    self.accounts_pandas[account_number]["date"] <= time_end)
            ]
        if currency is not None:
            in_currency_result = in_time_result.loc[in_time_result["currency"] == currency]
        else:
            in_currency_result = in_time_result
        if income_type is not None:
            in_type_result = in_currency_result.loc[in_currency_result["income"] == income_type]
        else:
            in_type_result = in_currency_result
        if max_amount is not None:
            in_amount_result = in_type_result.loc[
                (in_type_result["amount"] >= min_amount) & (in_type_result["amount"] <= max_amount)]
        else:
            in_amount_result = in_type_result.loc[(in_type_result["amount"] >= min_amount)]
        if text is not None:
            in_text_result = in_amount_result.loc[
                text in in_amount_result["text"] or text in in_amount_result["name_transaction_party"] or text in
                in_amount_result["account_transaction_party"]]
        else:
            in_text_result = in_amount_result
        return in_text_result
