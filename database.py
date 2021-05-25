import json
import os
from multiprocessing.dummy import Pool as ThreadPool
import numpy as np


def file_read(path, encoding="utf-8"):
    """

    :param path: 文件路径
    :param encoding: 读取编码，默认UTF-8
    :return:
    """
    f = open(path, 'r', encoding=encoding)
    text = str(f.read())
    f.close()
    return text


def file_write(path, text, encoding="utf-8"):
    f = open(path, 'w', encoding=encoding)
    f.write(text)
    f.close()


class Db:
    enable = [str, float, bool]

    def __init__(self, paths: list):
        """

        :param paths:
        """
        self.paths = []
        for path in paths:
            if os.path.exists(path):
                self.paths.append(path)
            else:
                raise Exception(f"路径{path}不存在")

    def load(self):
        """

        :return: list[list[Db.StructureAccount]]
        """
        pool = ThreadPool()
        results = pool.map(self.read, self.paths)
        pool.close()
        pool.join()
        return results

    def save(self, data, paths):
        if len(data) != len(paths):
            raise Exception("数据与数量不相等!")
        dataset = []
        for x, y in np.array((data, paths)).T:
            dataset.append((x, y))
        pool = ThreadPool()
        pool.map(self.write, dataset)
        pool.close()
        pool.join()

    def read(self, path):
        """

        :param path:
        :return:
        """
        json_text = file_read(path)
        json_obj = json.loads(json_text)
        accounts = []
        for account in json_obj:
            obj = self.deserialization(account, self.StructureAccount)
            accounts.append(obj)
        print(f"{path} 已载入")
        return accounts

    def write(self, path, data):
        accounts = []
        for account in data:
            accounts.append(self.serialization(account, self.StructureAccount))
        json_text = json.dumps(accounts)
        file_write(path, json_text)
        print(f"{path} 已写入")

    def serialization(self, obj, Class):
        dict_structure = self.get_structure(Class)
        for variable in dict_structure.keys():
            if dict_structure[variable] in self.enable:
                dict_structure[variable] = getattr(obj, variable)
            elif type(dict_structure[variable]) is list:
                dict_structure[variable] = []
                for data in getattr(obj, variable):
                    if type(data) in self.enable:
                        dict_structure[variable].append(data)
                    else:
                        dict_structure[variable].append(self.serialization(data, type(data)))
            else:
                dict_structure[variable] = self.serialization(data, type(data))
        return dict_structure

    def deserialization(self, account, Class):
        dict_structure = self.get_structure(Class)
        obj = Class()
        for variable in dict_structure.keys():
            if dict_structure[variable] in self.enable:
                value = account[variable]
                _type = dict_structure[variable]
                if type(value) is not _type:
                    value = dict_structure[variable](value)
                setattr(obj, variable, value)
            elif type(dict_structure[variable]) is list:
                setattr(obj, variable, [])
                for data in account[variable]:
                    getattr(obj, variable).append(self.deserialization(data, dict_structure[variable][0]))
            else:
                setattr(obj, variable, self.deserialization(account[variable], dict_structure[variable]))
        return obj

    @staticmethod
    def get_structure(Class):
        methods = dir(Class)
        obj = Class()
        dict_structure = {}
        for variable in dir(obj):
            if variable not in methods:
                type_variables = getattr(obj, variable)
                dict_structure[variable] = type_variables
        return dict_structure

    class StructureAccount:
        def __init__(self):
            self.details = [self.StructureDetail]
            self.account_number = str
            self.name = str
            self.enabled_currency = str

        class StructureDetail:
            def __init__(self):
                self.date = str
                self.text = str
                self.name_transaction_party = str
                self.account_transaction_party = str
                self.amount = float
                self.income = bool
                self.currency = str
                self.balance = float
