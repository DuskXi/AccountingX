import re
import numpy as np
from database import Db
import pandas as pd
import os


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
        files = get_all_files(data_path)
        new_files = []
        for file in files:
            if re.Match(r"\.[xX]$", file[1]):
                new_files.append(file)
        files = new_files
        self.files = files
        self.db = Db(np.array(self.files)[:, 1])
        self.datasets: list[list[Db.StructureAccount]] = []
        self.isLoad = False

    def load(self):
        self.datasets = self.db.load()
        self.isLoad = True

    def pre_process(self):

        for accounts in self.datasets:
            for account in accounts:
                ''
