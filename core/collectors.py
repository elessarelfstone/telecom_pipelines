import os
import json

from box import Box
from os import path

import pandas as pd

from core.core import HandlersFactory
from core.utils import Utils


class CollectFromExcelToCSV():
    @staticmethod
    def basic_parse(xl_path, conf, job):
        data = pd.DataFrame()
        xls = pd.ExcelFile(xl_path)
        xls_sheets = xls.sheet_names
        for sh in Utils.params(params_file).sheets:
            df = pd.read_excel(xl_file_path,
                               sheet_name=xls_sheets[sh],
                               skiprows=Utils.params(params_file).skiprows,
                               index_col=None,
                               dtype=str,
                               header=None)

            data = data.append(df, ignore_index=True)
        data = data.replace(['nan', 'None'], '', regex=True)

        return data

    @staticmethod
    def collect(instance, file):


    @staticmethod
    def path(conf, job, directory):
        name = Box(json.loads(conf)).name
        data_format = Box(json.loads(conf)).storage.data_format
        return path.join(directory, "{}.{}".format(name, data_format))






