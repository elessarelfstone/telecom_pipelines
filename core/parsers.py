import csv
import codecs
import json
import os
import re

from time import sleep

from box import Box
import requests
from bs4 import BeautifulSoup

import pandas as pd

from core.core import HandlersFactory
from core.utils import Utils


class ParseFromExcelToCSV():
    @staticmethod
    def xls_retrieve(xlspath, srconf):
        data = pd.DataFrame()
        xls = pd.ExcelFile(xlspath)
        xls_sheets = xls.sheet_names
        sheets = Box(json.loads(srconf)).data.sheets
        for sh in sheets:
            df = pd.read_excel(xlspath,
                               sheet_name=xls_sheets[sh],
                               skiprows=Box(json.loads(srconf)).data.area.indent.top,
                               index_col=None,
                               dtype=str,
                               header=None)

            data = data.append(df, ignore_index=True)
        data = data.replace(['nan', 'None'], '', regex=True)
        return data

    @staticmethod
    def xlss_retrieve(xlspaths, srconf):
        total = pd.DataFrame()
        for xlspath in xlspaths:
            data = ParseFromExcelToCSV.xls_retrieve(xlspath, srconf)
            total = total.append(data, ignore_index=True)
        return total

    @staticmethod
    def save(df, srconf, jobconf, fpath):
        header = dict(Box(json.loads(srconf)).data.header).keys()
        df = df.iloc[:, 0:len(header)]
        sep = Box(json.loads(jobconf)).separator
        df.to_csv(fpath, sep=sep, encoding='utf-8', header=None, index=None)
        return df

    @staticmethod
    def parse(instance, fpath):
        if isinstance(instance.xlspath, str):
            df = ParseFromExcelToCSV.xls_retrieve(instance.xlspath, instance.srconf)
        else:
            df = ParseFromExcelToCSV.xlss_retrieve(instance.xlspath, instance.srconf)
        data = ParseFromExcelToCSV.save(df, instance.srconf, instance.jobconf, fpath)
        return data.shape[0]

    @staticmethod
    def path(srconf, jobconf, dpath):
        name = Box(json.loads(srconf)).name
        data_format = Box(json.loads(jobconf)).data_format
        return os.path.join(dpath, "{}.{}".format(name, data_format))


class ParseFromAPIToCSV():
    @staticmethod
    def get_query(local_dict, query_dict):
        local_dict.update({k: v for k, v in query_dict.items() if v is not None})
        return str(local_dict).replace("\'", '\"')

    @staticmethod
    def write_data(fpath, data, delimiter=";"):
        with open(fpath, "a", encoding="utf8") as f:
            csv_writer = csv.writer(f, delimiter=delimiter)
            for row in data:
                csv_writer.writerow(row.values())

    @staticmethod
    def parse(instance, fpath):
        url = Box(json.loads(instance.srconf)).url
        conf_query = dict(Box(json.loads(instance.srconf)).data.query)
        local_query = {"from": 1}
        data = Utils.get_json_data(url.format(ParseFromAPIToCSV.get_query(local_query, conf_query)))
        i = 0
        while len(data):
            data = Utils.get_json_data(url.format(ParseFromAPIToCSV.get_query(local_query, conf_query)))
            ParseFromAPIToCSV.write_data(fpath, data)
            i += 1
            frm = conf_query["size"] * i + 1
            local_query = {"from": frm}
            data = Utils.get_json_data(url.format(ParseFromAPIToCSV.get_query(local_query, conf_query)))
            sleep(3)

    @staticmethod
    def path(srconf, jobconf, dpath):
        name = Box(json.loads(srconf)).name
        data_format = Box(json.loads(jobconf)).data_format
        return os.path.join(dpath, "{}.{}".format(name, data_format))


class ParseJavaScriptJsonToCSV():
    @staticmethod
    def get_data(instance):
        url = Box(json.loads(instance.srconf)).url
        raw = codecs.encode(requests.get(url).text, encoding="utf8")
        soup = BeautifulSoup(raw, 'lxml')
        scripts = soup.find_all('script')
        json_key = Box(json.loads(instance.srconf)).storage.json_data_key
        pattern = r'("ref"\s*):(\s*\[\S+\])'
        for script in scripts:
            res = re.search(pattern, script.text)
            if res:
                json_raw = res.group(2)
                break
        return json.loads(json_raw)

    @staticmethod
    def write_data(fpath, data, delimiter=";"):
        with open(fpath, "a", encoding="utf8") as f:
            csv_writer = csv.writer(f, delimiter=delimiter)
            for row in data:
                csv_writer.writerow(row.values())

    @staticmethod
    def parse(instance, fpath):
        data = ParseJavaScriptJsonToCSV.get_data(instance)
        ParseJavaScriptJsonToCSV.write_data(fpath, data)

    @staticmethod
    def path(srconf, jobconf, dpath):
        name = Box(json.loads(srconf)).name
        data_format = Box(json.loads(jobconf)).data_format
        return os.path.join(dpath, "{}.{}".format(name, data_format))



HandlersFactory.register("xlsparse_to_csv", ParseFromExcelToCSV)
HandlersFactory.register("web_api_raw_json_parse_to_csv", ParseFromAPIToCSV)
HandlersFactory.register("web_html_javascript_json_parse_to_csv", ParseJavaScriptJsonToCSV)

