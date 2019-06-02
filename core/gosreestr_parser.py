import csv
import codecs
import json
import os
import re
import random
from multiprocessing.pool import ThreadPool

from time import sleep

from box import Box
import requests
from bs4 import BeautifulSoup

import pandas as pd

from core.core import HandlersFactory
from core.utils import Utils


class ParseGosRegisterToCSV():
    @staticmethod
    def get_list_page(url, page=0):
        form_data = {'pager-page-index_search-GrObjectsHeadRevisions': page,
                     '__RequestVerificationToken': '3pryUOiM7Njcl/5ikr8yzVfluNAZsgo53GgCphJ1Z3aCs5RZStR2q4b aI5ErKV6FktJh2Al54t2TZrYae0ivH9YJpe1DeugE3K4x9XCz/E5h3wK7gTm8uUjDDD X8R2GVuTbPvQL4fcvRcacDeBVuHamoM=',
                     'query-structure-search-GrObjectsHeadRevisions':'<LogicGroup GroupOperatorValue="And" GroupOperatorText="И" ><Condition IsStaticCondition="True" FieldName="tbGrObjects_flBlock" FieldText="Блокировка" ConditionOperatorValue="In" ConditionOperatorText="Входит в" Value="" ValueText="Слияние; Банкротство; Свободно; Ликвидация; Реабилитация; Остаток; Продан и неоформлена продажа; Сегментация; Акционирование; Преобразование в гп; Перевод в коммунальную собственность; Перевод в номинальное держание; Перевод в республиканскую собственность; Преобразование в тоо; Залоговый фонд" /></LogicGroup>'}
        with requests.Session() as s:
            s.headers = {"User-Agent": "Mozilla/5.0"}
            res = s.post(url, data=form_data).text
        soup = BeautifulSoup(res, 'lxml')
        return soup

    @staticmethod
    def get_pages_cnt(soup):
        html = soup.find(class_="page-selector")
        return int(html["total-pages-count"])

    @staticmethod
    def get_detail_links(soup):
        trs = soup.find('table', id='search-GrObjectsHeadRevisions').find('tbody').find_all('tr')
        urls = []
        for tr in trs:
            tds = tr.find_all('td')
            urls.append(tds[0].find('a').get('href'))
        return urls

    @staticmethod
    def get_data(url):
        # sleep(random.randint(4,10))
        non_reak_space = '&nbsp'
        res = requests.get(url).text
        soup = BeautifulSoup(res, 'lxml')
        tables = soup.find_all('table', class_='arrange-fields-table')
        data = []
        for table in tables:
            trs = table.find('tbody').find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                val = tds[1].text.replace(non_reak_space, '').replace(';', ',')
                data.append(val)
        return data

    @staticmethod
    def process(links):
        data = []
        for link in links:
            data.append(ParseGosRegisterToCSV.get_data(link))
            sleep(25)
        return data
        # pool = ThreadPool(3)
        # return pool.map(ParseGosRegisterToCSV.get_data, links)

    @staticmethod
    def write_data(fpath, data):
        for row in data:
            with open(fpath, 'a', encoding="utf8") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(row)

    @staticmethod
    def parse(instance, fpath):
        url = Box(json.loads(instance.srconf)).url
        soup = ParseGosRegisterToCSV.get_list_page(url)
        pages_cnt = ParseGosRegisterToCSV.get_pages_cnt(soup)
        for i in range(10):
            soup = ParseGosRegisterToCSV.get_list_page(url, i)
            links = ParseGosRegisterToCSV.get_detail_links(soup)
            data = ParseGosRegisterToCSV.process(links)
            ParseGosRegisterToCSV.write_data(fpath, data)
            sleep(20)


    @staticmethod
    def path(srconf, jobconf, dpath):
        name = Box(json.loads(srconf)).name
        data_format = Box(json.loads(jobconf)).data_format
        return os.path.join(dpath, "{}.{}".format(name, data_format))
