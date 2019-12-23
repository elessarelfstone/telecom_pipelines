import os
import re
import json
import requests
import uuid

from bs4 import BeautifulSoup
from box import Box

from core.core import HandlersFactory
from core.utils import Utils
from settings import TEMP_PATH


class DownloaderByUrlToFile():
    def download(self, instance, fpath):
        try:
            url = Box(json.loads(instance.srconf)).url
            result = requests.get(url, verify=False)
            with open(fpath, 'wb') as f:
                f.write(result.content)
            return fpath
        except Exception as e:
            raise e

    @staticmethod
    def path(srconf):
        name = Box(json.loads(srconf)).name
        ext = Box(json.loads(srconf)).storage.type
        dpath = TEMP_PATH
        os.makedirs(dpath, exist_ok=True)
        return os.path.join(dpath, "{}.{}".format(name, ext))


class DownloaderByUrlListToFile():

    def download(self, instance, fpath):
        try:
            for url, path in zip(self.urls(instance.srconf), fpath):
                result = requests.get(url, verify=False)
                with open(path, 'wb') as f:
                    f.write(result.content)
            return fpath
        except Exception as e:
            raise e

    @staticmethod
    def urls(conf):
        url = Box(json.loads(conf)).url
        html = requests.get(Box(json.loads(conf)).url).text
        soup = BeautifulSoup(html, 'lxml')
        container = Box(json.loads(conf)).storage.html.container_tag
        attrs = Box(json.loads(conf)).storage.html.container_attrs
        href_regex = Box(json.loads(conf)).storage.html.url_regexp
        urls = soup.find(container, attrs=attrs).find_all("a", href=re.compile(href_regex))
        urls = [url.get('href') for url in urls]
        return [u if u.startswith("http") else Utils().base_url(url)+u for u in urls]

    @staticmethod
    def path(conf):
        name = Box(json.loads(conf)).name
        ext = Box(json.loads(conf)).storage.type
        urls = DownloaderByUrlListToFile.urls(conf)
        directory = TEMP_PATH
        os.makedirs(directory, exist_ok=True)
        return [os.path.join(directory, "{}_{}.{}".format(name, i, ext)) for i, url in enumerate(urls)]


class DownloaderByUrlStatGovCompanies():

    def download(self, instance, fpath):
        try:
            for url, path in zip(self.urls(instance.srconf), fpath):
                result = requests.get(url, verify=False)
                with open(path, 'wb') as f:
                    f.write(result.content)
            return fpath
        except Exception as e:
            raise e

    @staticmethod
    def urls(conf):
        url = Box(json.loads(conf)).url
        base_url = Box(json.loads(conf)).base_url
        js = requests.get(url, verify=False).text
        regex = Box(json.loads(conf)).storage.html.url_regexp
        res = re.findall(regex, js)
        urls = ["{}{}".format(base_url, res) for res in res]
        return urls

    @staticmethod
    def path(conf):
        name = Box(json.loads(conf)).name
        ext = Box(json.loads(conf)).storage.type
        urls = DownloaderByUrlStatGovCompanies().urls(conf)
        directory = TEMP_PATH
        os.makedirs(directory, exist_ok=True)
        return [os.path.join(directory, "{}_{}.{}".format(name, i, ext)) for i, url in enumerate(urls)]


HandlersFactory.register("download_url", DownloaderByUrlToFile)
HandlersFactory.register("download_urllist_ajax", DownloaderByUrlStatGovCompanies)

