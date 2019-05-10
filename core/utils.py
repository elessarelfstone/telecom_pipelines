import os
import uuid
import zipfile
from pathlib import Path
from urllib.parse import urlparse

from rarfile import RarFile


class Utils():
    @staticmethod
    def read_file(file):
        with open(file, "r", encoding="utf8") as f:
            result = f.read()
        return result

    @staticmethod
    def base_url(url):
        parsed_uri = urlparse(url)
        result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return result

    @staticmethod
    def ext(file):
        suffix = Path(file).suffix
        return suffix[1:len(suffix)]

    @staticmethod
    def get_archive_object(file):
        if Utils.ext(file) == 'rar':
            archive = RarFile(file)
        elif Utils.ext(file) == 'zip':
            archive = zipfile.ZipFile(file)
        return archive

    @staticmethod
    def all_exists(paths):
        result = True
        for path in paths:
            if not os.path.exists(path):
                result = False
                break
        return result

    @staticmethod
    def uuid(chars_num=23):
        return str(uuid.uuid4())[:chars_num]

