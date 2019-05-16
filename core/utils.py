import os
import uuid
import gzip
import shutil
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
            # Utils.ext(file) == 'zip':
        else:
            archive = zipfile.ZipFile(file)
        # print(archive)
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

    @staticmethod
    def gzip(file):
        gzip_file = file + '.gzip'
        with open(file, 'rb') as f_in:
            with gzip.open(gzip_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return gzip_file

