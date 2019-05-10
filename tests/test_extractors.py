import json
import re
import os
import unittest

from box import Box

import extractors
from settings import WEB_SOURCES_CONFIG_DIR, TEMP_PATH
from core.core import Extractor, Downloader, HandlersFactory
from core.utils import Utils


class TestStatGovExtract(unittest.TestCase):
    def setUp(self):
        self.directory = TEMP_PATH

    def test_kato_file_extract(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kato.json')
        json_raw = Utils.read_file(src_conf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, download_handler)
        file_path = service.download()
        extract_handler = HandlersFactory.get_handler(Extractor.handler_name(json_raw))
        service = Extractor(json_raw, file_path, extract_handler)
        file_paths = service.extract()
        self.assertTrue(Utils.all_exists(file_paths))

    def test_companies_files_extract(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_companies.json')
        json_raw = Utils.read_file(src_conf_path)
        # name = Box(json.loads(json_raw)).name
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, download_handler)
        downloaded_files_path = service.download()
        extract_handler = HandlersFactory.get_handler(Extractor.handler_name(json_raw))
        all_files = list()
        for f in downloaded_files_path:
            service = Extractor(json_raw, f, extract_handler)
            extracted_files_path = service.extract()
            all_files.extend(extracted_files_path)
        self.assertTrue(Utils.all_exists(all_files))
