import os
import re
import unittest
import utils
from core.core import Downloader, HandlersFactory


class TestStatGovDownload(unittest.TestCase):
    def setUp(self):
        directory = os.getenv("TEMP_DIR")
        statgov_files = os.listdir(utils.WEB_SOURCES_ROOT)
        statgov_files = filter(lambda x: re.search("^[^_]+_statgov", x), statgov_files)
        statgov_files = [os.path.join(directory, f) for f in statgov_files]
        for f in statgov_files:
            if os.path.exists(f):
                os.remove(f)

    def test_oked_download_by_url(self):
        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_oked.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_nved_download_by_url(self):
        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_nved.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_mkeis_download_by_url(self):
        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_mkeis.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_kurk_download_by_url(self):
        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_kurk.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_kpved_download_by_url(self):
        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_kpved.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_kato_download_by_url(self):
        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_kato.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_companies_download_by_urllist(self):

        def _all_downloaded(paths):
            result = True
            for path in paths:
                if not os.path.exists(path):
                    result = False
                    break
            return result

        src_conf_path = os.path.join(utils.WEB_SOURCES_ROOT, 'web_statgov_companies.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        file_paths = service.download()
        self.assertTrue(_all_downloaded(file_paths))

