import re
import os
import unittest

from settings import WEB_SOURCES_CONFIG_DIR, TEMP_PATH
from core.core import Downloader, HandlersFactory


class TestStatGovDownload(unittest.TestCase):
    def setUp(self):
        self.directory = TEMP_PATH
        statgov_files = os.listdir(WEB_SOURCES_CONFIG_DIR)
        statgov_files = filter(lambda x: re.search("^[^_]+_statgov", x), statgov_files)
        statgov_files = [os.path.join(self.directory, f) for f in statgov_files]
        for f in statgov_files:
            if os.path.exists(f):
                os.remove(f)

    def test_oked_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_oked.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_nved_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_nved.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_mkeis_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_mkeis.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_kurk_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kurk.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_kpved_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kpved.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_kato_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kato.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
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

        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_companies.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_paths = service.download()
        self.assertTrue(_all_downloaded(file_paths))


class TestKgdGovDownload(unittest.TestCase):
    def setUp(self):
        self.directory = TEMP_PATH
        statgov_files = os.listdir(WEB_SOURCES_CONFIG_DIR)
        statgov_files = filter(lambda x: re.search("^[^_]+_kgdgov", x), statgov_files)
        statgov_files = [os.path.join(self.directory, f) for f in statgov_files]
        for f in statgov_files:
            if os.path.exists(f):
                os.remove(f)

    def test_bankrupt_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_bankrupt.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_inactive_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_inactive.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_invalid_registration_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_invalid_registration.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_pseudo_company_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_pseudo_company.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_tax_arrears_150_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_tax_arrears_150.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_violation_tax_code_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_violation_tax_code.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))

    def test_wrong_address_download_by_url(self):
        src_conf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_wrong_address.json')
        with open(src_conf_path, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, self.directory, handler)
        file_path = service.download()
        self.assertTrue(os.path.exists(file_path))
