import os
import unittest


from settings import WEB_SOURCES_CONFIG_DIR, JOBS_CONFIG_DIR, WEB_DATA_PATH, TEMP_PATH
from core.core import XLSParser, Extractor, Downloader, HandlersFactory, Parser
from core.utils import Utils


class TestStatGovParse(unittest.TestCase):

    def setUp(self):
        self.temp_path = TEMP_PATH
        self.data_path = WEB_DATA_PATH

    def test_oked_old_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_oked_old.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_oked_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_oked.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_nved_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_nved.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_mkeis_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_mkeis.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_kurk_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kurk.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_kpved_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kpved.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_kato_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_kato.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        extract_handler = HandlersFactory.get_handler(Extractor.handler_name(src_json))
        service = Extractor(src_json, downloaded_file, self.temp_path, extract_handler)
        service.extract()
        xlspaths = service.path(src_json, self.temp_path)
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, xlspaths, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_companies_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_statgov_companies.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        extract_handler = HandlersFactory.get_handler(Extractor.handler_name(src_json))
        service = Extractor(src_json, downloaded_file, self.temp_path, extract_handler)
        service.extract()
        xlspaths = service.path(src_json, self.temp_path)
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(src_json, job_json))
        service = XLSParser(src_json, job_json, xlspaths, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)


class TestKgdGovParse(unittest.TestCase):
    def setUp(self):
        self.temp_path = TEMP_PATH
        self.data_path = WEB_DATA_PATH

    def test_bunkrupt_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_bankrupt.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_inactive_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_inactive.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_invalid_registration_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_inactive.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_pseudo_company_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_pseudo_company.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_tax_arrears_150_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_tax_arrears_150.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_violation_tax_code_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_violation_tax_code.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_wrong_address_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_wrong_address.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        download_handler = HandlersFactory.get_handler(Downloader.handler_name(src_json))
        service = Downloader(src_json, download_handler)
        downloaded_file = service.download()
        parse_handler = HandlersFactory.get_handler(XLSParser.handler_name(job_json))
        service = XLSParser(src_json, job_json, downloaded_file, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        rows_cnt = service.parse()
        self.assertTrue(os.path.exists(csvfile))
        self.assertGreater(rows_cnt, 0)

    def test_refinance_rate_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_kgdgov_refinance_rate.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        parse_handler = HandlersFactory.get_handler(Parser.handler_name(src_json, job_json))
        service = Parser(src_json, job_json, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        service.parse()
        self.assertTrue(os.path.exists(csvfile))

class TestGosReestr(unittest.TestCase):
    def setUp(self):
        self.data_path = WEB_DATA_PATH

    def test_gos_reestr(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_gosreestr.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        parse_handler = HandlersFactory.get_handler(Parser.handler_name(src_json, job_json))
        service = Parser(src_json, job_json, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        service.parse()
        self.assertTrue(os.path.exists(csvfile))
