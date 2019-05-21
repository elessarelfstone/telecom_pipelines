import os
import unittest


from settings import WEB_SOURCES_CONFIG_DIR, JOBS_CONFIG_DIR, WEB_DATA_PATH, TEMP_PATH
from core.core import HandlersFactory, Parser
from core.utils import Utils


class TestDataGovParseAPI(unittest.TestCase):
    def setUp(self):
        self.temp_path = TEMP_PATH
        self.data_path = WEB_DATA_PATH

    def test_address_parse_to_csv(self):
        srconf_path = os.path.join(WEB_SOURCES_CONFIG_DIR, 'web_datagov_addresses.json')
        jobconf_path = os.path.join(JOBS_CONFIG_DIR, 'to_csv.json')
        src_json = Utils.read_file(srconf_path)
        job_json = Utils.read_file(jobconf_path)
        parse_handler = HandlersFactory.get_handler(Parser.handler_name(src_json, job_json))
        service = Parser(src_json, job_json, self.data_path, parse_handler)
        csvfile = service.path(src_json, job_json, self.data_path)
        service.parse()
        self.assertTrue(os.path.exists(csvfile))
