import os

import luigi

from core.core import APIParser, HandlersFactory
from core.utils import Utils
from settings import WEB_SOURCES_CONFIG_DIR, JOBS_CONFIG_DIR, WEB_DATA_PATH


class ParseElkApiFromArchives(luigi.Task):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        return luigi.LocalTarget(APIParser.path(src_conf, job_conf, WEB_DATA_PATH))

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        handler = HandlersFactory.get_handler(APIParser.handler_name(job_conf))
        service = APIParser(src_conf, job_conf, WEB_DATA_PATH, handler)
        service.parse()
