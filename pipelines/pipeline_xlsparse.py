import os

import luigi

from core.core import XLSParser, HandlersFactory
from core.utils import Utils
from settings import WEB_SOURCES_CONFIG_DIR, JOBS_CONFIG_DIR, WEB_DATA_PATH
from pipelines.pipeline_download import DownloadFile
from pipelines.pipeline_extract import ExtractFile, ExtractFiles


class ParseXLS(luigi.Task):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return DownloadFile(sourcefile=self.sourcefile)

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        return luigi.LocalTarget(XLSParser.path(src_conf, job_conf, WEB_DATA_PATH))

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        handler = HandlersFactory.get_handler(XLSParser.handler_name(src_conf, job_conf))
        service = XLSParser(src_conf, job_conf, self.input().path, WEB_DATA_PATH, handler)
        service.parse()


class ParseXLSFromArchive(luigi.Task):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ExtractFile(sourcefile=self.sourcefile)

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        return luigi.LocalTarget(XLSParser.path(src_conf, job_conf, WEB_DATA_PATH))

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        handler = HandlersFactory.get_handler(XLSParser.handler_name(src_conf, job_conf))
        service = XLSParser(src_conf, job_conf, [f.path for f in self.input()], WEB_DATA_PATH, handler)
        service.parse()


class ParseXLSFromArchives(luigi.Task):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ExtractFiles(sourcefile=self.sourcefile)

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        return luigi.LocalTarget(XLSParser.path(src_conf, job_conf, WEB_DATA_PATH))

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        src_conf = Utils.read_file(src_file)
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        handler = HandlersFactory.get_handler(XLSParser.handler_name(src_conf, job_conf))
        service = XLSParser(src_conf, job_conf, [f.path for f in self.input()], WEB_DATA_PATH, handler)
        service.parse()
