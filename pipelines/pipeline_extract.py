import os

import luigi

from core.core import Extractor, HandlersFactory
from core.utils import Utils
from settings import WEB_SOURCES_CONFIG_DIR, TEMP_PATH
from pipelines.pipeline_download import DownloadFile


class ExtractFile(luigi.Task):

    sourcefile = luigi.Parameter()

    def requires(self):
        return DownloadFile(sourcefile=self.sourcefile)

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        json_raw = Utils.read_file(src_file)
        handler = HandlersFactory.get_handler(Extractor.handler_name(json_raw))
        service = Extractor(json_raw, self.input().path, TEMP_PATH, handler)
        return [luigi.LocalTarget(f) for f in service.path(json_raw, TEMP_PATH)]

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, str(self.sourcefile))
        json_raw = Utils.read_file(src_file)
        handler = HandlersFactory.get_handler(Extractor.handler_name(json_raw))
        service = Extractor(json_raw, self.input().path, TEMP_PATH, handler)
        service.extract()


class ExtractFiles(luigi.Task):

    sourcefile = luigi.Parameter()

    def requires(self):
        return DownloadFile(sourcefile=self.sourcefile)

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, self.sourcefile)
        json_raw = Utils.read_file(src_file)
        handler = HandlersFactory.get_handler(Extractor.handler_name(json_raw))
        service = Extractor(json_raw, [lt.path for lt in self.input()], TEMP_PATH, handler)
        return [luigi.LocalTarget(f) for f in service.path(json_raw, TEMP_PATH)]

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, self.sourcefile)
        json_raw = Utils.read_file(src_file)
        handler = HandlersFactory.get_handler(Extractor.handler_name(json_raw))
        service = Extractor(json_raw, [lt.path for lt in self.input()], TEMP_PATH,  handler)
        service.extract()




