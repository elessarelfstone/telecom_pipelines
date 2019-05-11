import os

import luigi

from core.core import Downloader, HandlersFactory
from core import utils
from settings import WEB_SOURCES_CONFIG_DIR, TEMP_PATH


class DownloadFile(luigi.Task):

    sourcefile = luigi.Parameter()

    def output(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, self.sourcefile)
        path = Downloader.path(utils.Utils.read_file(src_file))
        if isinstance(path, str):
            return luigi.LocalTarget(path)
        else:
            return [luigi.LocalTarget(pth) for pth in path]

    def run(self):
        src_file = os.path.join(WEB_SOURCES_CONFIG_DIR, self.sourcefile)
        with open(src_file, "r", encoding="utf8") as f:
            json_raw = f.read()
        handler = HandlersFactory.get_handler(Downloader.handler_name(json_raw))
        service = Downloader(json_raw, handler)
        service.download()
