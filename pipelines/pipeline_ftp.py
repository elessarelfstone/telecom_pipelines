import os
from datetime import datetime as dt

import luigi
from luigi.contrib.ftp import RemoteTarget, RemoteFileSystem

from pipelines.pipeline_xlsparse import ParseXLS, ParseXLSFromArchive, ParseXLSFromArchives
from settings import FTP_HOST, FTP_REMOTE_PATH, FTP_USER, FTP_PASS
from core.utils import Utils


class CopyFromFileToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseXLS(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        self.output().put(self.input().path, atomic=False)


class CopyFromArchToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseXLSFromArchive(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        self.output().put(self.input().path, atomic=False)


class CopyFromArchsToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseXLSFromArchives(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        self.output().put(self.input().path, atomic=False)
