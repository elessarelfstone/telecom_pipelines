import json
import os
from datetime import datetime as dt

import luigi
from luigi.contrib.ftp import RemoteTarget, RemoteFileSystem
from box import Box

from pipelines.pipeline_xlsparse import ParseXLS, ParseXLSFromArchive, ParseXLSFromArchives
from pipelines.pipeline_parsers import ParseFromWebToCsv
from settings import JOBS_CONFIG_DIR, FTP_HOST, FTP_REMOTE_PATH, FTP_USER, FTP_PASS
from core.utils import Utils


class CopyFromFileToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseXLS(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        if Box(json.loads(job_conf)).gzip:
            ftp_path += '.gzip'
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        path = self.input().path
        if Box(json.loads(job_conf)).gzip:
            Utils().gzip(self.input().path)
            path += '.gzip'
        self.output().put(path, atomic=False)


class CopyFromArchToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseXLSFromArchive(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        if Box(json.loads(job_conf)).gzip:
            ftp_path += '.gzip'
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        path = self.input().path
        if Box(json.loads(job_conf)).gzip:
            Utils().gzip(self.input().path)
            path += '.gzip'
        self.output().put(path, atomic=False)


class CopyFromArchsToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseXLSFromArchives(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        if Box(json.loads(job_conf)).gzip:
            ftp_path += '.gzip'
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        path = self.input().path
        if Box(json.loads(job_conf)).gzip:
            Utils().gzip(self.input().path)
            path += '.gzip'
        self.output().put(path, atomic=False)


class CopyFromParsingToFtp(luigi.ExternalTask):

    sourcefile = luigi.Parameter()
    jobfile = luigi.Parameter()

    def requires(self):
        return ParseFromWebToCsv(sourcefile=self.sourcefile, jobfile=self.jobfile)

    def output(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        base = os.path.basename(self.input().path)
        day = dt.today().strftime("%Y%m%d")
        ftp_path = os.path.join(FTP_REMOTE_PATH, "{}_{}.{}".format(os.path.splitext(base)[0], day, Utils.ext(self.input().path)))
        if Box(json.loads(job_conf)).gzip:
            ftp_path += '.gzip'
        return RemoteTarget(ftp_path, FTP_HOST, username=FTP_USER, password=FTP_PASS)

    def run(self):
        job_file = os.path.join(JOBS_CONFIG_DIR, str(self.jobfile))
        job_conf = Utils.read_file(job_file)
        path = self.input().path
        if Box(json.loads(job_conf)).gzip:
            Utils().gzip(self.input().path)
            path += '.gzip'
        self.output().put(path, atomic=False)
