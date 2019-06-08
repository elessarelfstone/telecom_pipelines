import os
from os import path
from os.path import expanduser

from dotenv import load_dotenv

ROOT = path.abspath(path.dirname(__file__))

load_dotenv()

# paths for data
TEMP_PATH = path.join(expanduser('~'), os.getenv("TEMP_DIR"))
DATA_PATH = path.join(expanduser('~'), os.getenv("DATA_DIR"))
WEB_DATA_PATH = path.join(DATA_PATH, "web")

# ftp settings
FTP_REMOTE_PATH = os.getenv("FTP_PATH")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")

# paths for configs

SOURCES_CONFIG_DIR = path.join(ROOT, 'sources')
WEB_SOURCES_CONFIG_DIR = path.join(SOURCES_CONFIG_DIR, 'web')
JOBS_CONFIG_DIR = path.join(ROOT, 'jobs')

# path for tests
TESTS_ROOT = path.join(ROOT, 'tests')



