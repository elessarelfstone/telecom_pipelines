import os
from os import path
from os.path import expanduser

from dotenv import load_dotenv

ROOT = path.abspath(path.dirname(__file__))
ENVS_ROOT = path.join(ROOT, 'env')

DEV_ENV_DIR = path.join(ENVS_ROOT, 'dev', '.env')
PROD_ENV_DIR = path.join(ENVS_ROOT, 'prod', '.env')

os.environ["ENV"] = "dev"

if os.environ["ENV"] == "dev":
    load_dotenv(DEV_ENV_DIR)
else:
    load_dotenv(PROD_ENV_DIR)


TEMP_PATH = path.join(expanduser('~'), os.getenv("TEMP_DIR"))
DATA_PATH = path.join(expanduser('~'), os.getenv("DATA_DIR"))

SOURCES_CONFIG_DIR = path.join(ROOT, 'sources')
WEB_SOURCES_CONFIG_DIR = path.join(SOURCES_CONFIG_DIR, 'web')

TESTS_ROOT = path.join(ROOT, 'tests')




