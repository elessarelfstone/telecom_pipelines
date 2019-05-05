import os
from os import path
from dotenv import load_dotenv

TESTS_ROOT=path.join(path.abspath(path.dirname(__file__)))
# SOURCES_CONFIG_ROOT = path.join(TESTS_ROOT, 'sources')
SOURCES_CONFIG_ROOT = path.abspath(path.join(path.dirname(__file__), os.pardir, 'sources'))
WEB_SOURCES_ROOT = path.join(SOURCES_CONFIG_ROOT, 'web')

ENVS_ROOT = path.abspath(path.join(path.dirname(__file__), os.pardir, 'env'))
DEV_ENV = path.join(ENVS_ROOT, 'dev', '.env')

load_dotenv(DEV_ENV)


