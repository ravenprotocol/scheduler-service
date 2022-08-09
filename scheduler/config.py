import pathlib

import os

FTP_FILES_PATH = os.path.join(pathlib.Path(__file__).parent.parent.parent.resolve(), "ravftp/files")
MINIMUM_SPLIT_SIZE = 100
PROJECT_DIR = pathlib.Path(__file__).parent.parent.resolve()
LOG_FILE_PATH = os.path.join(PROJECT_DIR, "debug.log")
RAVENVERSE_DATABASE_URI = os.environ.get("RAVENVERSE_DATABASE_URI")
DATA_FILES_PATH = os.path.join(PROJECT_DIR, "files")

# redis
RAVENVERSE_REDIS_HOST = os.environ.get("RAVENVERSE_REDIS_HOST")
RAVENVERSE_REDIS_PORT = os.environ.get("RAVENVERSE_REDIS_PORT")
RAVENVERSE_REDIS_DB = os.environ.get("RAVENVERSE_REDIS_DB")
