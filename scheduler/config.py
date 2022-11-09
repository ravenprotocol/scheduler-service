from __future__ import annotations

import os
import pathlib

FTP_FILES_PATH = os.path.join(pathlib.Path(
    __file__).parent.parent.parent.resolve(), 'ravftp/files')
MINIMUM_SPLIT_SIZE = 100
PROJECT_DIR = pathlib.Path(__file__).parent.parent.resolve()
LOG_FILE_PATH = os.path.join(PROJECT_DIR, 'debug.log')
RAVENVERSE_DATABASE_URI = os.environ.get('RAVENVERSE_DATABASE_URI')
DATA_FILES_PATH = os.path.join(PROJECT_DIR, 'files')
