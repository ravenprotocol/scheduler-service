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

RAVENAUTH_URL = str(os.environ.get('RAVENAUTH_URL'))
RAVENAUTH_TOKEN_VERIFY_URL = os.path.join(RAVENAUTH_URL, 'api/token/verify/')
RAVENAUTH_GET_USER_URL = os.path.join(RAVENAUTH_URL, 'auth/user/get')
RAVENAUTH_WALLET = os.path.join(RAVENAUTH_URL, 'auth/wallet/')
RAVENAUTH_WALLET_TRANSACTION = os.path.join(RAVENAUTH_URL, 'auth/wallet/transaction/')
RAVENVERSE_SUPERUSER_TOKEN = os.environ.get('RAVENVERSE_SUPERUSER_TOKEN')
RAVENVERSE_SUPERUSER_USERNAME = os.environ.get('RAVENVERSE_SUPERUSER_USERNAME')
RAVENVERSE_SUPERUSER_PASSWORD = os.environ.get('RAVENVERSE_SUPERUSER_PASSWORD')