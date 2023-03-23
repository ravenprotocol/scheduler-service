import json
import os
import pickle as pkl
import requests
import numpy as np
import logging
from .config import DATA_FILES_PATH, \
RAVENVERSE_SUPERUSER_TOKEN, RAVENVERSE_SUPERUSER_USERNAME, \
RAVENVERSE_SUPERUSER_PASSWORD, RAVENAUTH_TOKEN_GET_URL, RAVENAUTH_TOKEN_VERIFY_URL


def load_data_from_file(file_path, np=None):
    # print("File path:", file_path)
    with open(file_path, 'rb') as f:
        x = pkl.load(f)
    return np.array(x)


def delete_data_file(data_id):
    file_path = os.path.join(DATA_FILES_PATH, f'data_{data_id}.json')
    if os.path.exists(file_path):
        os.remove(file_path)


def save_data_to_file(data_id, data):
    """
    Method to save data in a pickle file
    """
    file_path = os.path.join(DATA_FILES_PATH, f'data_{data_id}.json')

    if os.path.exists(file_path):
        os.remove(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as f:
        if isinstance(data, np.ndarray):
            data = data.tolist()
        json.dump(data, f)

    return file_path

def get_access_token():
    response = requests.post(url="{}".format(RAVENAUTH_TOKEN_GET_URL),
                             data={"username": RAVENVERSE_SUPERUSER_USERNAME,
                                   "password": RAVENVERSE_SUPERUSER_PASSWORD})
    if response.status_code == 200:
        return response.json()['access']
    else:
        logging.debug(response.text)
        return None

def verify_and_get_access_token():
    response = requests.post(url="{}".format(RAVENAUTH_TOKEN_VERIFY_URL),
                             data={"token": RAVENVERSE_SUPERUSER_TOKEN})

    if response.status_code == 200:
        return RAVENVERSE_SUPERUSER_TOKEN
    else:
        logging.debug("Invalid access token, getting new access token")
        return get_access_token()