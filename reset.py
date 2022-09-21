from __future__ import annotations
from scheduler import reset_database

from dotenv import load_dotenv

load_dotenv()


if __name__ == '__main__':
    reset_database()
