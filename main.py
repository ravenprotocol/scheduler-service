from __future__ import annotations
from scheduler.main import run_scheduler

from dotenv import load_dotenv

load_dotenv()


if __name__ == '__main__':
    run_scheduler()
