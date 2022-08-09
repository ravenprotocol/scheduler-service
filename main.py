from dotenv import load_dotenv

load_dotenv()

from scheduler.main import run_scheduler
from scheduler.globals import globals

if __name__ == '__main__':
    globals.logger.debug("hello")

    run_scheduler()
