from dotenv import load_dotenv

load_dotenv()

from scheduler.main import run_scheduler

if __name__ == '__main__':
    run_scheduler()
