from dotenv import load_dotenv

load_dotenv()

from scheduler import reset_database

if __name__ == '__main__':
    reset_database()
