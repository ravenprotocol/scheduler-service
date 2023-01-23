from .manager import DBManager
from .models import (
    Op,
    Graph,
    Data,
    Client,
    ClientOpMapping,
    ObjectiveClientMapping,
    Objective,
)

ravdb = DBManager()


def reset_database():
    ravdb.drop_database()
    ravdb.create_database()
    ravdb.create_tables()
