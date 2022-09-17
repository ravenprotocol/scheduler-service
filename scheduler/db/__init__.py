from __future__ import annotations

from .manager import DBManager
from .models import Client
from .models import ClientOpMapping
from .models import Data
from .models import Graph
from .models import Objective
from .models import ObjectiveClientMapping
from .models import Op

ravdb = DBManager()


def reset_database():
    ravdb.drop_database()
    ravdb.create_database()
    ravdb.create_tables()
