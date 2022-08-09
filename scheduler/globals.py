from .logger import get_logger
from .singleton import Singleton


@Singleton
class Globals(object):
    def __init__(self):
        self._logger = get_logger()

    @property
    def logger(self):
        return self._logger


globals = Globals.Instance()
