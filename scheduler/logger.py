from __future__ import annotations

import logging.handlers

from .config import LOG_FILE_PATH


def get_logger():
    # Set up a specific logger with our desired output level
    logger = logging.getLogger(__name__)
    log_formatter = logging.Formatter(
        '%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Add the log message handler to the logger
    file_handler = logging.handlers.RotatingFileHandler(LOG_FILE_PATH)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    return logger
