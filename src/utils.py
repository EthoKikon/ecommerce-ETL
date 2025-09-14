# src/utils.py
import logging

from src.config import LOG_LEVEL


def get_logger(name=__name__):
    logging.basicConfig(
        level=LOG_LEVEL,
        format=("%(asctime)s %(levelname)s %(name)s - " "%(message)s"),
    )
    return logging.getLogger(name)
