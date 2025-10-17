"""Centralized logger for DataFlow project."""
from logging import Logger
import logging
import sys


def get_logger(name: str = "dataflow", level: int = logging.INFO) -> Logger:
    """Return a configured logger.

    Structured formatter and stream handler are added.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
