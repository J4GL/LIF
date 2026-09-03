"""Logging configuration for the DHT scraper."""
import logging
from typing import Optional

LOGGER_NAME = "dht_scraper"
LOG_FORMAT = "%(asctime)s %(levelname)s [%(threadName)s] %(message)s"


# Parents: main
# Keywords: logging, handlers, log file, verbose
def configure_logging(log_file: Optional[str], verbose: bool) -> logging.Logger:
    assert log_file is None or isinstance(log_file, str)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    assert len(logger.handlers) >= 1
    return logger
