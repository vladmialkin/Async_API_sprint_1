import logging
import logging.config
import sys
from os import getenv


def init_logging() -> None:
    log_config = dict(
            version=1,
            disable_existing_loggers=False,
            loggers={
                "main": {
                    "level": getenv('LOG_LEVEL', 'INFO').upper(),
                    "handlers": ["console", "main"],
                },
                "es": {
                    "level": getenv('LOG_LEVEL', 'INFO').upper(),
                    "handlers": ["console", "main"],
                },
                "postgres": {
                    "level": getenv('LOG_LEVEL', 'INFO').upper(),
                    "handlers": ["console", "main"],
                },
                "data_transform": {
                    "level": getenv('LOG_LEVEL', 'INFO').upper(),
                    "handlers": ["console", "main"],
                }
            },
            handlers={
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "generic",
                    "stream": sys.stdout,
                },
                "main": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "generic",
                    "filename": 'main.log',
                    "maxBytes": 5000000,
                    "backupCount": 3,
                }
            },
            formatters={
                "generic": {
                    "format": "%(asctime)s[%(name)s][%(levelname)s] %(message)s",
                    "datefmt": "[%Y-%m-%d %H:%M:%S]",
                    "class": "logging.Formatter",
                }
            }
        )

    logging.config.dictConfig(log_config)
