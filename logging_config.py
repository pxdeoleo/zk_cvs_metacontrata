import logging.config
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

def setup_logging(log_dir: str = "logs") -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sync_{datetime.now():%Y-%m-%d}.log")

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            },
        },

        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": "INFO",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "standard",
                "filename": log_file,
                "encoding": "utf8",
                "mode": "a",
                "level": "INFO",
            },
        },

        "root": {
            "handlers": ["console", "file"],
            "level": "INFO",
        },
    }

    logging.config.dictConfig(logging_config)
