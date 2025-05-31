import logging
from logging.handlers import RotatingFileHandler
import os


class LOGGER:
    def __init__(self, file_path):

        self.log_handler = RotatingFileHandler(
            file_path,
            maxBytes=1_000_000,  
            backupCount=5        
        )
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        self.log_handler.setFormatter(formatter)

        self.logger = logging.getLogger("cache_base")
        self.logger.setLevel(logging.INFO)


        if not any(isinstance(h, RotatingFileHandler) and h.baseFilename == os.path.abspath(file_path)
                   for h in self.logger.handlers):
            self.logger.addHandler(self.log_handler)

    def debug(self, text):
        self.logger.debug(text)

    def info(self, text):
        self.logger.info(text)

    def warning(self, text):
        self.logger.warning(text)

    def error(self, text):
        self.logger.error(text)

    def critical(self, text):
        self.logger.critical(text)

    def exception(self, text):
        self.logger.exception(text)

    def close(self):
        handlers = self.logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)
