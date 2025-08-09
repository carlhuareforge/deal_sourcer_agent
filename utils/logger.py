import logging
import os
from datetime import datetime

class Logger:
    def __init__(self, log_dir="logs"):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"app_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self._logger = logging.getLogger(__name__)

    def log(self, message):
        self._logger.info(message)

    def error(self, message, *args, **kwargs):
        self._logger.error(message, *args, **kwargs)

    def warn(self, message):
        self._logger.warning(f"⚠️ {message}")

    def debug(self, message):
        self._logger.debug(message)

    def success(self, message):
        self._logger.info(f"✅ {message}")

# Initialize a global logger instance
logger = Logger(log_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs'))