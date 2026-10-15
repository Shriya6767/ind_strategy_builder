from src.core.modules import logging, os, date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Log Format
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s"
)

FORMATTER = logging.Formatter(LOG_FORMAT)


class DailyFileHandler(logging.FileHandler):
    """One log file per calendar day -- logs/YYYY-MM-DD.log -- switched the
    moment the first record of a new day is written."""

    def __init__(self, directory: str, encoding: str = "utf-8"):
        self.directory = directory
        self._day = date.today()
        super().__init__(self._path(), encoding=encoding)

    def _path(self) -> str:
        return os.path.join(self.directory, f"{self._day:%Y-%m-%d}.log")

    def emit(self, record):
        today = date.today()
        if today != self._day:
            self.acquire()
            try:
                self._day = today
                if self.stream:
                    self.stream.close()
                    self.stream = None
                self.baseFilename = os.path.abspath(self._path())
            finally:
                self.release()
        super().emit(record)


def get_logger(name: str) -> logging.Logger:

    logger = logging.getLogger(name)
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    file_handler = DailyFileHandler(LOG_DIR)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(FORMATTER)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(FORMATTER)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger