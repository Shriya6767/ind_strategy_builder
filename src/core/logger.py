from src.core.modules import logging, RotatingFileHandler, os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


INFO_LOG_FILE = os.path.join(LOG_DIR, "info.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")


# Log Format
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s"
)

FORMATTER = logging.Formatter(LOG_FORMAT)


def get_logger(name: str) -> logging.Logger:

    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # ---------------- INFO HANDLER ----------------
    info_handler = RotatingFileHandler(
        INFO_LOG_FILE,
        maxBytes=10 * 1024 * 1024,   # 10 MB
        backupCount=5,
        encoding="utf-8"
    )

    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(FORMATTER)

    # ---------------- ERROR HANDLER ----------------
    error_handler = RotatingFileHandler(
        ERROR_LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )

    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(FORMATTER)

    # ---------------- CONSOLE HANDLER ----------------
    console_handler = logging.StreamHandler()

    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(FORMATTER)

    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger