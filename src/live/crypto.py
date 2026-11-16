"""Encryption at rest for broker API keys, secrets and session tokens.

Fernet (AES-128-CBC + HMAC, from the `cryptography` package): the database
holds only ciphertext, so a DB dump cannot be used to trade on anyone's
account. The key comes from BROKER_SECRET_KEY; when that is unset one is
derived from JWT_SECRET so development works without extra setup.
"""
from src.core.modules import Fernet, InvalidToken, hashlib, base64
from src.core import config
from src.core.security import JWT_SECRET
from src.core.logger import get_logger

logger = get_logger(__name__)


def _build_fernet() -> Fernet:
    if config.BROKER_SECRET_KEY:
        return Fernet(config.BROKER_SECRET_KEY.encode("utf-8"))
    derived = base64.urlsafe_b64encode(hashlib.sha256(JWT_SECRET.encode("utf-8")).digest())
    logger.warning("BROKER_SECRET_KEY is not set -- deriving the broker key from JWT_SECRET. "
                   "Set a dedicated Fernet key for production.")
    return Fernet(derived)


_fernet = _build_fernet()


def encrypt(value) -> str | None:
    if value is None or value == "":
        return None
    return _fernet.encrypt(str(value).encode("utf-8")).decode("utf-8")


def decrypt(token) -> str | None:
    if not token:
        return None
    try:
        return _fernet.decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        raise ValueError("Stored broker credential cannot be decrypted (BROKER_SECRET_KEY changed?).")


def mask(value: str | None, keep: int = 4) -> str | None:
    """'abcd1234efgh' -> '********efgh' for the broker list UI."""
    if not value:
        return None
    return "*" * max(len(value) - keep, 4) + value[-keep:]
