"""Authentication primitives: email normalization, password hashing,
login tokens, one-time codes, and the FastAPI dependency that turns an
incoming request into "which user is this".

Two ideas carry everything here:

* Passwords are never stored. bcrypt turns a password into a one-way
  hash ("$2b$12$..."); at login the typed password is hashed the same way
  and the two hashes are compared. Even with a full copy of the database
  nobody can recover the passwords, and bcrypt is deliberately slow
  (~100 ms) so guessing millions of them is impractical.

* After login the server hands the client a JWT (JSON Web Token): a small
  signed JSON payload {user_id, name, email, expiry}. The client sends it
  back on every request in the Authorization header; the server verifies
  the signature with its secret and trusts the payload without a database
  lookup. No server-side session table, and every worker process can
  verify tokens independently.
"""
from src.core.modules import (
    bcrypt, jwt, secrets, hashlib, hmac, datetime, timedelta, timezone, re,
    HTTPException, Header,
)
from src.core import config
from src.core.logger import get_logger

logger = get_logger(__name__)

OTP_TTL_MINUTES = 5          # a code is valid for this long
OTP_RESEND_SECONDS = 30      # minimum gap between two codes to one number
OTP_MAX_ATTEMPTS = 5         # wrong guesses before the code is burnt
PASSWORD_MIN_LENGTH = 8
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_HOURS = config.JWT_EXPIRES_HOURS

# The key that signs every login token. Losing it means every token is
# invalid (everyone logs in again); leaking it means anyone can forge
# tokens -- keep it in .env, never in git. Without one we generate a
# random key per process start so development works, but tokens then
# die on every restart, hence the warning.
JWT_SECRET = config.JWT_SECRET
if not JWT_SECRET:
    JWT_SECRET = secrets.token_hex(32)
    logger.warning("JWT_SECRET is not set in .env -- using a random per-process secret; "
                   "logins will not survive a restart. Set JWT_SECRET for production.")


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$")


def normalize_email(raw) -> str:
    """Lower-cased, trimmed; rejects anything that is not shaped like an
    address. Stored lower-case so Alice@X.com and alice@x.com are one user."""
    email = str(raw or "").strip().lower()
    if not _EMAIL_RE.match(email) or len(email) > 255:
        raise ValueError("Enter a valid email address.")
    return email


def validate_password(password: str) -> None:
    if not isinstance(password, str) or len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"Password must be at least {PASSWORD_MIN_LENGTH} characters.")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    if not password_hash:
        return False
    return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


def create_access_token(user: dict) -> tuple[str, int]:
    """Signed token carrying the user's identity; returns (token, seconds
    until expiry). `sub` (subject) is the standard claim for 'who'."""
    now = datetime.now(timezone.utc)
    expires_in = JWT_EXPIRES_HOURS * 3600
    payload = {
        "sub": str(user["user_id"]),
        "name": user.get("name"),
        "email": user.get("email"),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM), expires_in


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


def get_current_user(authorization: str = Header(default=None)) -> dict:
    """FastAPI dependency: add `user=Depends(get_current_user)` to a route
    and it only runs for a valid `Authorization: Bearer <token>` header;
    otherwise the client gets 401 before the route body executes. The
    returned dict is the token's payload -- no database round-trip per
    request."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"status": False, "message": "Login required."})
    token = authorization.split(" ", 1)[1].strip()
    try:
        claims = decode_access_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail={"status": False, "message": "Session expired, please login again."})
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail={"status": False, "message": "Invalid session, please login again."})
    return {"user_id": int(claims["sub"]), "name": claims.get("name"), "email": claims.get("email")}


def generate_otp() -> str:
    """6 digits from the OS's cryptographic random source (never `random`,
    whose output is predictable)."""
    return f"{secrets.randbelow(10 ** 6):06d}"


def hash_otp(target: str, otp: str) -> str:
    """The database holds only this keyed hash of the code (target = the
    email it was sent to). A plain hash of a 6-digit number could
    be reversed by trying all million values, so the server secret is
    mixed in (HMAC) -- without it the table is useless to an attacker."""
    return hmac.new(JWT_SECRET.encode("utf-8"), f"{target}:{otp}".encode("utf-8"), hashlib.sha256).hexdigest()


def otp_matches(target: str, otp: str, otp_hash: str) -> bool:
    # compare_digest takes the same time whether the first or last byte
    # differs, so response timing leaks nothing about the code.
    return hmac.compare_digest(hash_otp(target, str(otp).strip()), otp_hash or "")