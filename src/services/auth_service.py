"""Signup (email + OTP), login (email + password), password reset (OTP by
email), Google sign-in, and the current user's profile.

Every method returns a dict with `status` (bool) and, on failure, a
`message` plus an `http_status` the route turns into the response code --
the routes stay thin, the rules live here.

Flow, matching the frontend screens:
  Signup screen  -> POST /auth/signup        (email, name, password)         -> code emailed
  Enter code     -> POST /auth/verify-otp    (email, otp)                    -> token
                    POST /auth/resend-otp    (email)
  Login screen   -> POST /auth/login         (email, password)               -> token
  Forgot pwd     -> POST /auth/forgot-password (email)                       -> code emailed
                    POST /auth/reset-password (email, otp, new_password)
  Google button  -> POST /auth/google        (id_token from Google)          -> token
"""
from src.core.modules import datetime, timedelta, RealDictCursor
from src.core.config import Database, GOOGLE_CLIENT_ID, OTP_DEV_MODE
from src.core.logger import get_logger
from src.core.security import (
    OTP_TTL_MINUTES, OTP_RESEND_SECONDS, OTP_MAX_ATTEMPTS,
    normalize_email, validate_password, hash_password, verify_password,
    create_access_token, generate_otp, hash_otp, otp_matches,
)
from src.core.otp_delivery import send_otp, OtpDeliveryError

logger = get_logger(__name__)

_USER_COLUMNS = ("user_id, email, name, password_hash, google_sub, is_email_verified, "
                 "is_active, created_at, last_login_at")


def _fail(message: str, http_status: int = 400) -> dict:
    return {"status": False, "message": message, "http_status": http_status}


def _public_user(row: dict) -> dict:
    return {
        "user_id": row["user_id"],
        "name": row["name"],
        "email": row.get("email"),
        "is_email_verified": bool(row.get("is_email_verified")),
        "created_at": row.get("created_at"),
    }


def _token_response(row: dict, message: str) -> dict:
    token, expires_in = create_access_token(row)
    return {
        "status": True,
        "message": message,
        "access_token": token,
        "token_type": "bearer",
        "expires_in": expires_in,
        "user": _public_user(row),
    }


def _is_verified(user: dict) -> bool:
    return bool(user.get("is_email_verified"))


class AuthService:

    @staticmethod
    def signup(request: dict) -> dict:
        try:
            email = normalize_email(request.get("email"))
            name = str(request.get("name") or "").strip()
            if not 2 <= len(name) <= 100:
                return _fail("Name must be 2-100 characters.")
            validate_password(request.get("password"))
        except ValueError as e:
            return _fail(str(e))

        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT {_USER_COLUMNS} FROM app_user WHERE email = %s;", (email,))
            existing = cursor.fetchone()
            if existing and _is_verified(existing):
                return _fail("This email is already registered. Please login.", 409)

            password_hash = hash_password(request["password"])
            if existing:
                # Signed up earlier but never entered the code: refresh the
                # details they just typed and send a new one.
                cursor.execute(
                    "UPDATE app_user SET name = %s, password_hash = %s WHERE user_id = %s;",
                    (name, password_hash, existing["user_id"]),
                )
            else:
                cursor.execute(
                    "INSERT INTO app_user (email, name, password_hash) VALUES (%s, %s, %s);",
                    (email, name, password_hash),
                )

            issued = AuthService._issue_otp(cursor, email, "signup")
            if not issued["status"]:
                conn.rollback()
                return issued
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"signup failed: {e}")
            return _fail("Could not start signup.", 500)
        finally:
            if conn:
                conn.close()

        return AuthService._deliver_otp(email, issued["otp"], "signup", "Verification code sent to your email.")


    @staticmethod
    def resend_otp(request: dict, purpose: str = "signup") -> dict:
        try:
            email = normalize_email(request.get("email"))
        except ValueError as e:
            return _fail(str(e))
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT {_USER_COLUMNS} FROM app_user WHERE email = %s;", (email,))
            user = cursor.fetchone()
            if not user:
                return _fail("No signup found for this email.", 404)
            if purpose == "signup" and _is_verified(user):
                return _fail("This email is already verified. Please login.", 409)
            issued = AuthService._issue_otp(cursor, email, purpose)
            if not issued["status"]:
                conn.rollback()
                return issued
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"resend_otp failed: {e}")
            return _fail("Could not resend the code.", 500)
        finally:
            if conn:
                conn.close()
        return AuthService._deliver_otp(email, issued["otp"], purpose, "Code re-sent.")


    @staticmethod
    def verify_signup_otp(request: dict) -> dict:
        """Correct code -> the account is verified and the user is logged in."""
        try:
            email = normalize_email(request.get("email"))
        except ValueError as e:
            return _fail(str(e))
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            check = AuthService._check_otp(cursor, email, str(request.get("otp") or ""), "signup")
            if not check["status"]:
                conn.commit()          # persists the attempt counter
                return check
            cursor.execute(
                f"UPDATE app_user SET is_email_verified = TRUE, last_login_at = NOW() "
                f"WHERE email = %s RETURNING {_USER_COLUMNS};",
                (email,),
            )
            user = cursor.fetchone()
            conn.commit()
            if not user:
                return _fail("No signup found for this email.", 404)
            return _token_response(user, "Email verified. You are logged in.")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"verify_signup_otp failed: {e}")
            return _fail("Could not verify the code.", 500)
        finally:
            if conn:
                conn.close()


    @staticmethod
    def login(request: dict) -> dict:
        """Email + password."""
        try:
            email = normalize_email(request.get("email"))
        except ValueError as e:
            return _fail(str(e))
        password = request.get("password") or ""
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT {_USER_COLUMNS} FROM app_user WHERE email = %s;", (email,))
            user = cursor.fetchone()
            # One message for "no such user" and "wrong password": telling
            # them apart would let anyone probe which emails have accounts.
            if not user or not verify_password(password, user.get("password_hash")):
                return _fail("Invalid email or password.", 401)
            if not user["is_active"]:
                return _fail("This account is disabled.", 403)
            if not _is_verified(user):
                return _fail("Email not verified yet. Please enter the code we sent you.", 403)
            cursor.execute("UPDATE app_user SET last_login_at = NOW() WHERE user_id = %s;", (user["user_id"],))
            conn.commit()
            return _token_response(user, "Login successful.")
        except Exception as e:
            logger.exception(f"login failed: {e}")
            return _fail("Could not login.", 500)
        finally:
            if conn:
                conn.close()


    @staticmethod
    def forgot_password(request: dict) -> dict:
        try:
            email = normalize_email(request.get("email"))
        except ValueError as e:
            return _fail(str(e))
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT {_USER_COLUMNS} FROM app_user WHERE email = %s;", (email,))
            user = cursor.fetchone()
            if not user or not _is_verified(user):
                return _fail("No account found for this email.", 404)
            issued = AuthService._issue_otp(cursor, email, "reset_password")
            if not issued["status"]:
                conn.rollback()
                return issued
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"forgot_password failed: {e}")
            return _fail("Could not start password reset.", 500)
        finally:
            if conn:
                conn.close()
        return AuthService._deliver_otp(email, issued["otp"], "reset_password", "Reset code sent to your email.")


    @staticmethod
    def reset_password(request: dict) -> dict:
        try:
            email = normalize_email(request.get("email"))
            validate_password(request.get("new_password"))
        except ValueError as e:
            return _fail(str(e))
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            check = AuthService._check_otp(cursor, email, str(request.get("otp") or ""), "reset_password")
            if not check["status"]:
                conn.commit()
                return check
            cursor.execute(
                "UPDATE app_user SET password_hash = %s WHERE email = %s;",
                (hash_password(request["new_password"]), email),
            )
            conn.commit()
            return {"status": True, "message": "Password updated. Please login with your new password."}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"reset_password failed: {e}")
            return _fail("Could not reset password.", 500)
        finally:
            if conn:
                conn.close()


    @staticmethod
    def google_login(request: dict) -> dict:
        """The browser gets an ID token from Google (Google Identity
        Services); we verify Google's signature on it and that it was
        issued for OUR client id, then trust the email/name inside."""
        if not GOOGLE_CLIENT_ID:
            return _fail("Google sign-in is not configured on the server.", 501)
        token = request.get("id_token")
        if not token:
            return _fail("id_token is required.")
        try:
            from google.oauth2 import id_token as google_id_token
            from google.auth.transport import requests as google_requests
            info = google_id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)
        except Exception as e:
            logger.warning(f"google token rejected: {e}")
            return _fail("Google sign-in could not be verified.", 401)
        if not info.get("email_verified", False):
            return _fail("Google account email is not verified.", 401)

        google_sub, email = info["sub"], normalize_email(info.get("email"))
        name = (info.get("name") or email.split("@")[0])[:100]
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT {_USER_COLUMNS} FROM app_user WHERE google_sub = %s OR email = %s;",
                           (google_sub, email))
            user = cursor.fetchone()
            if user:
                # Google has verified this email for us -- an unfinished
                # email/password signup with the same address is completed.
                cursor.execute(
                    "UPDATE app_user SET google_sub = COALESCE(google_sub, %s), is_email_verified = TRUE, "
                    f"last_login_at = NOW() WHERE user_id = %s RETURNING {_USER_COLUMNS};",
                    (google_sub, user["user_id"]),
                )
            else:
                cursor.execute(
                    "INSERT INTO app_user (email, name, google_sub, is_email_verified, last_login_at) "
                    f"VALUES (%s, %s, %s, TRUE, NOW()) RETURNING {_USER_COLUMNS};",
                    (email, name, google_sub),
                )
            user = cursor.fetchone()
            conn.commit()
            if not user["is_active"]:
                return _fail("This account is disabled.", 403)
            return _token_response(user, "Login successful.")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"google_login failed: {e}")
            return _fail("Could not complete Google sign-in.", 500)
        finally:
            if conn:
                conn.close()


    @staticmethod
    def me(user_id: int) -> dict:
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(f"SELECT {_USER_COLUMNS} FROM app_user WHERE user_id = %s;", (user_id,))
            user = cursor.fetchone()
            if not user:
                return _fail("User not found.", 404)
            return {"status": True, "user": _public_user(user)}
        finally:
            if conn:
                conn.close()


    @staticmethod
    def _issue_otp(cursor, target: str, purpose: str) -> dict:
        """Creates a fresh code for (target, purpose): enforces the resend
        cooldown, burns any older live code, stores only the keyed hash.
        Returns the plain code for delivery -- it never touches the DB."""
        cursor.execute(
            "SELECT created_at FROM otp_verification WHERE target = %s AND purpose = %s "
            "ORDER BY created_at DESC LIMIT 1;",
            (target, purpose),
        )
        last = cursor.fetchone()
        if last:
            since = (datetime.now() - last["created_at"]).total_seconds()
            if since < OTP_RESEND_SECONDS:
                return _fail(f"Please wait {int(OTP_RESEND_SECONDS - since)}s before requesting another code.", 429)
        cursor.execute(
            "UPDATE otp_verification SET consumed = TRUE WHERE target = %s AND purpose = %s AND NOT consumed;",
            (target, purpose),
        )
        otp = generate_otp()
        cursor.execute(
            "INSERT INTO otp_verification (target, purpose, otp_hash, expires_at) VALUES (%s, %s, %s, %s);",
            (target, purpose, hash_otp(target, otp), datetime.now() + timedelta(minutes=OTP_TTL_MINUTES)),
        )
        return {"status": True, "otp": otp}


    @staticmethod
    def _check_otp(cursor, target: str, otp: str, purpose: str) -> dict:
        cursor.execute(
            "SELECT otp_id, otp_hash, expires_at, attempts FROM otp_verification "
            "WHERE target = %s AND purpose = %s AND NOT consumed ORDER BY created_at DESC LIMIT 1;",
            (target, purpose),
        )
        row = cursor.fetchone()
        if not row:
            return _fail("No active code for this email. Please request a new one.", 400)
        if datetime.now() > row["expires_at"]:
            return _fail("The code has expired. Please request a new one.", 400)
        if row["attempts"] >= OTP_MAX_ATTEMPTS:
            return _fail("Too many wrong attempts. Please request a new code.", 429)
        if not otp_matches(target, otp, row["otp_hash"]):
            cursor.execute("UPDATE otp_verification SET attempts = attempts + 1 WHERE otp_id = %s;", (row["otp_id"],))
            remaining = OTP_MAX_ATTEMPTS - row["attempts"] - 1
            return _fail(f"Incorrect code. {remaining} attempt(s) left.", 400)
        cursor.execute("UPDATE otp_verification SET consumed = TRUE WHERE otp_id = %s;", (row["otp_id"],))
        return {"status": True}


    @staticmethod
    def _deliver_otp(target: str, otp: str, purpose: str, message: str) -> dict:
        """Send after the transaction committed, so a delivered code always
        has a matching row. OTP_DEV_MODE=true echoes the code in the
        response for frontend/Postman testing without an email account."""
        try:
            send_otp(target, otp, purpose)
        except OtpDeliveryError as e:
            return _fail(f"Code could not be sent: {e}", 502)
        out = {"status": True, "message": message, "email": target,
               "otp_expires_in": OTP_TTL_MINUTES * 60, "resend_after": OTP_RESEND_SECONDS}
        if OTP_DEV_MODE:
            out["dev_otp"] = otp
        return out