"""Delivers a one-time code to the user's email. OTP_PROVIDER (.env):

  console  log the code (development)
  email    SMTP -- free and needs no approval from anyone. A Gmail account
           works as the sender: turn on 2-Step Verification, create an App
           Password (16 characters), put the address in SMTP_USER and the
           App Password in SMTP_PASSWORD. Gmail allows ~500 mails/day.
"""
from src.core.modules import smtplib, EmailMessage
from src.core import config
from src.core.logger import get_logger

logger = get_logger(__name__)

_SUBJECT = {
    "signup": "Your verification code",
    "reset_password": "Your password reset code",
}


class OtpDeliveryError(Exception):
    pass


def send_otp(email: str, otp: str, purpose: str) -> None:
    provider = config.OTP_PROVIDER
    if provider == "console":
        logger.warning(f"[OTP:{purpose}] {email} -> {otp}  (OTP_PROVIDER=console, not sent)")
    elif provider == "email":
        _send_email(email, otp, purpose)
    else:
        raise OtpDeliveryError(f"Unknown OTP_PROVIDER '{provider}' (use console or email).")


def _send_email(to_address: str, otp: str, purpose: str) -> None:
    if not config.SMTP_USER or not config.SMTP_PASSWORD:
        raise OtpDeliveryError("SMTP_USER and SMTP_PASSWORD must be set in .env for OTP_PROVIDER=email")
    msg = EmailMessage()
    msg["Subject"] = f"{_SUBJECT.get(purpose, 'Your code')}: {otp}"
    msg["From"] = f"{config.SMTP_FROM_NAME} <{config.SMTP_FROM}>"
    msg["To"] = to_address
    action = "verify your email" if purpose == "signup" else "reset your password"
    msg.set_content(
        f"Your code to {action} is:\n\n    {otp}\n\n"
        f"It is valid for 5 minutes. If you did not request this, ignore this email.\n"
    )
    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=15) as smtp:
            smtp.starttls()                      # encrypt before sending the password
            smtp.login(config.SMTP_USER, config.SMTP_PASSWORD)
            smtp.send_message(msg)
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP login rejected -- for Gmail use an App Password, not the account password.")
        raise OtpDeliveryError("Email sender login failed.")
    except (smtplib.SMTPException, OSError) as e:
        logger.error(f"SMTP send failed: {e}")
        raise OtpDeliveryError("Could not send the email.")