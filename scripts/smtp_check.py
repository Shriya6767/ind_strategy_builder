"""One-off check that the SMTP settings in .env can actually send mail.

    venv\\Scripts\\python scripts\\smtp_check.py you@example.com

Sends a test code to the given address using the same code path the
signup flow uses (OTP_PROVIDER=email settings: SMTP_HOST/PORT/USER/
PASSWORD/FROM/FROM_NAME). Prints the exact error if it fails."""
import sys
sys.path.insert(0, ".")
from src.core import config
from src.core.otp_delivery import _send_email, OtpDeliveryError

if len(sys.argv) != 2:
    sys.exit("usage: python scripts/smtp_check.py <recipient-email>")
print(f"SMTP {config.SMTP_HOST}:{config.SMTP_PORT} as {config.SMTP_USER} -> {sys.argv[1]}")
try:
    _send_email(sys.argv[1], "123456", "signup")
    print("OK: test code 123456 sent -- check the inbox (and spam folder).")
except OtpDeliveryError as e:
    sys.exit(f"FAILED: {e}")
