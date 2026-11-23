REENTRY_MODES = ("RE_ASAP", "RE_ASAP_REVERSE", "RE_COST", "RE_COST_REVERSE", "RE_MOMENTUM", "RE_MOMENTUM_REVERSE", "LAZY_LEG")

OVERALL_REENTRY_MODES = ("RE_ASAP", "RE_ASAP_REVERSE", "RE_MOMENTUM", "RE_MOMENTUM_REVERSE")

# Sensex option lot size (BSE, since Nov 2024). PnL = price move * QUANTITY * lot_size.
QUANTITY = 20

COST_BUFFER_PCT = 0.05

# A premium-based strike criterion takes a contract only when its price is within this
# fraction of the requested premium (or of the range bound it lies beyond).
PREMIUM_MATCH_TOLERANCE = 0.30

MARKET_OPEN_SECS = 9 * 3600 + 15 * 60        # 09:15:00
MARKET_CLOSE_SECS = 15 * 3600 + 40 * 60      # 15:40:00 -- BSE F&O close since MARKET_CLOSE_EXTENDED_FROM
MARKET_CLOSE_SECS_BEFORE = 15 * 3600 + 30 * 60   # 15:30:00 -- close of every session before MARKET_CLOSE_EXTENDED_FROM
MARKET_CLOSE_EXTENDED_FROM = "2026-08-03"    # first session traded to 15:40 (backtest day-1 close switches here)
DEFAULT_ENTRY_TIME = "09:16:00"
DEFAULT_EXIT_TIME = "15:30:00"

SUPPORTED_STRATEGY_TYPES = ("intraday", "sequential", "btst", "positional")
MULTIDAY_TYPES = ("btst", "positional")
VALID_DAYS = ("M", "T", "W", "Th", "F", "Sa", "Su")

OTP_TTL_MINUTES = 5             # a code is valid for this long
OTP_RESEND_SECONDS = 30         # minimum gap between two codes to one address
OTP_MAX_ATTEMPTS = 5            # wrong guesses before the code is burnt
PASSWORD_MIN_LENGTH = 8
JWT_ALGORITHM = "HS256"
