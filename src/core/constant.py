REENTRY_MODES = ("RE_ASAP", "RE_ASAP_REVERSE", "RE_COST", "RE_COST_REVERSE", "RE_MOMENTUM", "RE_MOMENTUM_REVERSE", "LAZY_LEG")

OVERALL_REENTRY_MODES = ("RE_ASAP", "RE_ASAP_REVERSE", "RE_MOMENTUM", "RE_MOMENTUM_REVERSE")

# Sensex option lot size (BSE, since Nov 2024). PnL = price move * QUANTITY * lot_size.
QUANTITY = 20

COST_BUFFER_PCT = 0.05

MARKET_OPEN_SECS = 9 * 3600 + 15 * 60        # 09:15:00
MARKET_CLOSE_SECS = 15 * 3600 + 30 * 60      # 15:30:00
OVERNIGHT_PAUSE_SECS = 15 * 3600 + 45 * 60   # BTST / positional holds pause at 15:45
AUTO_RESTART_SCAN_SECS = 8 * 3600 + 45 * 60  # the worker scan auto-restarts overnight holds from 08:45
ENTRY_TIME_WINDOW = ("09:15:15", "15:29:00")   # AlgoTest's activation dialog limits
EXIT_TIME_WINDOW = ("09:15:05", "15:29:30")

SUPPORTED_STRATEGY_TYPES = ("intraday", "sequential", "btst", "positional")
MULTIDAY_TYPES = ("btst", "positional")
MOMENTUM_TYPES = ("POINTS_UP", "POINTS_DOWN", "PERCENT_UP", "PERCENT_DOWN",
                  "UNDERLYING_POINTS_UP", "UNDERLYING_POINTS_DOWN", "UNDERLYING_PERCENT_UP", "UNDERLYING_PERCENT_DOWN")
PREMIUM_TYPES = ("POINTS", "PERCENT")
UNDERLYING_TYPES = ("UNDERLYING_POINTS", "UNDERLYING_PERCENT")
SENSEX_UNDERLYING = "SENSEX"
STRIKE_STEP = 100                            # BSE lists Sensex strikes every 100 points

ACTIVE_STATUSES = ("scheduled", "running", "paused")
TERMINAL_STATUSES = ("squared_off", "completed", "error", "cancelled", "manual")
OPEN_STATUSES = ("open", "exiting")
PENDING_STATUSES = ("pending", "entering", "waiting")
# status_reason suffix of a RECOVERABLE error (AlgoTest's Error state): the worker keeps the
# runner, the open legs stay monitored and Resume retries the failed leg(s)
RESUME_HINT = "-- click Resume to retry"
OVERNIGHT_REASON = "market closed -- restart before 09:15"
RESTART_REASON = "restart scheduled at "
MARGIN_ERROR_MARKERS = ("margin", "insufficient", "fund")     # broker rejection texts that mean a margin shortfall

REENTRY_CUTOFF_SECS = 60        # no fresh entry (re-entry / conditional fill) inside this window before exit_time
STALE_LTP_SECS = 30             # log (never an event) when a leg's LTP is older than this
STALE_WARN_EVERY_SECS = 30      # at most one such log line per leg per this interval
RECONCILE_EVERY_SECS = 60       # live mode: compare open legs with the broker's position book this often
RECONCILE_GRACE_SECS = 20       # a leg filled less than this long ago is not judged against the position book
PREDECESSOR_POLL_SECS = 5       # how often a successor deployment checks whether the older one has exited
CHASE_STEP_SECS = 2             # how often an unfilled limit is re-priced while chasing
WAIT_SAVE_EVERY_SECS = 5        # range high/low progress is written at most this often per leg
SL_REFRESH_EVERY_SECS = 30      # safety-net poll of a resting SL-L order when the order socket is quiet

TICK = 0.05
VALID_DAYS = ("M", "T", "W", "Th", "F", "Sa", "Su")
MPP_SLABS = ((10, 5.0), (100, 3.0), (500, 2.0), (float("inf"), 1.0))   # option premium upper bound -> protection %

# The binary market data API is the only one we use (both XTS servers): REST replies are
# JSON, the socket streams `xts-binary-packet` frames whatever publishFormat is requested.
MARKETDATA_PATH = "/apibinarymarketdata"
SEG_NAME = {11: "BSECM", 12: "BSEFO", 1: "NSECM", 2: "NSEFO", 3: "NSECD", 13: "BSECD"}
ORDER_OPEN_STATES = ("New", "Open", "PendingNew", "Replaced", "PartiallyFilled", "PendingReplace", "PendingCancel")
ORDER_TERMINAL_STATES = ("Filled", "Cancelled", "Rejected")
# Order-call throttle (calls/second). The XTS doc's rate-limit table says 10/s for
# place+modify+cancel combined, but this is OFF (0) until a real 429 is observed from the broker.
ORDER_THROTTLE_RATE = 0
MAX_IN_FLIGHT = 8               # order calls in flight at once (a 50-leg strategy streams, it does not burst)
CONNECT_RETRIES = 3             # attempts when the TCP connect fails (nothing was sent yet)
RETRY_DELAY = 0.2
WARM_CONNECTIONS = 4            # connections opened at login and kept alive for order bursts
KEEPALIVE_EVERY = 20.0          # seconds between keep-alive pings (servers drop idle connections after ~60 s)
POLL_AFTER_SECONDS = 3.0        # order socket silent this long -> poll the order book
SESSION_DEAD_MARKERS = ("session has been expired", "session expired", "invalid token", "token/authorization",
                        "wrong instance", "not logged in")
FEED_RELOGIN_EVERY_SECS = 60    # a kicked market-data session logs in again at most this often
HOST_LOOKUP_DEFAULT_PASSWORD = "2021HostLookUpAccess"   # Symphony's documented default
SIO_RECONNECT_MIN, SIO_RECONNECT_MAX = 1.0, 15.0
SIO_KICK_MIN, SIO_KICK_MAX = 30.0, 300.0        # the server keeps closing the namespace (dead token)
SIO_OPEN_TIMEOUT = 15.0

OTP_TTL_MINUTES = 5             # a code is valid for this long
OTP_RESEND_SECONDS = 30         # minimum gap between two codes to one address
OTP_MAX_ATTEMPTS = 5            # wrong guesses before the code is burnt
PASSWORD_MIN_LENGTH = 8
JWT_ALGORITHM = "HS256"