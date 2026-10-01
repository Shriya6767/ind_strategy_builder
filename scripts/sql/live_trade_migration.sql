-- ============================================================================
-- Live trading (Symphony Open XTS).  Idempotent: safe to run twice.
-- Run on the host:  psql -h <host> -U <user> -d <db> -f scripts/sql/live_trade_migration.sql
-- ============================================================================

-- One row per broker connection a user has set up ("Broker Setup" page).
-- API keys/secrets are Fernet-encrypted with BROKER_SECRET_KEY; the API
-- never returns them, only a masked suffix.
CREATE TABLE IF NOT EXISTS broker_account (
    broker_account_id     BIGSERIAL PRIMARY KEY,
    user_id               BIGINT NOT NULL REFERENCES app_user(user_id),
    broker                VARCHAR(30)  NOT NULL DEFAULT 'open_xts',
    connection_name       VARCHAR(100) NOT NULL,
    interactive_key_enc   TEXT NOT NULL,
    interactive_secret_enc TEXT NOT NULL,
    marketdata_key_enc    TEXT,                       -- optional when LIVE_FEED_* is configured
    marketdata_secret_enc TEXT,
    connection_url        VARCHAR(255) NOT NULL,      -- https://xts.broker.com  (origin, no path)
    host_lookup_url       VARCHAR(255),               -- https://xts.broker.com/hostlookup (optional)
    host_lookup_password_enc TEXT,
    interactive_path      VARCHAR(60)  NOT NULL DEFAULT '/interactive',          -- overwritten by HostLookUp
    marketdata_path       VARCHAR(60)  NOT NULL DEFAULT '/apimarketdata',
    dealer_client_id      VARCHAR(30),                -- DMA/dealer accounts only
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_broker_account_user ON broker_account (user_id);

-- Today's XTS session tokens (24 h validity). One row per broker account,
-- replaced on every login. Tokens are encrypted like the keys.
CREATE TABLE IF NOT EXISTS broker_session (
    broker_account_id     BIGINT PRIMARY KEY REFERENCES broker_account(broker_account_id) ON DELETE CASCADE,
    interactive_token_enc TEXT,
    interactive_user_id   VARCHAR(50),
    client_id             VARCHAR(50),                -- clientID sent on every order
    is_investor_client    BOOLEAN,
    marketdata_token_enc  TEXT,
    marketdata_user_id    VARCHAR(50),
    logged_in_at          TIMESTAMP,
    expires_at            TIMESTAMP,
    last_error            TEXT
);

-- Per-strategy execution settings (the "Select execution" dialog). A
-- strategy is "Ready to deploy" once a row exists.
CREATE TABLE IF NOT EXISTS live_execution_setting (
    user_id               BIGINT NOT NULL REFERENCES app_user(user_id),
    strategy_id           INTEGER NOT NULL,
    version               INTEGER NOT NULL DEFAULT 0,  -- 0 = latest version at activation
    broker_account_id     BIGINT REFERENCES broker_account(broker_account_id) ON DELETE SET NULL,
    settings              JSONB NOT NULL,              -- see src/live/execution.py ExecutionSettings
    auto_activate         BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, strategy_id)
);

-- One activation of one strategy for one trading day.
-- status: scheduled | running | paused | squared_off | completed | error | cancelled
CREATE TABLE IF NOT EXISTS live_deployment (
    deployment_id         BIGSERIAL PRIMARY KEY,
    user_id               BIGINT NOT NULL REFERENCES app_user(user_id),
    strategy_id           INTEGER NOT NULL,
    strategy_name         VARCHAR(255),
    version               INTEGER NOT NULL,
    broker_account_id     BIGINT REFERENCES broker_account(broker_account_id) ON DELETE SET NULL,
    mode                  VARCHAR(10) NOT NULL DEFAULT 'paper',   -- paper | live
    trade_date            DATE NOT NULL,
    exit_date             DATE NOT NULL,               -- = trade_date (intraday) or next trading day (btst)
    status                VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    status_reason         TEXT,
    settings              JSONB NOT NULL,              -- frozen ExecutionSettings for this run
    strategy_snapshot     JSONB NOT NULL,              -- frozen {strategy, legs} used by the runner
    realised_pnl          NUMERIC(14,2) NOT NULL DEFAULT 0,
    is_archived           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_live_deployment_user_date ON live_deployment (user_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_live_deployment_active ON live_deployment (status) WHERE status IN ('scheduled', 'running', 'paused');

-- One row per leg ENTRY (a re-entry is a new row with attempt+1).
-- status: pending | entering | open | exiting | closed | error | skipped
CREATE TABLE IF NOT EXISTS live_leg (
    live_leg_id           BIGSERIAL PRIMARY KEY,
    deployment_id         BIGINT NOT NULL REFERENCES live_deployment(deployment_id) ON DELETE CASCADE,
    leg_number            INTEGER NOT NULL,
    attempt               INTEGER NOT NULL DEFAULT 1,
    exchange_segment      VARCHAR(10) NOT NULL DEFAULT 'BSEFO',
    instrument_id         BIGINT,
    symbol                VARCHAR(60),
    expiry                DATE,
    strike                INTEGER,
    option_type           VARCHAR(2),                  -- CE | PE
    side                  VARCHAR(4) NOT NULL,         -- BUY | SELL
    quantity              INTEGER NOT NULL,            -- units sent to the broker
    lots                  INTEGER NOT NULL,
    status                VARCHAR(12) NOT NULL DEFAULT 'pending',
    entry_order_id        VARCHAR(40),
    entry_price           NUMERIC(12,2),
    entry_time            TIMESTAMP,
    underlying_at_entry   NUMERIC(12,2),
    stoploss_price        NUMERIC(12,2),
    target_price          NUMERIC(12,2),
    exit_order_id         VARCHAR(40),
    exit_price            NUMERIC(12,2),
    exit_time             TIMESTAMP,
    exit_reason           VARCHAR(30),
    pnl                   NUMERIC(14,2),
    error                 TEXT,
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_live_leg_deployment ON live_leg (deployment_id);

-- Every broker request/response and every socket update, as sent/received.
CREATE TABLE IF NOT EXISTS live_order (
    live_order_id         BIGSERIAL PRIMARY KEY,
    deployment_id         BIGINT REFERENCES live_deployment(deployment_id) ON DELETE CASCADE,
    live_leg_id           BIGINT,
    app_order_id          VARCHAR(40),
    unique_tag            VARCHAR(20),                 -- our orderUniqueIdentifier
    action                VARCHAR(12) NOT NULL,        -- place | modify | cancel | update
    side                  VARCHAR(4),
    order_type            VARCHAR(12),
    quantity              INTEGER,
    price                 NUMERIC(12,2),
    status                VARCHAR(20),                 -- New | Open | Filled | Cancelled | Rejected | ...
    filled_qty            INTEGER,
    avg_price             NUMERIC(12,2),
    reason                TEXT,
    payload               JSONB,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_live_order_deployment ON live_order (deployment_id, created_at);

-- Human-readable audit trail shown in the UI ("09:35:00 Leg 1 entry filled @ 309.5").
CREATE TABLE IF NOT EXISTS live_event (
    live_event_id         BIGSERIAL PRIMARY KEY,
    deployment_id         BIGINT REFERENCES live_deployment(deployment_id) ON DELETE CASCADE,
    level                 VARCHAR(8) NOT NULL DEFAULT 'info',   -- info | warn | error
    message               TEXT NOT NULL,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_live_event_deployment ON live_event (deployment_id, created_at);

-- How the leg came to be entered: ENTRY | MOMENTUM | RANGE_BREAKOUT | SEQUENTIAL |
-- RE_ASAP | RE_ASAP_REVERSE | RE_COST | RE_COST_REVERSE | RE_MOMENTUM | RE_MOMENTUM_REVERSE |
-- LAZY_LEG | OVERALL_<mode>
ALTER TABLE live_leg ADD COLUMN IF NOT EXISTS entry_mode VARCHAR(30);

-- Where today's interactive session actually lives. HostLookUp can return a
-- different origin/path than the account's connection_url (e.g. another port
-- with no path prefix); orders and the order socket must go THERE.
ALTER TABLE broker_session ADD COLUMN IF NOT EXISTS interactive_origin VARCHAR(255);
ALTER TABLE broker_session ADD COLUMN IF NOT EXISTS interactive_path   VARCHAR(60);

-- Pending conditional entries (momentum / range breakout / RE_COST / observation
-- legs) so a worker restart re-arms them exactly as they were: same contract,
-- same trigger, same range high/low, same deadline.
CREATE TABLE IF NOT EXISTS live_wait (
    deployment_id         BIGINT NOT NULL REFERENCES live_deployment(deployment_id) ON DELETE CASCADE,
    leg_number            INTEGER NOT NULL,
    attempt               INTEGER NOT NULL,
    kind                  VARCHAR(12) NOT NULL,        -- momentum | range | cost
    leg_kind              VARCHAR(12) NOT NULL,        -- trade | observation
    entry_mode            VARCHAR(30),
    side                  VARCHAR(4),
    instrument_id         BIGINT,
    watch_segment         INTEGER,
    watch_instrument_id   BIGINT,
    quantity              INTEGER,
    lots                  INTEGER,
    trigger_price         NUMERIC(12,2),
    trigger_up            BOOLEAN,
    range_hi              NUMERIC(12,2),
    range_lo              NUMERIC(12,2),
    range_end_at          TIMESTAMP,
    range_high_side       BOOLEAN,
    deadline              TIMESTAMP,
    trade_id              INTEGER NOT NULL DEFAULT 1,
    reentry_sl_left       INTEGER NOT NULL DEFAULT 0,
    reentry_tgt_left      INTEGER NOT NULL DEFAULT 0,
    meta                  JSONB NOT NULL,              -- the leg definition (and seq_meta for observation legs)
    status                VARCHAR(12) NOT NULL DEFAULT 'waiting',   -- waiting | done | skipped | cancelled
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deployment_id, leg_number, attempt)
);

-- Execution-settings parity: the stop-loss resting at the broker as an SL-L
-- order, the intended ("trigger") entry price used when tgt_sl_ref_price =
-- TRIGGER, and the order types the broker enables for BSEFO (from the login reply).
ALTER TABLE live_leg       ADD COLUMN IF NOT EXISTS sl_order_id VARCHAR(40);
ALTER TABLE live_leg       ADD COLUMN IF NOT EXISTS ref_price   NUMERIC(12,2);
ALTER TABLE broker_session ADD COLUMN IF NOT EXISTS order_types VARCHAR(200);
