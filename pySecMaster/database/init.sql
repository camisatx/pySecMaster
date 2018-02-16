-- Change default postgres password
ALTER ROLE postgres WITH PASSWORD 'correct horse battery staple';

-- Create database called pysecmaster
CREATE DATABASE pysecmaster;

-- Block all users by default
REVOKE ALL ON DATABASE pysecmaster FROM public;
REVOKE ALL ON SCHEMA public FROM public;

-- Create pymaster user with all privileges for pysecmaster database
CREATE USER pymaster WITH PASSWORD 'correct horse battery staple';
GRANT ALL PRIVILEGES ON DATABASE pysecmaster TO pymaster;

-- Add remote_users role with read only access
CREATE ROLE remote_users WITH PASSWORD 'wrong horse battery staple';
-- Grant read only access to existing objects
GRANT CONNECT ON DATABASE pysecmaster TO remote_users;
GRANT USAGE ON SCHEMA public TO remote_users;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO remote_users;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO remote_users;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO remote_users;
-- Grant read only access to new objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO remote_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO remote_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO remote_users;

-- Add remote user for josh and ricardo under the remote_users role
CREATE USER remote_josh IN ROLE remote_users;
CREATE USER remote_ricardo IN ROLE remote_users;

-- Change to pysecmaster database using the user pymaster
\c pysecmaster pymaster

CREATE TABLE IF NOT EXISTS symbology (
    symbol_id       BIGINT                      NOT NULL,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    type            TEXT,
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_symbology_sources
    ON symbology(source, source_id);

CREATE TABLE IF NOT EXISTS baskets (
    basket_id       SERIAL                      PRIMARY KEY,
    name            TEXT                        NOT NULL,
    description     TEXT,
    start_date      TIMESTAMP WITH TIME ZONE,
    end_date        TIMESTAMP WITH TIME ZONE,
    created_by      TEXT,
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE);

CREATE TABLE IF NOT EXISTS basket_values (
    basket_val_id   BIGSERIAL                   PRIMARY KEY,
    basket_id       INTEGER                     NOT NULL,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(basket_id) REFERENCES baskets(basket_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);

CREATE TABLE IF NOT EXISTS classification (
    classification_id   BIGSERIAL                   PRIMARY KEY,
    source              TEXT                        NOT NULL,
    source_id           TEXT                        NOT NULL,
    standard            TEXT,
    code                INTEGER,
    level_1             TEXT,
    level_2             TEXT,
    level_3             TEXT,
    level_4             TEXT,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_classification_values
    ON classification(source, source_id, standard, level_1, level_2, level_3,
    level_4);

CREATE TABLE IF NOT EXISTS csidata_stock_factsheet (
    csi_number          TEXT                        PRIMARY KEY,
    symbol              TEXT,
    name                TEXT,
    exchange            TEXT,
    sub_exchange        TEXT,
    is_active           SMALLINT,
    start_date          DATE,
    end_date            DATE,
    conversion_factor   SMALLINT,
    switch_cf_date      DATE,
    pre_switch_cf       SMALLINT,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE);
CREATE INDEX IF NOT EXISTS idx_csidata_symbol
    ON csidata_stock_factsheet(symbol);

CREATE TABLE IF NOT EXISTS data_vendor (
    data_vendor_id      INTEGER                     PRIMARY KEY,
    name                TEXT                        UNIQUE,
    url                 TEXT,
    support_email       TEXT,
    api                 TEXT,
    consensus_weight    SMALLINT,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE);

CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id        SMALLINT                    PRIMARY KEY,
    symbol             TEXT                        UNIQUE NOT NULL,
    goog_symbol        TEXT,
    yahoo_symbol       TEXT,
    csi_symbol         TEXT,
    tsid_symbol        TEXT                        NOT NULL,
    name               TEXT,
    country            TEXT,
    city               TEXT,
    currency           TEXT,
    time_zone          TEXT,
    utc_offset         REAL,
    open               TIME,
    close              TIME,
    lunch              TEXT,
    created_date       TIMESTAMP WITH TIME ZONE,
    updated_date       TIMESTAMP WITH TIME ZONE);

CREATE TABLE IF NOT EXISTS indices (
    index_id            SERIAL                      PRIMARY KEY,
    stock_index         TEXT                        NOT NULL,
    source              TEXT                        NOT NULL,
    source_id           TEXT                        NOT NULL,
    as_of_date          TIMESTAMP WITH TIME ZONE,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);

CREATE TABLE IF NOT EXISTS quandl_codes (
    q_code_id           BIGSERIAL                   PRIMARY KEY,
    data_vendor         TEXT                        NOT NULL,
    data                TEXT                        NOT NULL,
    component           TEXT                        NOT NULL,
    period              TEXT,
    symbology_source    TEXT                        NOT NULL,
    q_code              TEXT                        NOT NULL,
    name                TEXT,
    start_date          TIMESTAMP WITH TIME ZONE,
    end_date            TIMESTAMP WITH TIME ZONE,
    frequency           TEXT,
    last_updated        TIMESTAMP WITH TIME ZONE,
    page_num            INTEGER,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(data_vendor)
        REFERENCES data_vendor(name)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_qc_data
    ON quandl_codes(data);

CREATE TABLE IF NOT EXISTS tickers (
    tsid                TEXT                        PRIMARY KEY,
    ticker              TEXT                        NOT NULL,
    name                TEXT,
    exchange_id         INT                         NOT NULL,
    is_active           SMALLINT,
    start_date          TIMESTAMP WITH TIME ZONE,
    end_date            TIMESTAMP WITH TIME ZONE,
    type                TEXT,
    sector              TEXT,
    industry            TEXT,
    sub_industry        TEXT,
    currency            TEXT,
    hq_country          TEXT,
    symbology_source    TEXT                        NOT NULL,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(symbology_source, tsid)
        REFERENCES symbology(source, source_id) ON UPDATE CASCADE,
    FOREIGN KEY(exchange_id)
        REFERENCES exchanges(exchange_id) ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_tickers_sector
    ON tickers(sector);

CREATE TABLE IF NOT EXISTS daily_prices (
    daily_price_id  BIGSERIAL                   PRIMARY KEY,
    data_vendor_id  SMALLINT,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    date            TIMESTAMP WITH TIME ZONE    NOT NULL,
    open            DECIMAL(11,4),
    high            DECIMAL(11,4),
    low             DECIMAL(11,4),
    close           DECIMAL(11,4),
    volume          BIGINT,
    ex_dividend     DECIMAL(6,3),
    split_ratio     DECIMAL(11,4),
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_dp_identifiers
    ON daily_prices(source, source_id, data_vendor_id, date DESC NULLS LAST,
    updated_date);

CREATE TABLE IF NOT EXISTS finra_data (
    finra_id                SERIAL                      PRIMARY KEY,
    source                  TEXT                        NOT NULL,
    source_id               TEXT                        NOT NULL,
    date                    TIMESTAMP WITH TIME ZONE    NOT NULL,
    short_volume            INTEGER,
    short_exempt_volume     INTEGER,
    total_volume            INTEGER,
    updated_date            TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_finra_source_id
    ON finra_data(source, source_id);

CREATE TABLE IF NOT EXISTS fundamental_data (
    fundamental_id  BIGSERIAL                   PRIMARY KEY,
    data_vendor_id  SMALLINT,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    date            TIMESTAMP WITH TIME ZONE    NOT NULL,
    field           TEXT,
    value           DECIMAL(14,2),
    note            TEXT,
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_fund_source_id
    ON fundamental_data(source, source_id, data_vendor_id, date DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS minute_prices (
    minute_price_id     BIGSERIAL                   PRIMARY KEY,
    data_vendor_id      SMALLINT,
    source              TEXT                        NOT NULL,
    source_id           TEXT                        NOT NULL,
    date                TIMESTAMP WITH TIME ZONE    NOT NULL,
    close               DECIMAL(11,4),
    high                DECIMAL(11,4),
    low                 DECIMAL(11,4),
    open                DECIMAL(11,4),
    volume              BIGINT,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_mp_identifiers
    ON minute_prices(source, source_id, data_vendor_id, date DESC NULLS LAST,
    updated_date);

CREATE TABLE IF NOT EXISTS option_chains (
    option_id       BIGSERIAL                   PRIMARY KEY,
    data_vendor_id  SMALLINT,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    symbol          TEXT,
    exchange        TEXT,
    currency        TEXT,
    multiplier      SMALLINT,
    contract_id     BIGINT                      NOT NULL,
    expiry          DATE,
    type            TEXT,
    strike          DECIMAL(8,2),
    pre_split       BOOLEAN,
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_option_chains_values
    ON option_chains(data_vendor_id, source, source_id, contract_id, expiry,
    strike, pre_split);

CREATE TABLE IF NOT EXISTS option_prices (
    option_prices_id    BIGSERIAL                   PRIMARY KEY,
    data_vendor_id      SMALLINT,
    option_id           BIGINT                      NOT NULL,
    date                TIMESTAMP WITH TIME ZONE    NOT NULL,
    bid                 DECIMAL(10,4),
    bid_size            INTEGER,
    ask                 DECIMAL(10,4),
    ask_size            INTEGER,
    close               DECIMAL(10,4),
    open_interest       INTEGER,
    volume              INTEGER,
    imp_volatility      DECIMAL(6,4),
    delta               DECIMAL(6,5),
    gamma               DECIMAL(6,5),
    rho                 DECIMAL(6,5),
    theta               DECIMAL(6,5),
    vega                DECIMAL(6,5),
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(option_id)
        REFERENCES option_chains(option_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_option_prices
    ON option_prices(option_id, data_vendor_id, date DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS tick_prices (
    tick_id         BIGSERIAL                   PRIMARY KEY,
    data_vendor_id  SMALLINT,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    date            TIMESTAMP WITH TIME ZONE,
    bid             DECIMAL(11,4),
    ask             DECIMAL(11,4),
    last            DECIMAL(11,4),
    high            DECIMAL(11,4),
    low             DECIMAL(11,4),
    close           DECIMAL(11,4),
    bid_size        INTEGER,
    ask_size        INTEGER,
    last_size       INTEGER,
    volume          INTEGER,
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_tick_values
    ON tick_prices(source, source_id, date DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS tick_prices_stream (
    tick_id         BIGSERIAL                   PRIMARY KEY,
    data_vendor_id  SMALLINT,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    date            TIMESTAMP WITH TIME ZONE,
    field           TEXT,
    value           DECIMAL(11,4),
    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_tick_stream_values
    ON tick_prices_stream(source, source_id, date DESC NULLS LAST, field);

CREATE TABLE IF NOT EXISTS conference_calls (
    conf_call_id        SERIAL                      PRIMARY KEY,
    source              TEXT                        NOT NULL,
    source_id           TEXT                        NOT NULL,
    symbol              TEXT,
    date                TIMESTAMP WITH TIME ZONE    NOT NULL,
    event_title         TEXT,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_conf_source_id
    ON conference_calls(source, source_id, date);

CREATE TABLE IF NOT EXISTS dividends (
    dividend_id         SERIAL                      PRIMARY KEY,
    source              TEXT                        NOT NULL,
    source_id           TEXT                        NOT NULL,
    symbol              TEXT,
    company             TEXT,
    dividend            DECIMAL(6,3),
    ex_dividend_date    TIMESTAMP WITH TIME ZONE    NOT NULL,
    record_date         TIMESTAMP WITH TIME ZONE,
    announcement_date   TIMESTAMP WITH TIME ZONE,
    payment_date        TIMESTAMP WITH TIME ZONE,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_div_source_id
    ON dividends(source, source_id, ex_dividend_date);

CREATE TABLE IF NOT EXISTS earnings (
    earnings_id     SERIAL                      PRIMARY KEY,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    symbol          TEXT,
    company_name    TEXT,
    date            TIMESTAMP WITH TIME ZONE    NOT NULL,
    reported_eps    DECIMAL(6,3),
    consensus_eps   DECIMAL(6,3),
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_earn_source_id
    ON earnings(source, source_id, date);

CREATE TABLE IF NOT EXISTS economic_events (
    event_id            SERIAL                      PRIMARY KEY,
    source              TEXT                        NOT NULL,
    source_id           TEXT                        NOT NULL,
    event_name          TEXT,
    date                TIMESTAMP WITH TIME ZONE,
    date_for            TIMESTAMP WITH TIME ZONE,
    actual              TEXT,
    briefing_forecast   TEXT,
    market_expects      TEXT,
    prior               TEXT,
    revised_from        TEXT,
    created_date        TIMESTAMP WITH TIME ZONE,
    updated_date        TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_econ_event_source_id
    ON economic_events(source, source_id, date, event_name);

CREATE TABLE IF NOT EXISTS ipo_pricings (
    ipo_id          SERIAL                      PRIMARY KEY,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    symbol          TEXT,
    company_name    TEXT,
    offer_date      TIMESTAMP WITH TIME ZONE,
    shares_offered  TEXT,
    proposed_price  TEXT,
    initial_price   TEXT,
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_ipop_source_id
    ON ipo_pricings(source, source_id, offer_date);

CREATE TABLE IF NOT EXISTS splits (
    split_id        SERIAL                      PRIMARY KEY,
    source          TEXT                        NOT NULL,
    source_id       TEXT                        NOT NULL,
    symbol          TEXT,
    company_name    TEXT,
    payable_date    TIMESTAMP WITH TIME ZONE,
    ex_date         TIMESTAMP WITH TIME ZONE,
    announced_date  TIMESTAMP WITH TIME ZONE,
    optionable      BOOLEAN,
    ratio           DECIMAL(11,4),
    created_date    TIMESTAMP WITH TIME ZONE,
    updated_date    TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE);
CREATE INDEX IF NOT EXISTS idx_splits_source_id
    ON splits(source, source_id, ex_date, ratio);
