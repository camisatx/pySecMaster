import sqlite3

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2015 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.2'

'''
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

# Future table additions:
#   corporate_actions
#   fundamentals    (company, energy, economic)
#   futures_prices


def main_tables(db_location):

    conn = sqlite3.connect(db_location)
    cur = conn.cursor()

    def symbology(c):
        c.execute('''CREATE TABLE IF NOT EXISTS symbology
        (symbol_id      INTEGER PRIMARY KEY,
        source          TEXT,
        source_id       TEXT,
        type            TEXT,
        created_date    FLOAT,
        updated_date    FLOAT)''')
        c.execute("""CREATE INDEX IF NOT EXISTS idx_symbology_source_id
                    ON symbology(source_id)""")

    def data_vendor(c):
        c.execute('''CREATE TABLE IF NOT EXISTS data_vendor
        (data_vendor_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        name                TEXT UNIQUE,
        url                 TEXT,
        support_email       TEXT,
        api                 TEXT,
        created_date        FLOAT,
        updated_date        FLOAT)''')

    def quandl_codes(c):
        c.execute('''CREATE TABLE IF NOT EXISTS quandl_codes
        (q_code_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        data_vendor         TEXT,
        data                TEXT,
        component           TEXT,
        period              TEXT,
        q_code              TEXT,
        name                TEXT,
        start_date          FLOAT,
        end_date            FLOAT,
        frequency           TEXT,
        last_updated        FLOAT,
        page_num            INTEGER,
        created_date        FLOAT,
        updated_date        FLOAT,
        FOREIGN KEY(data_vendor) REFERENCES data_vendor(name))''')
        c.execute("""CREATE INDEX IF NOT EXISTS idx_qc_data
                    ON quandl_codes(data)""")

    def fundamental_data(c):
        c.execute('''CREATE TABLE IF NOT EXISTS fundamental_data
        (fundamental_id INTEGER PRIMARY KEY AUTOINCREMENT,
        q_code          TEXT,
        date            TEXT,
        value           REAL,
        note            TEXT,
        created_date    FLOAT,
        updated_date    FLOAT,
        FOREIGN KEY(q_code) REFERENCES quandl_codes(q_code))''')
        c.execute("""CREATE INDEX IF NOT EXISTS idx_fund_q_code
                    ON fundamental_data(q_code)""")

    symbology(cur)
    data_vendor(cur)
    quandl_codes(cur)
    fundamental_data(cur)

    print('All tables in MainTables are created')


def stock_tables(db_location):

    conn = sqlite3.connect(db_location)
    cur = conn.cursor()

    def csidata_stock_factsheet(c):
        c.execute('''CREATE TABLE IF NOT EXISTS csidata_stock_factsheet
        (CsiNumber          INTEGER PRIMARY KEY,
        Symbol              TEXT,
        Name                TEXT,
        Exchange            TEXT,
        IsActive            INTEGER,
        StartDate           TEXT,
        EndDate             TEXT,
        Sector              TEXT,
        Industry            TEXT,
        ConversionFactor    INTEGER,
        SwitchCfDate        TEXT,
        PreSwitchCf         INTEGER,
        LastVolume          INTEGER,
        Type                TEXT,
        ChildExchange       TEXT,
        Currency            TEXT,
        created_date        FLOAT,
        updated_date        FLOAT)''')

    def exchange(c):
        c.execute('''CREATE TABLE IF NOT EXISTS exchange
         (exchange_id       INTEGER PRIMARY KEY AUTOINCREMENT,
         abbrev             TEXT UNIQUE,
         abbrev_goog        TEXT,
         abbrev_yahoo       TEXT,
         name               TEXT,
         country            TEXT,
         city               TEXT,
         currency           TEXT,
         utc_offset         INT,
         open               TEXT,
         close              TEXT,
         lunch              TEXT,
         created_date       FLOAT,
         updated_date       FLOAT)''')

    def indices(c):
        c.execute('''CREATE TABLE IF NOT EXISTS indices
        (index_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_index         TEXT,
        symbol_id           INTEGER,
        as_of               TEXT,
        created_date        FLOAT,
        updated_date        FLOAT,
        FOREIGN KEY(symbol_id) REFERENCES quandl_codes(symbol_id))''')

    def tickers(c):
        c.execute('''CREATE TABLE IF NOT EXISTS tickers
        (symbol_id      INTEGER PRIMARY KEY,
        ticker          TEXT,
        name            TEXT,
        exchange        TEXT,
        child_exchange  TEXT,
        is_active       INTEGER,
        start_date      FLOAT,
        end_date        FLOAT,
        type            TEXT,
        sector          TEXT,
        industry        TEXT,
        sub_industry    TEXT,
        currency        TEXT,
        hq_country      TEXT,
        created_date    FLOAT,
        updated_date    FLOAT,
        FOREIGN KEY(symbol_id) REFERENCES quandl_codes(symbol_id),
        FOREIGN KEY(exchange) REFERENCES exchange(abbrev))''')
        c.execute("""CREATE INDEX IF NOT EXISTS idx_tickers_sector
                    ON tickers(sector)""")

    def daily_prices(c):
        c.execute('''CREATE TABLE IF NOT EXISTS daily_prices
        (daily_price_id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_vendor_id  INT,
        q_code          TEXT,
        date            TEXT,
        open            REAL,
        high            REAL,
        low             REAL,
        close           REAL,
        volume          REAL,
        ex_dividend     REAL,
        split_ratio     REAL,
        adj_open        REAL,
        adj_high        REAL,
        adj_low         REAL,
        adj_close       REAL,
        adj_volume      REAL,
        updated_date    FLOAT,
        FOREIGN KEY(data_vendor_id) REFERENCES data_vendor(data_vendor_id),
        FOREIGN KEY(q_code) REFERENCES quandl_codes(q_code))''')
        c.execute("""CREATE INDEX IF NOT EXISTS idx_dp_q_code
                    ON daily_prices(q_code)""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_dp_date
                    ON daily_prices(date)""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_dp_updated_date
                    ON daily_prices(updated_date)""")

    def minute_prices(c):
        c.execute('''CREATE TABLE IF NOT EXISTS minute_prices
        (minute_price_id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_vendor_id  INT,
        q_code          TEXT,
        date            TEXT,
        close           REAL,
        high            REAL,
        low             REAL,
        open            REAL,
        volume          REAL,
        updated_date    FLOAT,
        FOREIGN KEY(data_vendor_id) REFERENCES data_vendor(data_vendor_id),
        FOREIGN KEY(q_code) REFERENCES quandl_codes(q_code))''')
        c.execute("""CREATE INDEX IF NOT EXISTS idx_mp_q_code
                    ON minute_prices(q_code)""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_mp_date
                    ON minute_prices(date)""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_mp_updated_date
                    ON minute_prices(updated_date)""")

    def finra_data(c):
        c.execute('''CREATE TABLE IF NOT EXISTS finra_data
        (finra_id               INTEGER PRIMARY KEY AUTOINCREMENT,
        q_code                  TEXT,
        date                    TEXT,
        short_volume            REAL,
        short_exempt_volume     REAL,
        total_volume            REAL,
        updated_date            FLOAT,
        FOREIGN KEY(q_code) REFERENCES quandl_codes(q_code))''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_finra_q_code
                    ON finra_data(q_code)''')

    def options_prices(c):
        c.execute('''CREATE TABLE IF NOT EXISTS options_prices
        (options_prices_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        data_vendor         INT,
        underlying          INT,
        underlying_price    REAL,
        quote_time          TEXT,
        strike              REAL,
        expiration          TEXT,
        type                TEXT,
        option_symbol       TEXT,
        last                REAL,
        bid                 REAL,
        ask                 REAL,
        chg                 REAL,
        pct_chg             TEXT,
        vol                 INT,
        open_int            INT,
        imp_vol             TEXT,
        updated_date        FLOAT,
        FOREIGN KEY(data_vendor) REFERENCES data_vendor(data_vendor_id))''')

    csidata_stock_factsheet(cur)
    exchange(cur)
    indices(cur)
    tickers(cur)
    daily_prices(cur)
    minute_prices(cur)
    finra_data(cur)
    # options_prices(cur)

    print('All tables in StockTables are created')


def events_tables(db_location):

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()

            def conference_calls(c):
                c.execute("""CREATE TABLE IF NOT EXISTS conference_calls
                (conf_call_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol_id           INTEGER,
                symbol              TEXT,
                date                FLOAT,
                event_title         TEXT,
                created_date        FLOAT,
                updated_date        FLOAT,
                FOREIGN KEY(symbol_id) REFERENCES symbology(symbol_id))""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_conf_sid
                            ON conference_calls(symbol_id)""")

            def earnings(c):
                c.execute("""CREATE TABLE IF NOT EXISTS earnings
                (earnings_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol_id       INTEGER,
                symbol          TEXT,
                company_name    TEXT,
                date            FLOAT,
                reported_eps    FLOAT,
                consensus_eps   FLOAT,
                created_date    FLOAT,
                updated_date    FLOAT,
                FOREIGN KEY(symbol_id) REFERENCES symbology(symbol_id))""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_earn_sid
                            ON earnings(symbol_id)""")

            def economic_events(c):
                c.execute("""CREATE TABLE IF NOT EXISTS economic_events
                (event_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol_id           INTEGER,
                event_name          TEXT,
                date                FLOAT,
                date_for            FLOAT,
                actual              TEXT,
                briefing_forecast   TEXT,
                market_expects      TEXT,
                prior               TEXT,
                revised_from        TEXT,
                created_date        FLOAT,
                updated_date        FLOAT,
                FOREIGN KEY(symbol_id) REFERENCES symbology(symbol_id))""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_econ_event_sid
                            ON economic_events(symbol_id)""")

            def ipo_pricings(c):
                c.execute("""CREATE TABLE IF NOT EXISTS ipo_pricings
                (ipo_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol_id       INTEGER,
                symbol          TEXT,
                company_name    TEXT,
                offer_date      FLOAT,
                shares_offered  TEXT,
                proposed_price  TEXT,
                initial_price   TEXT,
                created_date    FLOAT,
                updated_date    FLOAT,
                FOREIGN KEY(symbol_id) REFERENCES symbology(symbol_id))""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_ipop_sid
                            ON ipo_pricings(symbol_id)""")

            def splits(c):
                c.execute("""CREATE TABLE IF NOT EXISTS splits
                (split_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol_id       INTEGER,
                symbol          TEXT,
                company_name    TEXT,
                payable_date    FLOAT,
                ex_date         FLOAT,
                announced_date  FLOAT,
                optionable      INTEGER,
                ratio           FLOAT,
                created_date    FLOAT,
                updated_date    FLOAT,
                FOREIGN KEY(symbol_id) REFERENCES symbology(symbol_id))""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_splits_sid
                            ON splits(symbol_id)""")

            conference_calls(cur)
            earnings(cur)
            economic_events(cur)
            ipo_pricings(cur)
            splits(cur)

            print('All tables in events_tables are in the database.')

    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to create the main events tables in the database')
        print(e)
        return
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in events_tables. Make '
              'sure the database address/name are correct.')
        return
    except Exception as e:
        print(e)
        raise SystemError('Error: An unknown issue occurred in events_tables')
