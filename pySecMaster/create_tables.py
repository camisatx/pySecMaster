__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2015 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.0'

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

import sqlite3

# Future table additions:
#   corporate_actions
#   holidays
#   fundamentals    (company, energy, economic)
#   futures_prices


def main_tables(db_location):

    conn = sqlite3.connect(db_location)
    cur = conn.cursor()

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

    data_vendor(cur)
    quandl_codes(cur)
    fundamental_data(cur)

    print('All tables in MainTables are created')


def stock_tables(db_location):

    conn = sqlite3.connect(db_location)
    cur = conn.cursor()

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
        (symbol_id              INTEGER PRIMARY KEY,
        ticker                  TEXT,
        exchange                TEXT,
        sector                  TEXT,
        industry                TEXT,
        sub_industry            TEXT,
        currency                TEXT,
        hq_country              TEXT,
        created_date            FLOAT,
        updated_date            FLOAT,
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

    exchange(cur)
    indices(cur)
    tickers(cur)
    daily_prices(cur)
    minute_prices(cur)
    finra_data(cur)
    # options_prices(cur)

    print('All tables in StockTables are created')
