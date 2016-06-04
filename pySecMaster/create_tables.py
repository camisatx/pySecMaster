import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from utilities.user_dir import user_dir

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.4.0'

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
#   fundamentals    (company, energy, economic)
#   futures_prices


def create_database(admin_user='postgres', admin_password='postgres',
                    database='pysecmaster', user='postgres'):
    """ Determine if the provided database exists within the postgres server.
    If the database doesn't exist, create it using the provided user as the
    owner. This requires connecting to the default database before psycopg2 is
    able to send an execute command.

    NOTE: The provided user must have a valid login role within postgres before
    they are able to log into the server and create databases.

    :param admin_user: String of the database admin user
    :param admin_password: String of the database admin password
    :param database: String of the database to create
    :param user: String of the user who should own the database
    """

    userdir = user_dir()['postgresql']

    conn = psycopg2.connect(database=userdir['main_db'],
                            user=admin_user,
                            password=admin_password,
                            host=userdir['main_host'],
                            port=userdir['main_port'])
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn:
            cur = conn.cursor()

            cur.execute("""SELECT datname FROM pg_catalog.pg_database
                        WHERE lower(datname)=lower('%s')""" % database)
            database_exist = cur.fetchone()

            if not database_exist:
                cur.execute("""CREATE DATABASE %s OWNER %s""" %
                            (database, user))
            else:
                print('The %s database already exists.' % database)

            cur.close()

    except psycopg2.Error as e:
        conn.rollback()
        print('Failed to create the %s database' % database)
        print(e)
        return
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in create_database. Make '
              'sure the database address/name are correct.')
        return
    except Exception as e:
        print(e)
        raise SystemError('Error: An unknown issue occurred in create_database')


def main_tables(database='pysecmaster', user='pysecmaster',
                password='pysecmaster', host='localhost', port=5432):

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()

            def baskets(c):
                c.execute('''CREATE TABLE IF NOT EXISTS baskets
                (basket_id      SERIAL                      PRIMARY KEY,
                name            TEXT                        NOT NULL,
                description     TEXT,
                start_date      TIMESTAMP WITH TIME ZONE,
                end_date        TIMESTAMP WITH TIME ZONE,
                created_by      TEXT,
                created_date    TIMESTAMP WITH TIME ZONE,
                updated_date    TIMESTAMP WITH TIME ZONE)''')

            def basket_values(c):
                c.execute('''CREATE TABLE IF NOT EXISTS basket_values
                (basket_val_id  BIGSERIAL                   PRIMARY KEY,
                basket_id       INTEGER                     NOT NULL,
                source          TEXT                        NOT NULL,
                source_id       TEXT                        NOT NULL,
                updated_date    TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(basket_id) REFERENCES baskets(basket_id),
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)''')

            def classification(c):
                c.execute("""CREATE TABLE IF NOT EXISTS classification
                (classification_id  BIGINT                      PRIMARY KEY,
                source_id           TEXT                        NOT NULL,
                source              TEXT                        NOT NULL,
                standard            TEXT,
                code                INTEGER,
                level_1             TEXT,
                level_2             TEXT,
                level_3             TEXT,
                level_4             TEXT,
                created_date        TIMESTAMP WITH TIME ZONE,
                updated_date        TIMESTAMP WITH TIME ZONE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS
                    idx_classification_values
                ON classification(source, source_id, standard, level_1,
                    level_2, level_3, level_4)""")

            def csidata_stock_factsheet(c):
                c.execute('''CREATE TABLE IF NOT EXISTS csidata_stock_factsheet
                (csi_number         TEXT                        PRIMARY KEY,
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
                updated_date        TIMESTAMP WITH TIME ZONE)''')
                c.execute("""CREATE INDEX IF NOT EXISTS idx_csidata_symbol
                ON csidata_stock_factsheet(symbol)""")

            def data_vendor(c):
                c.execute('''CREATE TABLE IF NOT EXISTS data_vendor
                (data_vendor_id     INTEGER                     PRIMARY KEY,
                name                TEXT                        UNIQUE,
                url                 TEXT,
                support_email       TEXT,
                api                 TEXT,
                consensus_weight    SMALLINT,
                created_date        TIMESTAMP WITH TIME ZONE,
                updated_date        TIMESTAMP WITH TIME ZONE)''')

            def exchanges(c):
                c.execute('''CREATE TABLE IF NOT EXISTS exchanges
                 (exchange_id       SMALLINT                    PRIMARY KEY,
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
                 updated_date       TIMESTAMP WITH TIME ZONE)''')

            def indices(c):
                c.execute('''CREATE TABLE IF NOT EXISTS indices
                (index_id           SERIAL                      PRIMARY KEY,
                stock_index         TEXT                        NOT NULL,
                source              TEXT                        NOT NULL,
                source_id           TEXT                        NOT NULL,
                as_of_date          TIMESTAMP WITH TIME ZONE,
                created_date        TIMESTAMP WITH TIME ZONE,
                updated_date        TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)''')

            def quandl_codes(c):
                c.execute('''CREATE TABLE IF NOT EXISTS quandl_codes
                (q_code_id          BIGSERIAL                   PRIMARY KEY,
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
                    ON UPDATE CASCADE)''')
                c.execute("""CREATE INDEX IF NOT EXISTS idx_qc_data
                            ON quandl_codes(data)""")

            def symbology(c):
                c.execute('''CREATE TABLE IF NOT EXISTS symbology
                (symbol_id      BIGINT                      NOT NULL,
                source          TEXT                        NOT NULL,
                source_id       TEXT                        NOT NULL,
                type            TEXT,
                created_date    TIMESTAMP WITH TIME ZONE,
                updated_date    TIMESTAMP WITH TIME ZONE)''')
                c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_symbology_sources ON symbology(source, source_id)""")

            def tickers(c):
                c.execute('''CREATE TABLE IF NOT EXISTS tickers
                (tsid               TEXT                        PRIMARY KEY,
                ticker              TEXT                        NOT NULL,
                name                TEXT,
                exchange            TEXT                        NOT NULL,
                child_exchange      TEXT,
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
                FOREIGN KEY(exchange)
                    REFERENCES exchanges(abbrev) ON UPDATE CASCADE)''')
                c.execute("""CREATE INDEX IF NOT EXISTS idx_tickers_sector
                            ON tickers(sector)""")

            symbology(cur)
            baskets(cur)
            basket_values(cur)
            classification(cur)
            csidata_stock_factsheet(cur)
            data_vendor(cur)
            exchanges(cur)
            indices(cur)
            quandl_codes(cur)
            # tickers(cur)

            conn.commit()

            cur.close()

            print('All tables in MainTables are created')

    except psycopg2.Error as e:
        conn.rollback()
        print('Failed to create the main tables in the database')
        print(e)
        return
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in main_tables. Make '
              'sure the database address/name are correct.')
        return
    except Exception as e:
        print(e)
        raise SystemError('Error: An unknown issue occurred in main_tables')


def data_tables(database='pysecmaster', user='pysecmaster',
                password='pysecmaster', host='localhost', port=5432):

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()

            def daily_prices(c):
                c.execute('''CREATE TABLE IF NOT EXISTS daily_prices
                (daily_price_id BIGSERIAL                   PRIMARY KEY,
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
                    ON UPDATE CASCADE)''')
                c.execute("""CREATE INDEX IF NOT EXISTS idx_dp_identifiers
                    ON daily_prices(source_id, data_vendor_id,
                        date DESC NULLS LAST, updated_date)""")
                # c.execute("""CREATE INDEX IF NOT EXISTS idx_dp_values
                #     ON daily_prices(source_id, data_vendor_id, date, close,
                #         volume)""")

            def finra_data(c):
                c.execute('''CREATE TABLE IF NOT EXISTS finra_data
                (finra_id               SERIAL                      PRIMARY KEY,
                source                  TEXT                        NOT NULL,
                source_id               TEXT                        NOT NULL,
                date                    TIMESTAMP WITH TIME ZONE    NOT NULL,
                short_volume            INTEGER,
                short_exempt_volume     INTEGER,
                total_volume            INTEGER,
                updated_date            TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)''')
                c.execute('''CREATE INDEX IF NOT EXISTS idx_finra_source_id
                            ON finra_data(source_id)''')

            def fundamental_data(c):
                c.execute('''CREATE TABLE IF NOT EXISTS fundamental_data
                (fundamental_id BIGSERIAL                   PRIMARY KEY,
                source          TEXT                        NOT NULL,
                source_id       TEXT                        NOT NULL,
                date            TIMESTAMP WITH TIME ZONE    NOT NULL,
                value           DECIMAL(14,2),
                note            TEXT,
                created_date    TIMESTAMP WITH TIME ZONE,
                updated_date    TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)''')
                c.execute("""CREATE INDEX IF NOT EXISTS idx_fund_source_id
                            ON fundamental_data(source_id)""")

            def minute_prices(c):
                c.execute('''CREATE TABLE IF NOT EXISTS minute_prices
                (minute_price_id    BIGSERIAL                   PRIMARY KEY,
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
                    ON UPDATE CASCADE)''')
                c.execute("""CREATE INDEX IF NOT EXISTS idx_mp_identifiers
                    ON minute_prices(source_id, data_vendor_id,
                    date DESC NULLS LAST, updated_date)""")
                # c.execute("""CREATE INDEX IF NOT EXISTS idx_mp_values
                #     ON minute_prices(source_id, data_vendor_id, date, close,
                #         volume)""")

            def option_chains(c):
                c.execute("""CREATE TABLE IF NOT EXISTS option_chains
                (option_id      BIGSERIAL                   PRIMARY KEY,
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
                bid             DECIMAL(10,4),
                bid_size        INTEGER,
                ask             DECIMAL(10,4),
                ask_size        INTEGER,
                close           DECIMAL(10,4),
                open_interest   INTEGER,
                volume          INTEGER,
                imp_volatility  DECIMAL(6,4),
                delta           DECIMAL(6,5),
                gamma           DECIMAL(6,5),
                rho             DECIMAL(6,5),
                theta           DECIMAL(6,5),
                vega            DECIMAL(6,5),
                updated_date    TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(data_vendor_id)
                    REFERENCES data_vendor(data_vendor_id),
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_option_chains_values
                    ON option_chains(source_id, contract_id, expiry, strike)""")

            def tick(c):
                c.execute("""CREATE TABLE IF NOT EXISTS tick_prices
                (tick_id        BIGSERIAL                   PRIMARY KEY,
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
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_tick_values
                            ON tick_prices(source_id, date DESC NULLS LAST)""")

            def tick_stream(c):
                c.execute("""CREATE TABLE IF NOT EXISTS tick_prices_stream
                (tick_id        BIGSERIAL                   PRIMARY KEY,
                source          TEXT                        NOT NULL,
                source_id       TEXT                        NOT NULL,
                date            TIMESTAMP WITH TIME ZONE,
                field           TEXT,
                value           DECIMAL(11,4),
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_tick_stream_values
                            ON tick_prices_stream(source_id,
                            date DESC NULLS LAST, field)""")

            daily_prices(cur)
            finra_data(cur)
            fundamental_data(cur)
            minute_prices(cur)
            option_chains(cur)
            tick(cur)
            tick_stream(cur)

            conn.commit()
            cur.close()

            print('All tables in data_tables are created')

    except psycopg2.Error as e:
        conn.rollback()
        print('Failed to create the data tables in the database')
        print(e)
        return
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in data_tables. Make '
              'sure the database address/name are correct.')
        return
    except Exception as e:
        print(e)
        raise SystemError('Error: An unknown issue occurred in data_tables')


def events_tables(database='pysecmaster', user='pysecmaster',
                  password='pysecmaster', host='localhost', port=5432):

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()

            def conference_calls(c):
                c.execute("""CREATE TABLE IF NOT EXISTS conference_calls
                (conf_call_id       SERIAL                      PRIMARY KEY,
                source              TEXT,
                source_id           TEXT,
                symbol              TEXT,
                date                TIMESTAMP WITH TIME ZONE    NOT NULL,
                event_title         TEXT,
                created_date        TIMESTAMP WITH TIME ZONE,
                updated_date        TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_conf_source_id
                            ON conference_calls(source_id, date)""")

            def dividends(c):
                c.execute("""CREATE TABLE IF NOT EXISTS dividends
                (dividend_id        SERIAL                      PRIMARY KEY,
                source              TEXT,
                source_id           TEXT,
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
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_div_source_id
                            ON dividends(source_id)""")

            def earnings(c):
                c.execute("""CREATE TABLE IF NOT EXISTS earnings
                (earnings_id    SERIAL                      PRIMARY KEY,
                source          TEXT,
                source_id       TEXT,
                symbol          TEXT,
                company_name    TEXT,
                date            TIMESTAMP WITH TIME ZONE    NOT NULL,
                reported_eps    DECIMAL(6,3),
                consensus_eps   DECIMAL(6,3),
                created_date    TIMESTAMP WITH TIME ZONE,
                updated_date    TIMESTAMP WITH TIME ZONE,
                FOREIGN KEY(source, source_id)
                    REFERENCES symbology(source, source_id)
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_earn_source_id
                            ON earnings(source_id)""")

            def economic_events(c):
                c.execute("""CREATE TABLE IF NOT EXISTS economic_events
                (event_id           SERIAL                      PRIMARY KEY,
                source              TEXT,
                source_id           TEXT,
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
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_econ_event_source_id
                            ON economic_events(source_id)""")

            def ipo_pricings(c):
                c.execute("""CREATE TABLE IF NOT EXISTS ipo_pricings
                (ipo_id         SERIAL                      PRIMARY KEY,
                source          TEXT,
                source_id       TEXT,
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
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_ipop_source_id
                            ON ipo_pricings(source_id)""")

            def splits(c):
                c.execute("""CREATE TABLE IF NOT EXISTS splits
                (split_id       SERIAL                      PRIMARY KEY,
                source          TEXT,
                source_id       TEXT,
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
                    ON UPDATE CASCADE)""")
                c.execute("""CREATE INDEX IF NOT EXISTS idx_splits_source_id
                            ON splits(source_id)""")

            conference_calls(cur)
            dividends(cur)
            earnings(cur)
            economic_events(cur)
            ipo_pricings(cur)
            splits(cur)

            conn.commit()
            cur.close()

            print('All tables in events_tables are in the database.')

    except psycopg2.Error as e:
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


if __name__ == '__main__':

    create_database()
    main_tables()
    data_tables()
    events_tables()
