from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.4.3'

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


def delete_sql_table_rows(database, user, password, host, port, query, table,
                          item, verbose=False):
    """ Execute the provided query in the specified table in the database.
    Normally, this will delete the existing prices over dates where the new
    prices would overlap. Returns a string value indicating whether the query
    was successfully executed.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param query: String representing the SQL query to perform on the database.
    :param table: String indicating which table should be worked on.
    :param item: String of the tsid being worked on.
    :param verbose: Boolean indicating whether debugging prints should occur.
    :return: String of either 'success' or 'failure', which the function that
        called this function uses determine whether it should add new values.
    """

    if verbose:
        print('Deleting all rows in %s that fit the provided criteria' % table)

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    try:
        with conn:
            cur = conn.cursor()
            cur.execute(query)
        outcome = 'success'
    except psycopg2.Error as e:
        conn.rollback()
        print(e)
        print('Error: Not able to delete the overlapping rows for %s in '
              'the %s table.' % (item, table))
        conn.close()
        outcome = 'failure'
    except conn.OperationalError:
        print('Unable to connect to the %s database in delete_sql_table_rows. '
              'Make sure the database address/name are correct.' % database)
        conn.close()
        outcome = 'failure'
    except Exception as e:
        print('Error: Unknown issue when trying to delete overlapping rows for'
              '%s in the %s table.' % (item, table))
        print(e)
        conn.close()
        outcome = 'failure'

    conn.close()
    return outcome


def df_to_sql(database, user, password, host, port, df, sql_table, exists,
              item, verbose=False):
    """ Save a DataFrame to a specified SQL database table.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param df: Pandas DataFrame with values to insert into the SQL database.
    :param sql_table: String indicating which table the DataFrame should be
        put into.
    :param exists: String indicating how the DataFrame values should interact
        with the existing values in the table. Valid parameters include
        'append' [new rows] and 'replace' [all existing table rows].
    :param item: String representing the item being inserted (i.e. the tsid)
    :param verbose: Boolean indicating whether debugging statements should print
    """

    if verbose:
        print('Entering the data for %s into the %s database.' %
              (item, database))

    engine = create_engine('postgresql://%s:%s@%s:%s/%s' %
                           (user, password, host, port, database))
    conn = engine.connect()

    # Try and except block writes the new data to the SQL Database.
    try:
        # if_exists options: append new df rows, replace all table values
        df.to_sql(sql_table, conn, if_exists=exists, index=False)
        if verbose:
            print('Successfully entered the values into the %s database' %
                  database)
    except Exception as e:
        print('Error: Unknown issue when adding the DataFrame to the %s '
              'database for %s' % (database, item))
        print(e)

    conn.close()


def query_all_active_tsids(database, user, password, host, port, table,
                           period=None):
    """ Get a list of all tickers that have data.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param table: String of the table that should be queried from
    :param period: Optional integer indicating the prior number of days a
        tsid must have had active data before it should be included.
    :return: DataFrame of all of the tsids for the specified table
    """

    # Two options for getting complete list of tickers
    #   1. Select all codes from symbology, then try each code to see if it has
    #       data. Initially quicker, but wasteful
    #   2. Select all unique codes from the prices table. Might take longer
    #       to query the initial data, but it is accurate

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()

            if period:
                beg_date = datetime.today() - timedelta(days=period)
                # query = ("""SELECT DISTINCT ON (source_id)
                #                 source_id as tsid
                #          FROM %s
                #          WHERE source='tsid' AND date>='%s'
                #          ORDER BY source_id ASC NULLS LAST""" %
                #          (table, beg_date))
                query = ("""SELECT sym.source_id AS tsid
                         FROM symbology AS sym,
                         LATERAL (
                             SELECT source_id
                             FROM %s
                             WHERE source_id = sym.source_id
                             AND source='tsid' AND date>='%s'
                             ORDER BY source_id ASC NULLS LAST
                             LIMIT 1) AS prices""" %
                         (table, beg_date))
            else:
                # Option 1:
                # query = ("""SELECT source_id
                #             FROM symbology
                #             WHERE source='tsid'""")

                # Option 2:
                # query = ("""SELECT DISTINCT ON (source_id)
                #              source_id as tsid
                #          FROM %s
                #          ORDER BY source_id ASC NULLS LAST""" % (table,))

                # Option 3:
                query = ("""SELECT sym.source_id AS tsid
                         FROM symbology AS sym,
                         LATERAL (
                             SELECT source_id
                             FROM %s
                             WHERE source_id = sym.source_id
                             AND source='tsid'
                             ORDER BY source_id ASC NULLS LAST
                             LIMIT 1) AS prices""" %
                         (table,))

            cur.execute(query)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid'])
                df.drop_duplicates(inplace=True)
            else:
                raise TypeError('Not able to query any tsid codes in '
                                'query_all_active_tsids')
    except psycopg2.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the %s database '
                        'in query_all_active_tsids' % database)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_all_active_tsids. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_all_active_tsids')

    conn.close()
    return df


def query_all_tsid_prices(database, user, password, host, port, table, tsid):
    """ Query all relevant interval data for this ticker from the relevant
    sources. Then process the price DataFrame.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param table: String of the database table to query from
    :param tsid: String of the tsid whose prices should be queried
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()
            if table == 'daily_prices':
                cur.execute("""SELECT data_vendor_id, date, open, high, low,
                                close, volume, ex_dividend, split_ratio
                            FROM daily_prices
                            WHERE source_id=%s AND source='tsid'""", (tsid,))
                columns = ['data_vendor_id', 'date', 'open', 'high', 'low',
                           'close', 'volume', 'ex_dividend', 'split_ratio']
            elif table == 'minute_prices':
                cur.execute("""SELECT data_vendor_id, date, open, high, low,
                                close, volume
                            FROM minute_prices
                            WHERE source_id=%s AND source='tsid'""", (tsid,))
                columns = ['data_vendor_id', 'date', 'open', 'high', 'low',
                           'close', 'volume']
            else:
                raise NotImplementedError('Table %s is not implemented '
                                          'within query_all_tsid_prices in'
                                          'database_queries.py' % table)

            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=columns)

                # Convert the ISO date to a datetime object
                if table == 'daily_prices':
                    df['date'] = pd.to_datetime(df['date'], utc=True)
                    df['date'] = df['date'].apply(lambda x: x.date())
                elif table == 'minute_prices':
                    df['date'] = pd.to_datetime(df['date'], utc=True)
                else:
                    raise NotImplementedError('Table %s is not implemented '
                                              'within query_all_tsid_prices in'
                                              'database_queries.py' % table)

                # Drop duplicate rows based on only the tsid and date columns
                df.drop_duplicates(subset=['data_vendor_id', 'date'],
                                   inplace=True)

                # Move and set the date and data_vendor_id columns to the index
                df.set_index(['date', 'data_vendor_id'], inplace=True)

                # Have to rename the indices as set_index removes the names
                df.index.name = ['date', 'data_vendor_id']

                df.sort_index(inplace=True)
            else:
                raise TypeError('Not able to query any prices for %s in '
                                'query_all_tsid_prices' % tsid)
    except psycopg2.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the %s database '
                        'in query_all_tsid_prices' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_all_tsid_prices')

    conn.close()
    return df


def query_codes(database, user, password, host, port, download_selection):
    """ Builds a DataFrame of tsid codes from a SQL query. These codes are the
    items that will have their data downloaded.

    With more databases, it may be necessary to have the user
    write custom queries if they only want certain items downloaded.
    Perhaps the best way will be to have some predefined queries, and if
    those don't work for the user, they write a custom query.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param download_selection: String that specifies which data is required
    :return: DataFrame with the the specified tsid values
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()

            # ToDo: Will need to create queries for additional items

            if download_selection == 'all':
                # Retrieve all tsid stock tickers
                cur.execute("""SELECT source_id
                               FROM symbology
                               WHERE source='tsid'""")
            elif download_selection == 'us_main':
                # Retrieve tsid tickers that trade only on main US exchanges
                #   and that have been active within the prior two years.
                beg_date = (datetime.now() - timedelta(days=730))
                cur.execute("""SELECT DISTINCT ON (source_id) source_id
                               FROM symbology
                               WHERE source='tsid'
                               AND symbol_id IN (
                                   SELECT symbol_id
                                   FROM symbology
                                   WHERE source='csi_data'
                                   AND source_id IN (
                                       SELECT csi_number
                                       FROM csidata_stock_factsheet
                                       WHERE end_date>=%s
                                       AND (exchange IN ('AMEX', 'NYSE')
                                       OR sub_exchange IN ('AMEX',
                                           'BATS Global Markets',
                                           'Nasdaq Capital Market',
                                           'Nasdaq Global Market',
                                           'Nasdaq Global Select',
                                           'NYSE', 'NYSE ARCA'))))
                               ORDER BY source_id ASC NULLS LAST""",
                            (beg_date.isoformat(),))
            elif download_selection == 'us_main_no_end_date':
                # Retrieve tsid tickers that trade only on main US exchanges
                cur.execute("""SELECT DISTINCT ON (source_id) source_id
                               FROM symbology
                               WHERE source='tsid'
                               AND symbol_id IN (
                                   SELECT symbol_id
                                   FROM symbology
                                   WHERE source='csi_data'
                                   AND source_id IN (
                                       SELECT csi_number
                                       FROM csidata_stock_factsheet
                                       WHERE (exchange IN ('AMEX', 'NYSE')
                                       OR sub_exchange IN ('AMEX',
                                           'BATS Global Markets',
                                           'Nasdaq Capital Market',
                                           'Nasdaq Global Market',
                                           'Nasdaq Global Select',
                                           'NYSE', 'NYSE ARCA'))))
                               ORDER BY source_id ASC NULLS LAST""")
            elif download_selection == 'us_canada_london':
                # Retrieve tsid tickers that trade on AMEX, LSE, MSE, NYSE,
                #   NASDAQ, TSX, VSE and PINK exchanges, and that have been
                #   active within the prior two years.
                beg_date = (datetime.now() - timedelta(days=730))
                cur.execute("""SELECT DISTINCT ON (source_id) source_id
                               FROM symbology
                               WHERE source='tsid'
                               AND symbol_id IN (
                                   SELECT symbol_id
                                   FROM symbology
                                   WHERE source='csi_data'
                                   AND source_id IN (
                                       SELECT csi_number
                                       FROM csidata_stock_factsheet
                                       WHERE end_date>=%s
                                       AND (exchange IN ('AMEX', 'LSE', 'NYSE',
                                       'TSX', 'VSE')
                                       OR sub_exchange IN ('AMEX',
                                           'BATS Global Markets',
                                           'Nasdaq Capital Market',
                                           'Nasdaq Global Market',
                                           'Nasdaq Global Select',
                                           'NYSE', 'NYSE ARCA',
                                           'OTC Markets Pink Sheets'))))
                               ORDER BY source_id ASC NULLS LAST""",
                            (beg_date.isoformat(),))
            else:
                raise TypeError('Improper download_selection was '
                                'provided in query_codes. If this is '
                                'a new query, ensure the SQL is '
                                'correct. Valid symbology download '
                                'selections include all, us_main,'
                                'us_main_no_end_date and us_canada_london.')

            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid'])
                df.drop_duplicates(inplace=True)

                # df.to_csv('query_tsid.csv')

            else:
                raise TypeError('Not able to determine the tsid from '
                                'the SQL query in query_codes.')
    except psycopg2.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the %s database '
                        'in query_codes' % database)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_codes. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_codes')

    conn.close()
    return df


def query_csi_stocks(database, user, password, host, port, query='all'):
    """ Query the CSI stock data based on the query specified.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param query: String of which query to run
    :return: DataFrame of exchanges
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    csi_df = None

    try:
        with conn:
            cur = conn.cursor()

            if query == 'all':
                cur.execute("""SELECT csi_number, symbol, exchange,
                               sub_exchange
                               FROM csidata_stock_factsheet""")
                rows = cur.fetchall()
                csi_df = pd.DataFrame(rows, columns=['sid', 'ticker',
                                                     'exchange',
                                                     'sub_exchange'])
                csi_df.sort_values('sid', axis=0, inplace=True)
                csi_df.reset_index(drop=True, inplace=True)

            elif query == 'exchanges_only':
                # Restricts tickers to those that are traded on exchanges only
                #   (AMEX, LSE, MSE, NYSE, OTC (NASDAQ, BATS), TSX, VSE). For
                #   the few duplicate tickers, choose the active one over the
                #   non-active one (same company but different start and end
                #   dates, with one being active).
                # NOTE: Due to different punctuations, it is possible for
                #   items with similar symbol, exchange and sub exchange
                #   to be returned (ie. 'ABK.A TSX' and 'ABK+A TSX')
                cur.execute("""SELECT DISTINCT ON (symbol, exchange)
                                   csi_number, symbol, exchange, sub_exchange
                               FROM (SELECT csi_number, symbol, exchange,
                                   sub_exchange, is_active
                                   FROM csidata_stock_factsheet
                                   WHERE (exchange IN ('AMEX', 'LSE', 'NYSE',
                                       'TSX', 'VSE')
                                   OR sub_exchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA',
                                       'OTC Markets Pink Sheets'))
                                   AND symbol IS NOT NULL)
                                   AS csi
                               ORDER BY symbol, exchange, is_active DESC
                                   NULLS LAST""")
                rows = cur.fetchall()
                if rows:
                    csi_df = pd.DataFrame(rows, columns=['sid', 'ticker',
                                                         'exchange',
                                                         'sub_exchange'])
                else:
                    raise SystemExit('Not able to retrieve any tickers after '
                                     'querying %s in query_csi_stocks'
                                     % (query,))

            elif query == 'main_us':
                # Restricts tickers to those that have been active within the
                #   prior two years. For the few duplicate tickers, choose the
                #   active one over the non-active one (same company but
                #   different start and end dates, with one being active).
                # NOTE: Due to different punctuations, it is possible for
                #   items with similar symbol, exchange and sub exchange
                #   to be returned (ie. 'ABK.A TSX' and 'ABK+A TSX')
                beg_date = (datetime.now() - timedelta(days=730))
                cur.execute("""SELECT DISTINCT ON (symbol, exchange)
                                   csi_number, symbol, exchange, sub_exchange
                               FROM (SELECT csi_number, symbol, exchange,
                                   sub_exchange, is_active
                                   FROM csidata_stock_factsheet
                                   WHERE end_date>=%s
                                   AND (exchange IN ('AMEX', 'NYSE')
                                   OR sub_exchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA'))
                                   AND symbol IS NOT NULL)
                                   AS csi
                               ORDER BY symbol, exchange, is_active DESC
                                   NULLS LAST""",
                            (beg_date.isoformat(),))
                rows = cur.fetchall()
                if rows:
                    csi_df = pd.DataFrame(rows, columns=['sid', 'ticker',
                                                         'exchange',
                                                         'sub_exchange'])
                else:
                    raise SystemExit('Not able to retrieve any tickers after '
                                     'querying %s in query_csi_stocks'
                                     % (query,))
            else:
                raise SystemExit('%s query does not exist within '
                                 'query_csi_stocks. Valid queries '
                                 'include: all, main_us, exchanges_only' %
                                 (query,))
    except psycopg2.Error as e:
        print(e)
        raise SystemError('Failed to query the data into the symbology table '
                          'within query_csi_stocks')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_csi_stocks. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_csi_stocks')

    conn.close()
    return csi_df


def query_csi_stock_start_date(database, user, password, host, port, tsid):
    """ Query the start date for the provided stock based on the start date
    in the csi data stock table.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param tsid: String of which tsid to check
    :return: Datetime object representing the start date
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT start_date
                        FROM csidata_stock_factsheet
                        WHERE csi_number=(
                            SELECT csi.source_id
                            FROM symbology csi
                            INNER JOIN symbology tsid
                            ON csi.symbol_id=tsid.symbol_id
                            WHERE csi.source='csi_data'
                            AND tsid.source='tsid'
                            AND tsid.source_id=(%s))""", (tsid,))
            row = cur.fetchone()
            if row:
                start_date_obj = row[0]
                start_date = start_date_obj.strftime('%Y-%m-%d')
            else:
                start_date = None
    except psycopg2.Error as e:
        print(e)
        raise SystemError('Failed to query the start date for %s within '
                          'query_csi_stock_start_date' % tsid)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_csi_stock_start_date. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_csi_stock_start_date')

    conn.close()
    return start_date


def query_data_vendor_id(database, user, password, host, port, name):
    """ Takes the name provided and tries to find data vendor(s) from the
    data_vendor table in the database. If nothing is returned in the
    query, then 'Unknown' is used.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param name: String that has the database name, or a special SQL string
        to retrieve extra ids (i.e. 'Quandl_%' to retrieve all Quandl ids)
    :return: If one vendor id is queried, return a int of the data vendor's id.
        If multiple ids are queried, return a list of all the ids. Otherwise,
        return 'Unknown'.
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    data = None

    try:
        with conn:
            cur = conn.cursor()
            query = """SELECT data_vendor_id
                    FROM data_vendor
                    WHERE name LIKE '%s'""" % name
            cur.execute(query)
            data = cur.fetchall()
            if data:  # A vendor was found
                if len(data) == 1:
                    # Only one vendor id returned, so only return that value
                    data = data[0][0]
                else:
                    # Multiple vendor ids were returned; return a list of them
                    df = pd.DataFrame(data, columns=['data_vendor_id'])
                    data = df['data_vendor_id'].values.tolist()
            else:
                data = 'Unknown'
                print('Not able to determine the data_vendor_id for %s'
                      % name)
    except psycopg2.Error as e:
        print('Error when trying to retrieve data from the %s database in '
              'query_data_vendor_id' % database)
        print(e)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_data_vendor_id. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_data_vendor_id')

    conn.close()
    return data


def query_existing_sid(database, user, password, host, port, source):
    """ Query existing symbology id values based on the provided source.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param source: String of which symbology source to query
    :return: DataFrame of exchanges
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT symbol_id, source_id
                        FROM symbology
                        WHERE source=%s""", (source,))
            rows = cur.fetchall()
            sid_df = pd.DataFrame(rows, columns=['symbol_id', 'source_id'])
    except psycopg2.Error as e:
        print(e)
        raise SystemError('Failed to query the %s data from the symbology '
                          'table within query_existing_sid' % source)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_existing_sid. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_existing_sid')

    conn.close()
    return sid_df


def query_exchanges(database, user, password, host, port):
    """
    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :return: DataFrame of exchanges
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT symbol, name, goog_symbol, yahoo_symbol,
                        csi_symbol, tsid_symbol
                        FROM exchanges""")
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=['symbol', 'name', 'goog_symbol',
                                             'yahoo_symbol', 'csi_symbol',
                                             'tsid_symbol'])

    except psycopg2.Error as e:
        print(e)
        raise SystemError('Failed to query the data from the exchange table '
                          'within query_exchanges')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_exchanges. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_exchanges')

    conn.close()
    return df


def query_last_price(database, user, password, host, port, table, vendor_id):
    """ Queries the pricing database to find the latest dates for each item
    in the database, regardless of whether it is in the tsid list.

    - DISTINCT ON answer
    http://stackoverflow.com/a/16920077
    - Optimize Groupwise maximum query
    http://stackoverflow.com/a/24377356
    - Performance optimization for many rows
    http://stackoverflow.com/a/25536748

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param table: String of the table whose prices should be worked on
    :param vendor_id: Integer or list of integers representing the vendor id
        whose prices should be considered
    :return: Returns a DataFrame with the tsid and the date of the latest
        data point for all tickers in the database.
    """

    if type(vendor_id) == list:
        vendor_id = ', '.join(["'" + str(vendor) + "'" for vendor in vendor_id])
    elif type(vendor_id) == int:
        vendor_id = str(vendor_id)
    else:
        # Should never occur
        raise TypeError('%s is an invalid type provided for the vendor_id '
                        'variable in query_last_price.' % type(vendor_id))

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()
            if table == 'daily_prices':
                # DISTINCT ON query - Good for small tables
                # cur.execute("""SELECT DISTINCT ON (source_id)
                #                 source_id AS tsid, date, updated_date
                #             FROM daily_prices
                #             WHERE data_vendor_id IN (%s)
                #             ORDER BY source_id, date DESC NULLS LAST""",
                #             (vendor_id,))
                # LATERAL join query - Good for very large tables
                #   Query from 2a in http://stackoverflow.com/a/25536748
                cur.execute("""SELECT sym.source_id, prices.date,
                                prices.updated_date
                            FROM symbology AS sym,
                            LATERAL (
                                SELECT date, updated_date
                                FROM daily_prices
                                WHERE source_id = sym.source_id
                                AND source = sym.source
                                AND data_vendor_id IN (%s)
                                ORDER BY source_id, date DESC NULLS LAST
                                LIMIT 1) AS prices""",
                            (vendor_id,))
            elif table == 'minute_prices':
                # DISTINCT ON query - Good for small tables
                # cur.execute("""SELECT DISTINCT ON (source_id)
                #                 source_id AS tsid, date, updated_date
                #             FROM minute_prices
                #             WHERE data_vendor_id IN (%s)
                #             ORDER BY source_id, date DESC NULLS LAST""",
                #             (vendor_id,))
                # LATERAL join query - Good for very large tables
                #   Query from 2a in http://stackoverflow.com/a/25536748
                cur.execute("""SELECT sym.source_id, prices.date,
                                prices.updated_date
                            FROM symbology AS sym,
                            LATERAL (
                                SELECT date, updated_date
                                FROM minute_prices
                                WHERE source_id = sym.source_id
                                AND source = sym.source
                                AND data_vendor_id IN (%s)
                                ORDER BY source_id, date DESC NULLS LAST
                                LIMIT 1) AS prices""",
                            (vendor_id,))
            else:
                raise NotImplementedError('%s is not implemented in '
                                          'query_last_price' % table)

            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=['tsid', 'date', 'updated_date'])
            df.set_index(['tsid'], inplace=True)

            if len(df.index) > 0:
                # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df['updated_date'] = pd.to_datetime(df['updated_date'], utc=True)
                # df.to_csv('query_last_price.csv')
    except psycopg2.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the %s database '
                        'in query_last_price.' % database)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_last_price. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_last_price')

    conn.close()
    return df


def query_load_table(database, user, password, host, port, table):
    """ Query all existing values from the provided table. Used in
    load_aux_tables for comparing existing and new values. Relevant tables
    include data_vendor and exchanges.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param table: String of which table to query
    :return: DataFrame of table values
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()
            if table == 'data_vendor':
                cur.execute("""SELECT * FROM data_vendor""")
                columns = ['data_vendor_id', 'name', 'url', 'support_email',
                           'api', 'consensus_weight', 'created_date',
                           'updated_date']
            elif table == 'exchanges':
                cur.execute("""SELECT * FROM exchanges""")
                columns = ['exchange_id', 'symbol', 'goog_symbol',
                           'yahoo_symbol', 'csi_symbol', 'tsid_symbol',
                           'name', 'country', 'city', 'currency', 'time_zone',
                           'utc_offset', 'open', 'close', 'lunch',
                           'created_date', 'updated_date']
            else:
                raise NotImplementedError('%s is not implemented within '
                                          'query_load_table' % table)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=columns)

            # created/updated date columns are not needed for comparison
            df.drop(labels=['created_date', 'updated_date'], axis=1,
                    inplace=True)
    except psycopg2.Error as e:
        print(e)
        raise SystemError('Failed to query the data from the symbology table '
                          '%s within query_load_table' % table)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_load_table. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_load_table')

    conn.close()
    return df


def query_q_codes(database, user, password, host, port, download_selection):
    """ Builds a list of Quandl Codes from a SQL query. These codes are the
    items that will have their data downloaded.

    With more databases, it may be necessary to have the user
    write custom queries if they only want certain items downloaded.
    Perhaps the best way will be to have some predefined queries, and if
    those don't work for the user, they write a custom query.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param download_selection: String that specifies which data is required
    :return: DataFrame with two columns (tsid, q_code)
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()

            # ToDo: Will need to create queries for additional items

            if download_selection == 'wiki':
                cur.execute("""SELECT DISTINCT ON (qcode.source_id)
                                tsid.source_id, qcode.source_id
                            FROM symbology tsid
                            INNER JOIN symbology qcode
                            ON tsid.symbol_id = qcode.symbol_id
                            WHERE tsid.source='tsid'
                            AND qcode.source='quandl_wiki'
                            ORDER BY qcode.source_id ASC NULLS LAST""")
            elif download_selection == 'goog':
                cur.execute("""SELECT DISTINCT ON (qcode.source_id)
                               tsid.source_id, qcode.source_id
                            FROM symbology tsid
                            INNER JOIN symbology qcode
                            ON tsid.symbol_id = qcode.symbol_id
                            WHERE tsid.source='tsid'
                            AND qcode.source='quandl_goog'
                            ORDER BY qcode.source_id ASC NULLS LAST""")
            elif download_selection == 'goog_us_main':
                # Retrieve tsid tickers that trade only on main US exchanges
                #   and that have been active within the prior two years.
                beg_date = (datetime.now() - timedelta(days=730))
                cur.execute("""SELECT DISTINCT ON (qcode.source_id)
                                tsid.source_id, qcode.source_id
                            FROM symbology tsid
                            INNER JOIN symbology qcode
                            ON tsid.symbol_id = qcode.symbol_id
                            WHERE tsid.source='tsid'
                            AND qcode.source='quandl_goog'
                            AND qcode.symbol_id IN (
                                SELECT symbol_id
                                FROM symbology
                                WHERE source='csi_data'
                                AND source_id IN (
                                    SELECT csi_number
                                    FROM csidata_stock_factsheet
                                    WHERE end_date>=%s
                                    AND (exchange IN ('AMEX', 'NYSE')
                                    OR sub_exchange IN ('AMEX',
                                        'BATS Global Markets',
                                        'Nasdaq Capital Market',
                                        'Nasdaq Global Market',
                                        'Nasdaq Global Select',
                                        'NYSE', 'NYSE ARCA'))))
                            ORDER BY qcode.source_id ASC NULLS LAST""",
                            (beg_date.isoformat(),))
            elif download_selection == 'goog_us_main_no_end_date':
                # Retrieve tsid tickers that trade only on main US exchanges
                cur.execute("""SELECT DISTINCT ON (qcode.source_id)
                                tsid.source_id, qcode.source_id
                            FROM symbology tsid
                            INNER JOIN symbology qcode
                            ON tsid.symbol_id = qcode.symbol_id
                            WHERE tsid.source='tsid'
                            AND qcode.source='quandl_goog'
                            AND qcode.symbol_id IN (
                                SELECT symbol_id
                                FROM symbology
                                WHERE source='csi_data'
                                AND source_id IN (
                                    SELECT csi_number
                                    FROM csidata_stock_factsheet
                                    WHERE (exchange IN ('AMEX', 'NYSE')
                                    OR sub_exchange IN ('AMEX',
                                        'BATS Global Markets',
                                        'Nasdaq Capital Market',
                                        'Nasdaq Global Market',
                                        'Nasdaq Global Select',
                                        'NYSE', 'NYSE ARCA'))))
                            ORDER BY qcode.source_id ASC NULLS LAST""")
            elif download_selection == 'goog_us_canada_london':
                # Retrieve tsid tickers that trade on AMEX, LSE, MSE, NYSE,
                #   NASDAQ, TSX, VSE and PINK exchanges, and that have been
                #   active within the prior two years.
                beg_date = (datetime.now() - timedelta(days=730))
                cur.execute("""SELECT DISTINCT ON (qcode.source_id)
                                tsid.source_id, qcode.source_id
                            FROM symbology tsid
                            INNER JOIN symbology qcode
                            ON tsid.symbol_id = qcode.symbol_id
                            WHERE tsid.source='tsid'
                            AND qcode.source='quandl_goog'
                            AND qcode.symbol_id IN (
                                SELECT symbol_id
                                FROM symbology
                                WHERE source='csi_data'
                                AND source_id IN (
                                    SELECT csi_number
                                    FROM csidata_stock_factsheet
                                    WHERE end_date>=%s
                                    AND (exchange IN ('AMEX', 'LSE', 'NYSE',
                                        'TSX', 'VSE')
                                    OR sub_exchange IN ('AMEX',
                                        'BATS Global Markets',
                                        'Nasdaq Capital Market',
                                        'Nasdaq Global Market',
                                        'Nasdaq Global Select',
                                        'NYSE', 'NYSE ARCA',
                                        'OTC Markets Pink Sheets'))))
                            ORDER BY qcode.source_id ASC NULLS LAST""",
                            (beg_date.isoformat(),))
            else:
                raise SystemError('Improper download_selection was provided '
                                  'in query_codes. If this is a new query, '
                                  'ensure the SQL is correct. Valid symbology '
                                  'download selections include wiki, goog, '
                                  'goog_us_main, goog_us_main_no_end_date,'
                                  'goog_us_canada_london, and goog_etf.')

            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid', 'q_code'])
                # df.drop_duplicates(inplace=True)

                # ticker_list = df.values.flatten()
                # df.to_csv('query_q_code.csv')
            else:
                raise SystemError('Not able to determine the q_codes '
                                  'from the SQL query in query_q_codes')
    except psycopg2.Error as e:
        print(e)
        raise SystemError('Error when trying to connect to the %s database '
                          'in query_q_codes' % database)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_q_codes. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_q_codes')

    conn.close()
    return df


def query_source_weights(database, user, password, host, port):
    """ Create a DataFrame of the source weights.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :return: DataFrame of all data sources
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    df = None

    try:
        with conn:
            cur = conn.cursor()
            query = ("""SELECT data_vendor_id, consensus_weight
                        FROM data_vendor""")
            cur.execute(query)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['data_vendor_id',
                                                 'consensus_weight'])
                df.drop_duplicates(inplace=True)
            else:
                raise TypeError('Unable to query data vendor weights within '
                                'query_source_weights')
    except psycopg2.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the %s database '
                        'in query_source_weights' % database)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_source_weights. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_source_weights')

    conn.close()
    return df


def query_tsid_based_on_exchanges(database, user, password, host, port,
                                  exchanges_list):
    """ Query all tsids that have one of the provided exchange abbreviations
    in their structure.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param exchanges_list: List of tsid exchange abbreviations
    :return: DataFrame of exchanges
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    exchanges_list_str = ["source_id LIKE '%." + exchange + ".%'"
                          for exchange in exchanges_list]
    exchanges_query = 'AND (' + ' OR '.join(exchanges_list_str) + ')'

    try:
        with conn:
            cur = conn.cursor()
            query = ("""SELECT DISTINCT ON (source_id) source_id
                     FROM symbology
                     WHERE source='tsid'
                     %s
                     ORDER BY source_id""" % (exchanges_query,))
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=['source_id'])

    except psycopg2.Error as e:
        print(e)
        raise SystemError('Failed to query the tsid data from the symbology '
                          'table within query_tsid_based_on_exchanges')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_tsid_based_on_exchanges. Make sure the '
                          'database address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_tsid_based_on_exchanges')

    conn.close()
    return df


def update_load_table(database, user, password, host, port, values_df, table,
                      verbose=False):
    """ Update the load table values for each item in the values_df. Assuming
    that the id column (first column) will never be reused by new values.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param values_df: DataFrame with all the symbology values to update
    :param table: String of the table being worked on
    :param verbose: Boolean of whether to print debugging statement
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    try:
        with conn:
            cur = conn.cursor()

            if table == 'data_vendor':
                for index, row in values_df.iterrows():
                    cur.execute("""UPDATE data_vendor
                                SET name=(%s), url=(%s), support_email=(%s),
                                api=(%s), consensus_weight=(%s),
                                updated_date=(%s)
                                WHERE data_vendor_id=(%s)""",
                                (row['name'], row['url'], row['support_email'],
                                 row['api'], row['consensus_weight'],
                                 row['updated_date'], row['data_vendor_id']))
                    if verbose:
                        print('Updated data vendor id %s' %
                              row['data_vendor_id'])

            elif table == 'exchanges':
                for index, row in values_df.iterrows():
                    cur.execute("""UPDATE exchanges
                                SET symbol=(%s), goog_symbol=(%s),
                                yahoo_symbol=(%s), csi_symbol=(%s),
                                tsid_symbol=(%s), name=(%s), country=(%s),
                                city=(%s), currency=(%s), time_zone=(%s),
                                utc_offset=(%s), open=(%s), close=(%s),
                                lunch=(%s), updated_date=(%s)
                                WHERE exchange_id=(%s)""",
                                (row['symbol'], row['goog_symbol'],
                                 row['yahoo_symbol'], row['csi_symbol'],
                                 row['tsid_symbol'], row['name'],
                                 row['country'], row['city'], row['currency'],
                                 row['time_zone'], row['utc_offset'],
                                 row['open'], row['close'], row['lunch'],
                                 row['updated_date'], row['exchange_id']))
                    if verbose:
                        print('Updated exchange id %s' % row['exchange_id'])

            else:
                raise NotImplementedError('%s is not implemented within '
                                          'update_load_table' % table)
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(e)
        raise SystemError('Failed to update the %s values within '
                          'update_load_table' % table)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'update_load_table. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in update_load_table')

    conn.close()


def update_classification_values(database, user, password, host, port,
                                 values_df, verbose=True):
    """ Update the classification table values for each item in the values_df.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param values_df: DataFrame with all the classification values to update
    :param verbose: Boolean of whether to print debugging statement
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']

    try:
        with conn:
            cur = conn.cursor()
            # Updating each database value
            for index, row in values_df.iterrows():
                if row['code'] not in nothing:
                    cur.execute("""UPDATE classification
                                SET code=(%s), updated_date=(%s)
                                WHERE source='tsid' AND source_id=(%s)
                                AND standard=(%s)""",
                                (row['code'], row['updated_date'],
                                 row['source_id'], row['standard']))
                if row['level_1'] not in nothing:
                    cur.execute("""UPDATE classification
                                SET level_1=(%s), updated_date=(%s)
                                WHERE source='tsid' AND source_id=(%s)
                                AND standard=(%s)""",
                                (row['level_1'], row['updated_date'],
                                 row['source_id'], row['standard']))
                if row['level_2'] not in nothing:
                    cur.execute("""UPDATE classification
                                SET level_2=(%s), updated_date=(%s)
                                WHERE source='tsid' AND source_id=(%s)
                                AND standard=(%s)""",
                                (row['level_2'], row['updated_date'],
                                 row['source_id'], row['standard']))
                if row['level_3'] not in nothing:
                    cur.execute("""UPDATE classification
                                SET level_3=(%s), updated_date=(%s)
                                WHERE source='tsid' AND source_id=(%s)
                                AND standard=(%s)""",
                                (row['level_3'], row['updated_date'],
                                 row['source_id'], row['standard']))
                if row['level_4'] not in nothing:
                    cur.execute("""UPDATE classification
                                SET level_4=(%s), updated_date=(%s)
                                WHERE source='tsid' AND source_id=(%s)
                                AND standard=(%s)""",
                                (row['level_4'], row['updated_date'],
                                 row['source_id'], row['standard']))
                if verbose:
                    print('Updated classification %s values for %s' %
                          (row['standard'], row['source_id']))
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(e)
        raise SystemError('Failed to update the symbology values within '
                          'update_symbology_values')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'update_symbology_values. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'update_symbology_values')

    conn.close()


def update_symbology_values(database, user, password, host, port, values_df,
                            verbose=True):
    """ Update the source_id and updated_date values for each item in the
    values_df. Assuming that the symbol_id will never be reused by new values.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param values_df: DataFrame with all the symbology values to update
    :param verbose: Boolean of whether to print debugging statement
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    try:
        with conn:
            cur = conn.cursor()
            # Updating each database value
            for index, row in values_df.iterrows():
                cur.execute("""UPDATE symbology
                            SET source_id=(%s), updated_date=(%s)
                            WHERE symbol_id=(%s) AND source=(%s)""",
                            (row['source_id'], row['updated_date'],
                             row['symbol_id'], row['source']))
                if verbose:
                    print('Updated symbology source id %s to be %s' %
                          (row['symbol_id'], row['source_id']))
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(e)
        raise SystemError('Failed to update the symbology values within '
                          'update_symbology_values')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'update_symbology_values. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'update_symbology_values')

    conn.close()
