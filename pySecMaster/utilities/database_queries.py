from datetime import datetime, timedelta
import pandas as pd
import sqlite3

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.3.2'

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


def delete_sql_table_rows(db_location, query, table, tsid, verbose=False):
    """ Execute the provided query in the specified table in the database.
    Normally, this will delete the existing prices over dates where the new
    prices would overlap. Returns a string value indicating whether the query
    was successfully executed.

    :param db_location: String of the directory location for the SQL database.
    :param query: String representing the SQL query to perform on the database.
    :param table: String indicating which table should be worked on.
    :param tsid: String of the tsid being worked on.
    :param verbose: Boolean indicating whether debugging prints should occur.
    :return: String of either 'success' or 'failure', which the function that
        called this function uses determine whether it should add new values.
    """

    if verbose:
        print('Deleting all rows in %s that fit the provided criteria' % table)

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(query)
        return 'success'
    except sqlite3.Error as e:
        conn.rollback()
        print(e)
        print('Error: Not able to delete the overlapping rows for %s in '
              'the %s table.' % (tsid, table))
        return 'failure'
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in delete_sql_table_rows. '
              'Make sure the database address/name are correct.')
        return 'failure'
    except Exception as e:
        print('Error: Unknown issue when trying to delete overlapping rows for'
              '%s in the %s table.' % (tsid, table))
        print(e)
        return 'failure'


def df_to_sql(df, db_location, sql_table, exists, item, verbose=False):
    """ Save a DataFrame to a specified SQL database table.

    :param df: Pandas DataFrame with values to insert into the SQL database.
    :param db_location: String of the directory location for the SQL database.
    :param sql_table: String indicating which table the DataFrame should be
        put into.
    :param exists: String indicating how the DataFrame values should interact
        with the existing values in the table. Valid parameters include
        'append' [new rows] and 'replace' [all existing table rows].
    :param item: String representing the item being inserted (i.e. the tsid)
    :param verbose: Boolean indicating whether debugging statements should print
    """

    if verbose:
        print('Entering the data for %s into the SQL database.' % (item,))

    conn = sqlite3.connect(db_location)

    # Try and except block writes the new data to the SQL Database.
    try:
        # if_exists options: append new df rows, replace all table values
        df.to_sql(sql_table, conn, if_exists=exists, index=False)
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("PRAGMA busy_timeout = 60000")
        if verbose:
            print('Successfully entered the values into the SQL Database')
    except conn.Error:
        conn.rollback()
        print("Failed to insert the DataFrame into the database for %s" %
              (item,))
    except conn.OperationalError:
        raise ValueError('Unable to connect to the SQL Database in df_to_sql. '
                         'Make sure the database address/name are correct.')
    except Exception as e:
        print('Error: Unknown issue when adding DF to SQL for %s' % (item,))
        print(e)


def query_all_active_tsids(db_location, table, period=None):
    """ Get a list of all tickers that have data.

    :param db_location: String of the database directory location
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

    try:
        conn = sqlite3.connect(db_location)
        with conn:
            cur = conn.cursor()

            if period:
                beg_date = datetime.today() - timedelta(days=period)
                query = ("""SELECT tsid
                            FROM %s
                            WHERE date>'%s'
                            GROUP BY tsid""" % (table, beg_date))
            else:
                # Option 1:
                # query = ("""SELECT source_id
                #             FROM symbology
                #             WHERE source='tsid' AND type='stock'""")

                # Option 2:
                query = ("""SELECT tsid
                            FROM %s
                            GROUP BY tsid""" % (table,))

            cur.execute(query)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid'])
                df.drop_duplicates(inplace=True)
                return df
            else:
                raise TypeError('Not able to query any tsid codes in '
                                'query_all_codes')
    except sqlite3.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the database '
                        'in query_all_codes')


def query_all_tsid_prices(db_location, table, tsid):
    """ Query all relevant interval data for this ticker from the relevant
    sources. Then process the price DataFrame.

    :param db_location: String of the database directory location
    :param table: String of the database table to query from
    :param tsid: String of the tsid whose prices should be queried
    """

    try:
        conn = sqlite3.connect(db_location)
        with conn:
            cur = conn.cursor()
            query = ("""SELECT data_vendor_id, date, open, high, low, close,
                        volume
                        FROM %s
                        WHERE tsid='%s'""" % (table, tsid))
            cur.execute(query)
            data = cur.fetchall()
            if data:
                columns = ['data_vendor_id', 'date', 'open', 'high', 'low',
                           'close', 'volume']
                df = pd.DataFrame(data, columns=columns)

                # Convert the ISO date to a datetime object
                df['date'] = pd.to_datetime(df['date'])
                # df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

                # Drop duplicate rows based on only the tsid and date columns
                df.drop_duplicates(subset=['data_vendor_id', 'date'],
                                   inplace=True)

                # Move and set the date and data_vendor_id columns to the index
                df.set_index(['date', 'data_vendor_id'], inplace=True)

                # Have to rename the indices as set_index removes the names
                df.index.name = ['date', 'data_vendor_id']

                df.sortlevel(inplace=True)
                return df
            else:
                raise TypeError('Not able to query any prices for %s in '
                                'query_all_tsid_prices' % tsid)
    except sqlite3.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the database '
                        'in query_all_tsid_prices')


def query_codes(db_location, download_selection):
    """ Builds a DataFrame of tsid codes from a SQL query. These codes are the
    items that will have their data downloaded.

    With more databases, it may be necessary to have the user
    write custom queries if they only want certain items downloaded.
    Perhaps the best way will be to have some predefined queries, and if
    those don't work for the user, they write a custom query.

    :param db_location: String of the database directory
    :param download_selection: String that specifies which data is required
    :return: DataFrame with the the specified tsid values
    """

    try:
        conn = sqlite3.connect(db_location)
        with conn:
            cur = conn.cursor()

            # ToDo: Will need to create queries for additional items

            if download_selection == 'all':
                # Retrieve all tsid stock tickers
                cur.execute("""SELECT source_id
                               FROM symbology
                               WHERE source='tsid' AND type='stock'""")
            elif download_selection == 'us_main':
                # Retrieve tsid tickers that trade only on main US exchanges
                #   and that have been active within the prior two years.
                beg_date = (datetime.utcnow() - timedelta(days=730))
                cur.execute("""SELECT source_id
                               FROM symbology
                               WHERE source='tsid'
                               AND symbol_id IN (
                                   SELECT CsiNumber
                                   FROM csidata_stock_factsheet
                                   WHERE EndDate > ?
                                   AND (Exchange IN ('AMEX', 'NYSE')
                                   OR ChildExchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA')))
                               AND type='stock'
                               GROUP BY source_id""",
                            (beg_date.isoformat(),))
            elif download_selection == 'us_main_no_end_date':
                # Retrieve tsid tickers that trade only on main US exchanges
                cur.execute("""SELECT source_id
                               FROM symbology
                               WHERE source='tsid'
                               AND symbol_id IN (
                                   SELECT CsiNumber
                                   FROM csidata_stock_factsheet
                                   WHERE (Exchange IN ('AMEX', 'NYSE')
                                   OR ChildExchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA')))
                               AND type='stock'
                               GROUP BY source_id""")
            elif download_selection == 'us_canada_london':
                # Retrieve tsid tickers that trade on AMEX, LSE, MSE, NYSE,
                #   NASDAQ, TSX, VSE and PINK exchanges, and that have been
                #   active within the prior two years.
                beg_date = (datetime.utcnow() - timedelta(days=730))
                cur.execute("""SELECT source_id
                               FROM symbology
                               WHERE source='tsid'
                               AND symbol_id IN (
                                   SELECT CsiNumber
                                   FROM csidata_stock_factsheet
                                   WHERE EndDate > ?
                                   AND (Exchange IN ('AMEX', 'LSE', 'NYSE',
                                   'TSX', 'VSE')
                                   OR ChildExchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA',
                                       'OTC Markets Pink Sheets')))
                               AND type='stock'
                               GROUP BY source_id""",
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
                return df
            else:
                raise TypeError('Not able to determine the tsid from '
                                'the SQL query in query_codes.')
    except sqlite3.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the database '
                        'in query_codes')


def query_last_price(db_location, table, vendor_id):
    """ Queries the pricing database to find the latest dates for each item
    in the database, regardless of whether it is in the tsid list.

    :param db_location: String of the database directory location
    :param table: String of the table whose prices should be worked on
    :param vendor_id: Integer or list of integers representing the vendor id
        whose prices should be considered
    :return: Returns a DataFrame with the tsid and the date of the latest
        data point for all tickers in the database.
    """

    if type(vendor_id) == list:
        vendor_id = ', '.join(["'" + str(vendor) + "'" for vendor in vendor_id])
    elif type(vendor_id) == int:
        vendor_id = "'" + str(vendor_id) + "'"
    else:
        # Should never occur
        raise TypeError('%s is an invalid type provided for the vendor_id '
                        'variable in query_last_price.' % type(vendor_id))

    try:
        conn = sqlite3.connect(db_location)
        with conn:
            query = """SELECT tsid, MAX(date) as date, updated_date
                        FROM %s
                        WHERE data_vendor_id IN (%s)
                        GROUP BY tsid""" % (table, vendor_id)
            df = pd.read_sql(query, conn, index_col='tsid')
            if len(df.index) == 0:
                return df

            # Convert the ISO dates to datetime objects
            df['date'] = pd.to_datetime(df['date'])
            df['updated_date'] = pd.to_datetime(df['updated_date'])
            # df.to_csv('query_last_price.csv')
            return df
    except sqlite3.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the database '
                        'in query_last_price.')


def query_q_codes(db_location, download_selection):
    """ Builds a list of Quandl Codes from a SQL query. These codes are the
    items that will have their data downloaded.

    With more databases, it may be necessary to have the user
    write custom queries if they only want certain items downloaded.
    Perhaps the best way will be to have some predefined queries, and if
    those don't work for the user, they write a custom query.

    :param db_location: String of the database directory location
    :param download_selection: String that specifies which data is required
    :return: DataFrame with two columns (tsid, q_code)
    """

    try:
        conn = sqlite3.connect(db_location)
        with conn:
            cur = conn.cursor()

            # ToDo: Will need to create queries for additional items

            if download_selection == 'wiki':
                cur.execute("""SELECT tsid.source_id, wiki.source_id
                               FROM symbology tsid
                               INNER JOIN symbology wiki
                               ON tsid.symbol_id = wiki.symbol_id
                               WHERE tsid.source='tsid'
                               AND wiki.source='quandl_wiki'
                               GROUP BY wiki.source_id""")
            elif download_selection == 'goog':
                cur.execute("""SELECT tsid.source_id, wiki.source_id
                               FROM symbology tsid
                               INNER JOIN symbology wiki
                               ON tsid.symbol_id = wiki.symbol_id
                               WHERE tsid.source='tsid'
                               AND wiki.source='quandl_goog'
                               GROUP BY wiki.source_id""")
            elif download_selection == 'goog_us_main':
                # Retrieve tsid tickers that trade only on main US exchanges
                #   and that have been active within the prior two years.
                beg_date = (datetime.utcnow() - timedelta(days=730))
                cur.execute("""SELECT tsid.source_id, wiki.source_id
                               FROM symbology tsid
                               INNER JOIN symbology wiki
                               ON tsid.symbol_id = wiki.symbol_id
                               WHERE tsid.source='tsid'
                               AND wiki.source='quandl_goog'
                               AND wiki.symbol_id IN (
                                   SELECT CsiNumber
                                   FROM csidata_stock_factsheet
                                   WHERE EndDate > ?
                                   AND (Exchange IN ('AMEX', 'NYSE')
                                   OR ChildExchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA')))
                               GROUP BY wiki.source_id""",
                            (beg_date.isoformat(),))
            elif download_selection == 'goog_us_main_no_end_date':
                # Retrieve tsid tickers that trade only on main US exchanges
                cur.execute("""SELECT tsid.source_id, wiki.source_id
                               FROM symbology tsid
                               INNER JOIN symbology wiki
                               ON tsid.symbol_id = wiki.symbol_id
                               WHERE tsid.source='tsid'
                               AND wiki.source='quandl_goog'
                               AND wiki.symbol_id IN (
                                   SELECT CsiNumber
                                   FROM csidata_stock_factsheet
                                   WHERE (Exchange IN ('AMEX', 'NYSE')
                                   OR ChildExchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA')))
                               GROUP BY wiki.source_id""")
            elif download_selection == 'goog_us_canada_london':
                # Retrieve tsid tickers that trade on AMEX, LSE, MSE, NYSE,
                #   NASDAQ, TSX, VSE and PINK exchanges, and that have been
                #   active within the prior two years.
                beg_date = (datetime.utcnow() - timedelta(days=730))
                cur.execute("""SELECT tsid.source_id, wiki.source_id
                                   FROM symbology tsid
                                   INNER JOIN symbology wiki
                                   ON tsid.symbol_id = wiki.symbol_id
                                   WHERE tsid.source='tsid'
                                   AND wiki.source='quandl_goog'
                                   AND wiki.symbol_id IN (
                                       SELECT CsiNumber
                                       FROM csidata_stock_factsheet
                                       WHERE EndDate > ?
                                       AND (Exchange IN ('AMEX', 'LSE', 'NYSE',
                                       'TSX', 'VSE')
                                       OR ChildExchange IN ('AMEX',
                                           'BATS Global Markets',
                                           'Nasdaq Capital Market',
                                           'Nasdaq Global Market',
                                           'Nasdaq Global Select',
                                           'NYSE', 'NYSE ARCA',
                                           'OTC Markets Pink Sheets')))
                                   GROUP BY wiki.source_id""",
                            (beg_date.isoformat(),))
            elif download_selection == 'goog_etf':
                cur.execute("""SELECT tsid.source_id, wiki.source_id
                               FROM symbology tsid
                               INNER JOIN symbology wiki
                               ON tsid.symbol_id = wiki.symbol_id
                               WHERE tsid.source='tsid'
                               AND wiki.source='quandl_goog'
                               AND wiki.symbol_id IN (
                                   SELECT CsiNumber
                                   FROM csidata_stock_factsheet
                                   WHERE Type='Exchange-Traded Fund')
                               GROUP BY wiki.source_id
                               """)
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
                return df
            else:
                raise SystemError('Not able to determine the q_codes '
                                  'from the SQL query in query_q_codes')
    except sqlite3.Error as e:
        print(e)
        raise SystemError('Error when trying to connect to the database '
                          'in query_q_codes')


def query_source_weights(db_location):
    """ Create a DataFrame of the source weights.

    :param db_location: String of the database directory location
    :return: DataFrame of all data sources
    """

    try:
        conn = sqlite3.connect(db_location)
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
                return df
            else:
                raise TypeError('Unable to query data vendor weights within '
                                'query_source_weights')
    except sqlite3.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the database '
                        'in query_source_weights')


def retrieve_data_vendor_id(db_location, name):
    """ Takes the name provided and tries to find data vendor(s) from the
    data_vendor table in the database. If nothing is returned in the
    query, then 'Unknown' is used.

    :param db_location: String of the database directory location
    :param name: String that has the database name, or a special SQL string
        to retrieve extra ids (i.e. 'Quandl_%' to retrieve all Quandl ids)
    :return: If one vendor id is queried, return a int of the data vendor's id.
        If multiple ids are queried, return a list of all the ids. Otherwise,
        return 'Unknown'.
    """

    try:
        conn = sqlite3.connect(db_location)
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
            return data
    except sqlite3.Error as e:
        print('Error when trying to retrieve data from database in '
              'retrieve_data_vendor_id')
        print(e)
