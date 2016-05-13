import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sqlite3
import time

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


def query_all_tsids_from_table(database, table):
    """ Get a list of all unique tsids from the specified table.

    :param database: String of the database name and directory
    :param table: String of the table that should be queried from
    :return: List of all of the unique tsids for the specified table
    """

    conn = sqlite3.connect(database)

    try:
        with conn:
            cur = conn.cursor()
            query = ("""SELECT tsid
                     FROM %s
                     GROUP BY tsid""" % (table,))

            cur.execute(query)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid'])
                df.drop_duplicates(inplace=True)

                tsid_unique_list = pd.unique((df['tsid']).values)
                return tsid_unique_list
            else:
                raise TypeError('Not able to query any tsid codes in '
                                'query_all_tsids_from_table')
    except sqlite3.Error as e:
        print(e)
        raise TypeError('Error when trying to connect to the %s database '
                        'in query_all_tsids_from_table' % database)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_all_tsids_from_table. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_all_tsids_from_table')


def query_all_tsid_prices(database, table, tsid):
    """ Query all of the data for this ticker from the sqlite database.

    :param database: String of the database name
    :param table: String of the table that should be queried from
    :param tsid: String of which tsid to check
    :return: Datetime object representing the start date
    """

    conn = sqlite3.connect(database)
    try:
        with conn:
            cur = conn.cursor()
            query = """SELECT data_vendor_id, tsid, date, close, high, low,
                        open, volume, updated_date
                    FROM %s
                    WHERE tsid='%s'""" % (table, tsid)
            cur.execute(query)
            data = cur.fetchall()
            if data:
                columns = ['data_vendor_id', 'tsid', 'date', 'close', 'high',
                           'low', 'open', 'volume', 'updated_date']
                df = pd.DataFrame(data, columns=columns)
                return df
            else:
                return pd.DataFrame()
    except sqlite3.Error as e:
        print(e)
        raise SystemError('Failed to query the %s data within '
                          'query_all_tsid_prices' % tsid)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_all_tsid_prices. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in '
                          'query_all_tsid_prices')


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
        return 'success'
    except psycopg2.Error as e:
        conn.rollback()
        print(e)
        print('Error: Not able to delete the overlapping rows for %s in '
              'the %s table.' % (item, table))
        return 'failure'
    except conn.OperationalError:
        print('Unable to connect to the %s database in delete_sql_table_rows. '
              'Make sure the database address/name are correct.' % database)
        return 'failure'
    except Exception as e:
        print('Error: Unknown issue when trying to delete overlapping rows for'
              '%s in the %s table.' % (item, table))
        print(e)
        return 'failure'


def df_to_sql(database, user, password, host, port, df, sql_table, exists,
              item, verbose=False):
    """ Save a DataFrame to a specified PostgreSQL database table.

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


def insert_df_to_db(database, user, password, host, port, price_df, table,
                    verbose=False):
    """

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param price_df: DataFrame of the tsid values
    :param table: String of the table to insert the DataFrame into
    :param verbose: Boolean of whether to print debugging statements
    """

    # Information about the new data
    tsid = price_df.loc[0, 'source_id']
    max_date = price_df['date'].max()
    min_date = price_df['date'].min()

    # Check if the postgre database table already has data for this ticker
    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)
    try:
        with conn:
            cur = conn.cursor()
            query = ("""SELECT source_id AS tsid, MAX(date), MIN(date)
                        FROM %s
                        WHERE source_id='%s'
                        GROUP BY source_id""" % (table, tsid))
            cur.execute(query)
            data = cur.fetchall()
            existing_df = pd.DataFrame(data, columns=['tsid', 'max', 'min'])
            existing_df['max'] = pd.to_datetime(existing_df['max'], utc=True)
            existing_df['min'] = pd.to_datetime(existing_df['min'], utc=True)
    except psycopg2.Error as e:
        conn.rollback()
        raise SystemError('Failed to query the existing data from %s within '
                          'insert_df_to_db because of %s' % (table, e))
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'insert_df_to_db. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        raise SystemError('Error occurred in insert_df_to_db: %s' % e)

    # If there is existing data and the new data's date range is more extensive
    #   than the stored data, delete the old data and add the new data
    # if existing_df.loc[0, 'tsid']:
    if len(existing_df) > 0:
        if (max_date > existing_df.loc[0, 'max'] and
                min_date <= existing_df.loc[0, 'min']):
            if verbose:
                print('Replacing the %s values because the new data had more '
                      'values than the currently stored data.' % tsid)

            # Delete the existing data for this tsid
            query = ("""DELETE FROM %s
                        WHERE source_id='%s'""" % (table, tsid))
            del_success = delete_sql_table_rows(database=database, user=user,
                                                password=password, host=host,
                                                port=port, query=query,
                                                table=table, item=tsid)
            if del_success == 'success':
                # Delete was successful, so insert the new data into the table
                df_to_sql(database=database, user=user, password=password,
                          host=host, port=port, df=price_df, sql_table=table,
                          exists='append', item=tsid)
            elif del_success == 'failure':
                # delete_sql_table_rows will issue a failure notice
                pass
        else:
            if verbose:
                print('Not inserting data for %s because duplicate data was '
                      'found in the %s database' % (tsid, database))
    else:
        # There is no existing data for this ticker, so insert the data
        df_to_sql(database=database, user=user, password=password, host=host,
                  port=port, df=price_df, sql_table=table, exists='append',
                  item=tsid)


def main(verbose=False):
    """ Move all values from the minute_prices table of the sqlite_database
    to the postgre database.

    :param verbose: Boolean of whether to print debugging statements
    """

    sqlite_database = 'C:/Users/joshs/Programming/Databases/pySecMaster/' \
                      'pySecMaster_m.db'

    from utilities.user_dir import user_dir
    userdir = user_dir()
    postgre_database = userdir['postgresql']['pysecmaster_db']
    postgre_user = userdir['postgresql']['pysecmaster_user']
    postgre_password = userdir['postgresql']['pysecmaster_password']
    postgre_host = userdir['postgresql']['pysecmaster_host']
    postgre_port = userdir['postgresql']['pysecmaster_port']

    table = 'minute_prices'

    # Get a list of unique tsids from the sqlite database's table
    tsid_list = query_all_tsids_from_table(database=sqlite_database,
                                           table=table)

    # Iterate through each tsid from the unique list
    for tsid in tsid_list:

        tsid_start = time.time()

        # Query all of the tsid's table prices from the sqlite database
        existing_price_df = query_all_tsid_prices(database=sqlite_database,
                                                  table=table, tsid=tsid)

        if len(existing_price_df) > 0:

            # Add the source column using 'tsid' between data_vendor_id and tsid
            existing_price_df.insert(1, 'source', 'tsid')

            # Change the tsid column name to 'source_id'
            existing_price_df.rename(columns={'tsid': 'source_id'},
                                     inplace=True)

            # existing_price_df['date'].tz_localize('UTC')
            existing_price_df['date'] = \
                pd.to_datetime(existing_price_df['date'], utc=True)

            # print(existing_price_df.head())
            # existing_price_df.to_csv('%s_min_prices.csv' % tsid)

            # Insert all the prices into the postgre database
            insert_df_to_db(database=postgre_database, user=postgre_user,
                            password=postgre_password, host=postgre_host,
                            port=postgre_port, price_df=existing_price_df,
                            table=table, verbose=verbose)
        else:
            # Should never happen, since the tsid wouldn't have been used anyway
            if verbose:
                print('No existing data found in the Sqlite3 database for %s' %
                      tsid)

        if verbose:
            print('Verifying the %s times for %s took %0.2f seconds' %
                  (table, tsid, time.time() - tsid_start))


if __name__ == '__main__':

    main(verbose=True)
