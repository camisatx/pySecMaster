import os
import pandas as pd
import sqlite3
import time

from pySecMaster import maintenance

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.3.0'

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


def query_existing_qcodes(db_location, table, verbose=False):

    start_time = time.time()
    if verbose:
        print('Retrieving the q_codes from %s...' % db_location)

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            query = """SELECT q_code
                    FROM %s
                    GROUP BY q_code""" % table
            cur.execute(query)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['q_code'])
                if verbose:
                    print('The query of the existing q_codes for %s took %0.1f '
                          'seconds.' % (table, time.time() - start_time))
                return df
            else:
                raise SystemError('Not able to determine the q_codes from the '
                                  'SQL query in query_existing_qcodes')
    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to query the q_codes from %s within '
              'query_existing_qcodes' % table)
        print(e)
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in '
              'query_existing_qcodes. Make sure the database '
              'address/name are correct.')
    except Exception as e:
        print(e)
        print('Error: Unknown issue occurred in query_existing_qcodes')


def query_qcode_data(db_location, table, qcode, verbose=False):

    start_time = time.time()
    if verbose:
        print('Retrieving all the %s data for %s...' % (table, qcode))

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            # Group by date to remove any duplicate values
            query = """SELECT *
                    FROM %s
                    WHERE q_code='%s'
                    GROUP BY date""" % (table, qcode)
            cur.execute(query)
            data = cur.fetchall()
            if data:
                daily_prices_col = ['daily_price_id', 'data_vendor_id',
                                    'q_code', 'date', 'open', 'high', 'low',
                                    'close', 'volume', 'ex_dividend',
                                    'split_ratio', 'adj_open', 'adj_high',
                                    'adj_low', 'adj_close', 'adj_volume',
                                    'updated_date']
                minute_prices_col = ['minute_price_id', 'data_vendor_id',
                                     'q_code', 'date', 'close', 'high', 'low',
                                     'open', 'volume', 'updated_date']
                if table == 'daily_prices':
                    df = pd.DataFrame(data, columns=daily_prices_col)
                elif table == 'minute_prices':
                    df = pd.DataFrame(data, columns=minute_prices_col)
                else:
                    raise SystemError('Incorrect table type provided to '
                                      'query_qcode_data. Valid table types '
                                      'include daily_prices and minute_prices')
                if verbose:
                    print('The query of the %s q_code data for %s took %0.2f '
                          'seconds.' % (table, qcode, time.time() - start_time))
                return df
            else:
                raise SystemError('Not able to determine the q_codes from the '
                                  'SQL query in query_qcode_data')
    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to query the price data from %s within query_qcode_data' %
              table)
        print(e)
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in query_qcode_data. Make '
              'sure the database address/name are correct.')
    except Exception as e:
        print(e)
        print('Error: Unknown issue occurred in query_qcode_data')


def query_symbology(db_location):

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT tsid.source_id, goog.source_id
                        FROM symbology tsid
                        INNER JOIN symbology goog
                        ON tsid.symbol_id = goog.symbol_id
                        WHERE tsid.source='tsid'
                        AND goog.source='quandl_goog'
                        GROUP BY goog.source_id""")
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid', 'goog'])
                return df
            else:
                raise SystemError('Not able to determine the quandl_goog codes '
                                  'from the SQL query in query_symbology')
    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to insert the data into the earnings table within '
              'query_symbology')
        print(e)
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in query_symbology. Make '
              'sure the database address/name are correct.')
    except Exception as e:
        print(e)
        print('Error: Unknown issue occurred in query_symbology')


def convert_qcode_to_tsid(db_location, price_df, table, qcode):

    # Remove the price_id and the q_code columns
    if table == 'daily_prices':
        price_df.drop('daily_price_id', axis=1, inplace=True)
    elif table == 'minute_prices':
        price_df.drop('minute_price_id', axis=1, inplace=True)
    price_df.drop('q_code', axis=1, inplace=True)

    # Translate the q_code to a tsid
    sym_codes = query_symbology(db_location=db_location)
    tsid = sym_codes.loc[sym_codes['goog'] == qcode, 'tsid'].values
    if tsid:
        tsid = tsid[0]
    else:
        tsid = None
        print('Unable to find a tsid for %s' % qcode)

    # Add a tsid column with the appropriate tsid value
    price_df.insert(0, 'tsid', tsid)

    return price_df


def df_to_sql(db_location, df, sql_table, exists, item, verbose=False):

    if verbose:
        print('Entering the data for %s into %s.' % (item, sql_table))

    conn = sqlite3.connect(db_location)
    # Try and except block writes the new data to the SQL Database.
    try:
        # if_exists options: append new df rows, replace all table values
        df.to_sql(sql_table, conn, if_exists=exists, index=False)
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("PRAGMA busy_timeout = 60000")
        if verbose:
            print('Successfully entered %s into %s' % (item, sql_table))
    except conn.Error:
        conn.rollback()
        print("Failed to insert the DataFrame into %s for %s" %
              (sql_table, item))
    except conn.OperationalError:
        raise ValueError('Unable to connect to the SQL Database in df_to_sql. '
                         'Make sure the database address/name are correct.')
    except Exception as e:
        print('Error: Unknown issue when adding the DataFrame for %s to %s' %
              (item, sql_table))
        print(e)


def delete_sql_table_rows(db_location, query, table, tsid):

    # print('Deleting all rows in %s that fit the provided criteria' % (table,))
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


def insert_df_to_db(db_location, price_df, table, verbose=False):

    # Information about the new data
    tsid = price_df.loc[0, 'tsid']
    max_date = price_df['date'].max()
    min_date = price_df['date'].min()

    # Check if the database table already has data for this ticker
    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            query = ("""SELECT tsid, MAX(date), MIN(date)
                        FROM %s
                        WHERE tsid='%s'""" % (table, tsid))
            cur.execute(query)
            data = cur.fetchall()
            existing_df = pd.DataFrame(data, columns=['tsid', 'max', 'min'])
    except sqlite3.Error as e:
        conn.rollback()
        raise SystemError('Failed to query the existing data from %s within '
                          'insert_df_to_db because of %s' % (table, e))
    except conn.OperationalError:
        raise SystemError('Unable to connect to the SQL Database in '
                          'insert_df_to_db. Make sure the database '
                          'address/name are correct.')
    except Exception as e:
        raise SystemError('Error occurred in insert_df_to_db: %s' % e)

    # If there is existing data and the new data's date range is more extensive
    #   than the stored data, delete the old data and add the new data
    if existing_df.loc[0, 'tsid']:
        if (max_date > existing_df.loc[0, 'max'] and
                min_date <= existing_df.loc[0, 'min']):
            if verbose:
                print('Replacing the %s values because it had more data than '
                      'the currently stored data.' % tsid)

            # Delete the existing data for this tsid
            query = ("""DELETE FROM %s
                        WHERE tsid='%s'""" % (table, tsid))
            del_success = delete_sql_table_rows(db_location=db_location,
                                                query=query, table=table,
                                                tsid=tsid)
            if del_success == 'success':
                # Delete was successful, so insert the new data into the table
                df_to_sql(df=price_df, db_location=db_location, sql_table=table,
                          exists='append', item=tsid, verbose=False)
            elif del_success == 'failure':
                # delete_sql_table_rows will issue a failure notice
                pass
        else:
            if verbose:
                print('Not inserting data for %s because duplicate data was '
                      'found in the database' % tsid)
    else:
        # There is not existing data for this ticker, so insert the data
        df_to_sql(df=price_df, db_location=db_location, sql_table=table,
                  exists='append', item=tsid, verbose=False)


def main(verbose=False):

    old_db_location = 'C:/Users/Josh/Desktop/pySecMaster_m old.db'
    new_db_location = 'C:/Users/Josh/Desktop/pySecMaster_m.db'
    table = 'minute_prices'    # daily_prices, minute_prices

    # Create a new database where the old prices will be copied to
    symbology_sources = ['csi_data', 'tsid', 'quandl_wiki', 'quandl_goog',
                         'seeking_alpha', 'yahoo']

    os.chdir('..')  # Need to move up a folder in order to access load_tables
    maintenance(database_link=new_db_location, quandl_ticker_source='csidata',
                database_list=['WIKI'], threads=8, quandl_key='',
                quandl_update_range=30, csidata_update_range=5,
                symbology_sources=symbology_sources)

    # Retrieve a list of all the tickers from the existing database table
    qcodes_df = query_existing_qcodes(db_location=old_db_location,
                                      table=table, verbose=True)

    for index, row in qcodes_df.iterrows():
        ticker = row['q_code']
        copy_start = time.time()

        # Retrieve all price data for this ticker
        raw_price_df = query_qcode_data(db_location=old_db_location,
                                        table=table, qcode=ticker,
                                        verbose=False)

        # Change the q_code column to a tsid column
        clean_price_df = convert_qcode_to_tsid(db_location=new_db_location,
                                               price_df=raw_price_df,
                                               table=table, qcode=ticker)

        tsid = clean_price_df.loc[0, 'tsid']
        # If there is no tsid, don't attempt to insert the data to the database
        if tsid:
            # Add the data to the database if there is not existing data
            insert_df_to_db(db_location=new_db_location,
                            price_df=clean_price_df, table=table, verbose=True)
            if verbose:
                print('Moving the %s from %s to %s took %0.2f seconds' %
                      (table, ticker, tsid, time.time() - copy_start))


if __name__ == '__main__':

    main(verbose=True)
