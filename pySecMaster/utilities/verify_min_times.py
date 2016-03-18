from datetime import datetime, timedelta
from dateutil import tz
import time

import os
import pandas as pd
import sqlite3

from pySecMaster import maintenance


__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.3.1'

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


def query_existing_tsids(db_location, table, verbose=False):

    start_time = time.time()
    if verbose:
        print('Retrieving the tsids from %s...' % db_location)

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            query = """SELECT tsid
                    FROM %s
                    GROUP BY tsid""" % table
            cur.execute(query)
            data = cur.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['tsid'])
                if verbose:
                    print('The query of the existing tsids for %s took %0.1f '
                          'seconds.' % (table, time.time() - start_time))
                return df
            else:
                raise SystemError('Not able to determine the tsids from the '
                                  'SQL query in query_existing_tsids')
    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to query the tsids from %s within '
              'query_existing_tsids' % table)
        print(e)
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in '
              'query_existing_tsids. Make sure the database '
              'address/name are correct.')
    except Exception as e:
        print(e)
        print('Error: Unknown issue occurred in query_existing_tsids')


def query_tsid_data(db_location, table, tsid, verbose=False):

    start_time = time.time()
    if verbose:
        print('Retrieving all the %s data for %s...' % (table, tsid))

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            query = """SELECT *
                    FROM %s
                    WHERE tsid='%s'""" % (table, tsid)
            cur.execute(query)
            data = cur.fetchall()
            if data:
                minute_prices_col = ['minute_price_id', 'data_vendor_id',
                                     'tsid', 'date', 'close', 'high', 'low',
                                     'open', 'volume', 'updated_date']
                if table == 'minute_prices':
                    df = pd.DataFrame(data, columns=minute_prices_col)
                else:
                    raise SystemError('Incorrect table type provided to '
                                      'query_tsid_data. Valid table type '
                                      'include minute_prices.')
                if verbose:
                    print('The query of the %s tsid data for %s took %0.2f '
                          'seconds.' % (table, tsid, time.time() - start_time))
                return df
            else:
                raise SystemError('Not able to determine the tsid from the '
                                  'SQL query in query_tsid_data')
    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to query the price data from %s within query_tsid_data' %
              table)
        print(e)
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in query_tsid_data. Make '
              'sure the database address/name are correct.')
    except Exception as e:
        print(e)
        print('Error: Unknown issue occurred in query_tsid_data')


def verify_minute_time(price_df, tsid):
    """
    Determine if each period's time is correct. First this calculates the
    correct start and end time for each day. These times are compared to the
    existing times, and if there is any discrepancy, the difference is made
    to the time.

    :param price_df: DataFrame of the original times
    :param tsid: String of the current tsid
    :return: DataFrame with adjusted times
    """

    def split_day(row, column):
        date = row[column]
        return date[:10]

    nyc_tz = tz.gettz('America/New_York')
    utc_tz = tz.gettz('UTC')

    minute_prices_col = ['data_vendor_id', 'tsid', 'date', 'close', 'high',
                         'low', 'open', 'volume', 'updated_date']

    price_df['day'] = price_df.apply(split_day, axis=1, args=('date',))
    price_df['date_obj'] = pd.to_datetime(price_df['date'])

    # Build a unique list of all the days (YYYY-MM-DD)
    unique_days = pd.unique(price_df['day'])

    # Calculate each day's time adjustments to use (in seconds)
    day_adjustments = {}
    for day in unique_days:
        # Retrieve the existing start and end times for this day
        day_price_df = price_df.loc[price_df['day'] == day]
        start_time = day_price_df.loc[day_price_df['date_obj'].idxmin(),
                                      'date_obj']
        start_time_utc = start_time.replace(tzinfo=utc_tz)
        end_time = day_price_df.loc[day_price_df['date_obj'].idxmax(),
                                    'date_obj']
        end_time_utc = end_time.replace(tzinfo=utc_tz)

        # Calculate the actual start and end times for this day
        working_day = datetime.strptime(day, '%Y-%m-%d')

        # Market opens at 9:30AM EST
        actual_start_time = working_day.replace(hour=9, minute=30, tzinfo=nyc_tz)
        actual_start_time_utc = actual_start_time.astimezone(tz=utc_tz)

        if day in ['2015-11-27', '2015-12-24']:
            # Market closes at 1PM EST
            actual_end_time = working_day.replace(hour=13, tzinfo=nyc_tz)
        else:
            # Market closes at 4PM EST
            actual_end_time = working_day.replace(hour=16, tzinfo=nyc_tz)
        actual_end_time_utc = actual_end_time.astimezone(tz=utc_tz)

        start_time_delta = actual_start_time_utc - start_time_utc
        end_time_delta = actual_end_time_utc - end_time_utc

        start_end_delta = abs(end_time_delta) - abs(start_time_delta)

        # The end time delta is viewed as higher quality, as the first time
        #   period could be missing (which would mess up the time adjustment).
        if abs(start_end_delta.total_seconds()) < (15*60):
            # Normal: start and end time delta are within 15 min of each other.
            #   Adjust all times by the end time delta.
            time_delta = end_time_delta.total_seconds()
        elif abs(end_time_delta.total_seconds()) < (15*60):
            # If the end time delta is within 15 min of the actual, us it's
            #   delta to adjust all the times. Indicates the start time is
            #   missing
            time_delta = end_time_delta.total_seconds()
        elif abs(start_time_delta.total_seconds()) < (15*60):
            # Shouldn't ever happen. The end time delta is greater than 15 min
            #   from the actual (missing), so if the start time is within 15
            #   min from the actual start, use it's delta to adjust all times.
            time_delta = start_time_delta.total_seconds()
        else:
            # Shouldn't ever happen. Both the start and end time delta are
            #   greater than 15 min from the actual times. No way to determine
            #   what to adjust by, so just adjust to using the end time delta.
            time_delta = end_time_delta.total_seconds()
            print("Don't know what time delta to use for %s on %s. Using the "
                  "end delta." % (tsid, day))

        day_adjustments[day] = int(time_delta)

    updated_times = []
    # Add the calculated time adjustments to the existing time
    for index, row in price_df.iterrows():

        # Check if the this time is the first of the day
        prior_date = price_df.iloc[(index - 1), price_df.columns.get_loc('day')]
        if prior_date != row['day']:
            # This is the first time for this day
            try:
                next_time = price_df.iloc[(index + 1),
                                          price_df.columns.get_loc('date_obj')]
            except IndexError:
                pass
            finally:
                # -60 indicates the next period is 1 min away (normal).
                # +3480, +3300 indicates the the next period is behind this time
                #   by roughly an hour.
                next_time_delta = row['date_obj'] - next_time

                if int(next_time_delta.total_seconds()) >= (10*60):
                    # This time period was not effected by the unix bug, but
                    #   needs to be moved back by the next time period delta
                    #   (plus 1 min) so it will be aligned with the other times
                    #   so that when the adjustment occurs below, all times
                    #   will be aligned.
                    price_df.loc[index, 'date_obj'] -= \
                        timedelta(seconds=next_time_delta.total_seconds() + 60)

        day_time_delta = day_adjustments[row['day']]
        cur_time = {}
        new_date = (price_df.loc[index, 'date_obj'] +
                    timedelta(seconds=day_time_delta))
        cur_time['minute_price_id'] = row['minute_price_id']
        cur_time['date'] = new_date.isoformat()
        updated_times.append(cur_time)

    updated_time_df = pd.DataFrame(updated_times,
                                   columns=['minute_price_id', 'date'])

    price_df.drop('date', axis=1, inplace=True)
    price_df.drop('day', axis=1, inplace=True)
    price_df.drop('date_obj', axis=1, inplace=True)
    price_df.drop('updated_date', axis=1, inplace=True)

    new_price_df = pd.merge(price_df, updated_time_df, on=['minute_price_id'])

    new_price_df.drop('minute_price_id', axis=1, inplace=True)
    new_price_df.insert(len(updated_time_df.columns), 'updated_date',
                        datetime.utcnow().isoformat())

    # Rearrange the DataFrame columns based on the minute_prices_col list
    new_price_df = new_price_df.ix[:, minute_prices_col]

    return new_price_df


def update_db_times(db_location, table, price_df):
    """
    For any periods that were changed, update the database values based on the
    minute_price_id variable.

    :param db_location: String of the database location
    :param table: String of the database table to be updating
    :param price_df: DataFrame of one symbol's times that are corrected
    """

    for index, row in price_df.iterrows():

        min_price_id = row['minute_price_id']
        date = row['date'].isoformat()
        updated_date = row['updated_date']

        if updated_date:
            # Only update the database time if the existing time was updated,
            #   which is indicated by the updated_date variable not being None

            conn = sqlite3.connect(db_location)
            try:
                with conn:
                    cur = conn.cursor()
                    cur.execute("""UPDATE %s
                                   SET date='%s', updated_date='%s'
                                   WHERE minute_price_id='%s'""" %
                                (table, date, updated_date, min_price_id))
                    conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise SystemError('Failed to update the times in %s within '
                                  'update_db_times because of %s' % (table, e))
            except conn.OperationalError:
                raise SystemError('Unable to connect to the SQL Database in '
                                  'update_db_times. Make sure the database '
                                  'address/name are correct.')
            except Exception as e:
                raise SystemError('Error occurred in update_db_times: %s' % e)


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
        # There is no existing data for this ticker, so insert the data
        df_to_sql(df=price_df, db_location=db_location, sql_table=table,
                  exists='append', item=tsid, verbose=False)


def main(verbose=False):

    old_db_location = 'C:/Users/joshs/Programming/Databases/pySecMaster/' \
                      'pySecMaster_m old.db'
    new_db_location = 'C:/Users/joshs/Programming/Databases/pySecMaster/' \
                      'pySecMaster_m new.db'
    table = 'minute_prices'

    # Create a new pySecMaster minute database
    symbology_sources = ['csi_data', 'tsid', 'quandl_wiki', 'quandl_goog',
                         'seeking_alpha', 'yahoo']
    os.chdir('..')  # Need to move up a folder in order to access load_tables
    maintenance(database_link=new_db_location, quandl_ticker_source='csidata',
                database_list=['WIKI'], threads=8, quandl_key='',
                quandl_update_range=30, csidata_update_range=5,
                symbology_sources=symbology_sources)

    # Retrieve a list of all existing tsid's
    current_tsid_df = query_existing_tsids(db_location=old_db_location,
                                           table=table, verbose=verbose)

    # Cycle through each tsid
    for index, row in current_tsid_df.iterrows():
        tsid = row['tsid']
        tsid_start = time.time()

        # Query the existing tsid's price times
        existing_price_df = query_tsid_data(db_location=old_db_location,
                                            table=table, tsid=tsid,
                                            verbose=verbose)

        # Change any incorrect times to best guess times (98% confident)
        updated_price_df = verify_minute_time(price_df=existing_price_df,
                                              tsid=tsid)

        # Update the database times with the corrected times
        insert_df_to_db(db_location=new_db_location, table=table,
                        price_df=updated_price_df, verbose=verbose)

        if verbose:
            print('Verifying the %s times for %s took %0.2f seconds' %
                  (table, tsid, time.time() - tsid_start))

if __name__ == '__main__':

    main(verbose=True)
