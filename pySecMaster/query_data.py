import pandas as pd
import re
import sqlite3
import time

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


def pull_daily_prices(query_type, beg_date, end_date, *args):

    try:
        conn = sqlite3.connect(database_location)
        with conn:
            cur = conn.cursor()
            if query_type == 'ticker':
                tsid, = args
                print('Extracting the daily prices for %s' % (tsid,))
                cur.execute(
                    """ SELECT date, tsid, open, high, low, close, volume,
                               adj_close
                        FROM daily_prices
                        WHERE tsid=? AND date>=? AND date<=?""",
                    (tsid, beg_date, end_date))
            elif query_type == 'index':
                # ToDo: Re-implement the indices table; need unique symbol_id
                index, as_of_date = args
                print('Extracting the daily prices for tickers in the %s' %
                      (index,))

                cur.execute(
                    """SELECT date, tsid, open, high, low, close, volume,
                              adj_close
                       FROM daily_prices
                       WHERE tsid IN (SELECT tsid
                                      FROM indices
                                      WHERE stock_index=?
                                      AND as_of_date=?)
                       AND date>=? AND date<=?""",
                    (index, as_of_date, beg_date, end_date))
            else:
                print('Error on query_type in pull_daily_prices. Make sure '
                      'query_type is correct.')

            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows,
                                  columns=['date', 'tsid', 'open', 'high',
                                           'low', 'close', 'volume',
                                           'adj_close'])
            else:
                raise SystemExit('No data returned from table query. Try '
                                 'adjusting the criteria for the query.')

            # The next two lines change the index of the df to be the date.
            df.index = df['date']
            df = df.drop('date', 1)

            return df

    except sqlite3.Error as e:
        print('Error when trying to retrieve price data from database in '
              'pull_daily_prices')
        print(e)


def pull_minute_prices(query_type, beg_date, end_date, *args):

    try:
        conn = sqlite3.connect(database_location)
        with conn:
            cur = conn.cursor()
            if query_type == 'ticker':
                tsid, = args
                print('Extracting the minute prices for %s' % (tsid,))
                cur.execute(
                    """ SELECT date, tsid, open, high, low, close, volume
                        FROM minute_prices
                        WHERE tsid=? AND date>=? AND date<=?""",
                    (tsid, beg_date, end_date))
            else:
                raise SystemExit('Error on query_type in pull_daily_prices. '
                                 'Make sure query_type is correct.')

            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows,
                                  columns=['date', 'tsid', 'open', 'high',
                                           'low', 'close', 'volume'])
            else:
                raise SystemExit('No data returned from table query. Try '
                                 'adjusting the criteria for the query.')

            # The next two lines change the index of the df to be the date.
            df.index = df['date']
            df = df.drop('date', 1)

            return df

    except sqlite3.Error as e:
        print(e)
        raise SystemExit('Error when trying to retrieve price data from '
                         'database in pull_daily_prices')


database_location = 'C:/Users/####/Desktop/pySecMaster_d.db'

query_type = 'ticker'     # index, ticker
tsid = 'TSLA.Q.0'
beg_date = '1950-01-01 00:00:00'
end_date = '2016-12-30 00:00:00'
frequency = 'minute'    # daily, minute

index = 'S&P 500'  # 'S&P 500', 'Russell Midcap', 'Russell 2000', 'Russell 1000'
as_of_date = '2015-01-01'

start_time = time.time()
if query_type == 'ticker':
    if frequency == 'daily':
        prices_df = pull_daily_prices(query_type, beg_date, end_date, tsid)
    elif frequency == 'minute':
        prices_df = pull_minute_prices(query_type, beg_date, end_date, tsid)
    else:
        raise SystemExit('Error on frequency in main function')

elif query_type == 'index':
    prices_df = pull_daily_prices(query_type, beg_date, end_date, index,
                                  as_of_date)

else:
    raise SystemExit('Error on query_type in main function')

print_friendly_tsid = re.sub('[.]', '_', tsid)
print('Query took %0.2f seconds' % (time.time() - start_time))
# print(prices_df.head(5))
prices_df.to_csv('%s_%s.csv' % (print_friendly_tsid, frequency))

unique_codes = pd.unique((prices_df['tsid']).values)
print('There are %i unique tsid codes' % (len(unique_codes)))
print('There are %s rows' % ('{:,}'.format(len(prices_df.index))))
