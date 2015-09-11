import pandas as pd
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
__version__ = '1.1'

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


def retrieve_q_code(sql_qry):

    try:
        conn = sqlite3.connect(database_location)
        with conn:
            cur = conn.cursor()
            cur.execute(sql_qry)
            rows = cur.fetchall()
            if rows:    # A q_code was found
                # codes = [row[0] for row in rows]
                df = pd.DataFrame(rows)
            else:
                print('Not able to determine the q_code for the provided SQL '
                      'query')
                raise SystemExit(0)
            return df
    except sqlite3.Error as e:
        print('Error when trying to retrieve data from database in '
              'retrieve_q_code')
        print(e)
        raise SystemExit(0)


def pull_daily_prices(query_type, data_vendor, beg_date, end_date, *args):

    try:
        conn = sqlite3.connect(database_location)
        with conn:
            cur = conn.cursor()
            if query_type == 'ticker':
                ticker, exchange = args
                print('Extracting the daily prices for %s that is traded on %s'
                      % (ticker, exchange))
                cur.execute(
                    """ SELECT date, q_code, open, high, low, close, volume,
                               adj_close, split_ratio
                        FROM daily_prices
                        WHERE q_code IN (SELECT q_code
                                         FROM quandl_codes
                                         WHERE component=? AND data=?
                                         AND data_vendor=?)
                        AND date>=? AND date<=?""",
                    (ticker, exchange, data_vendor, beg_date, end_date))
            elif query_type == 'index':
                # ToDo: Re-implement the indices table; need unique symbol_id
                raise SystemExit('Not able to retrieve data based on indices.')
                # index, = args
                # print('Extracting the daily prices for tickers in the %s' %
                #       (index,))
                #
                # # NOTE: there might be a chance that the IN variable only
                # #   allows 999 items
                # cur.execute(
                #     """SELECT date, q_code, open, high, low, close, volume,
                #               adj_close, split_ratio
                #        FROM daily_prices
                #        WHERE q_code IN (SELECT q_code
                #                         FROM quandl_codes
                #                         WHERE symbol_id IN (SELECT symbol_id
                #                                             FROM indices
                #                                           WHERE stock_index=?)
                #                         AND data_vendor=?)
                #        AND date>=? AND date<=?""",
                #     (index, data_vendor, beg_date, end_date))

            else:
                print('Error on query_type in pull_daily_prices. Make sure '
                      'query_type is correct.')

            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows,
                                  columns=['date', 'q_code', 'open', 'high',
                                           'low', 'close', 'volume',
                                           'adj_close', 'split_ratio'])
            else:
                raise SystemExit('No data returned from table query. Try '
                                 'adjusting the criteria for the query.')

            # The next two lines change the index of the df to be the date.
            # Comment them out to prevent this.
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
                ticker, exchange = args
                print('Extracting the minute prices for %s that is traded on %s'
                      % (ticker, exchange))
                cur.execute(
                    """ SELECT date, q_code, open, high, low, close, volume
                        FROM minute_prices
                        WHERE q_code IN (SELECT q_code
                                         FROM quandl_codes
                                         WHERE component=? AND data=?
                                         AND data_vendor='GOOG')
                        AND date>=? AND date<=?""",
                    (ticker, exchange, beg_date, end_date))

            else:
                raise SystemExit('Error on query_type in pull_daily_prices. '
                                 'Make sure query_type is correct.')

            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows,
                                  columns=['date', 'q_code', 'open', 'high',
                                           'low', 'close', 'volume'])
            else:
                raise SystemExit('No data returned from table query. Try '
                                 'adjusting the criteria for the query.')

            # The next two lines change the index of the df to be the date.
            # Comment them out to prevent this.
            df.index = df['date']
            df = df.drop('date', 1)

            return df

    except sqlite3.Error as e:
        print(e)
        raise SystemExit('Error when trying to retrieve price data from '
                         'database in pull_daily_prices')


database_location = 'C:/Users/####/Desktop/pySecMaster.db'

index = 'S&P 500'  # 'S&P 500', 'Russell Midcap', 'Russell 2000', 'Russell 1000'

ticker = 'AAPL'
exchange = 'NASDAQ'
daily_data_vendor = 'WIKI'     # WIKI, GOOG
beg_date = '2015-01-01 00:00:00'
end_date = '2015-12-30 00:00:00'
frequency = 'daily'    # daily, minute

query_type = 'ticker'     # index, ticker, exchange, country, etc.

start_time = time.time()
if query_type == 'ticker':
    if frequency == 'daily':
        prices_df = pull_daily_prices(query_type, daily_data_vendor, beg_date,
                                      end_date, ticker, exchange)
    elif frequency == 'minute':
        prices_df = pull_minute_prices(query_type, beg_date, end_date, ticker,
                                       exchange)
    else:
        raise SystemExit('Error on frequency in main function')

elif query_type == 'index':
    prices_df = pull_daily_prices(query_type, daily_data_vendor, beg_date,
                                  end_date, index)

else:
    raise SystemExit('Error on query_type in main function')

print('Query took %0.2f seconds' % (time.time() - start_time))
# print(prices_df.head(5))
prices_df.to_csv('%s_%s.csv' % (ticker, frequency))

unique_q_codes = pd.unique((prices_df['q_code']).values)
print('There are %i unique q_codes' % (len(unique_q_codes)))
print('There are %s rows' % ('{:,}'.format(len(prices_df.index))))
