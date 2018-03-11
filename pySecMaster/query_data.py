from datetime import datetime
import numpy as np
import pandas as pd
import psycopg2
import re
import time

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2018 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.5.0'

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


def calculate_adjusted_prices(df, column):
    """ Vectorized approach for calculating the adjusted prices for the
    specified column in the provided DataFrame. This creates a new column
    called 'adj_<column name>' with the adjusted prices. This function requires
    that the DataFrame have columns with dividend and split values.

    NOTE: This assumes the input split values direct. E.g. 7-for-1 split = 7

    :param df: DataFrame with raw prices along with dividend and split_ratio
        values
    :param column: String of which price column should have adjusted prices
        created for it
    :return: DataFrame with the addition of the adjusted price column
    """

    adj_column = 'adj_' + column

    # Reverse the DataFrame order, sorting by date in descending order
    df.sort_index(ascending=False, inplace=True)

    price_col = df[column].values
    split_col = df['split'].values
    dividend_col = df['dividend'].values
    adj_price_col = np.zeros(len(df.index))
    adj_price_col[0] = price_col[0]

    for i in range(1, len(price_col)):
        adj_price_col[i] = \
            round((adj_price_col[i - 1] + adj_price_col[i - 1] *
                   (((price_col[i] * (1/split_col[i - 1])) -
                     price_col[i - 1] -
                     dividend_col[i - 1]) / price_col[i - 1])), 4)

    df[adj_column] = adj_price_col

    # Change the DataFrame order back to dates ascending
    df.sort_index(ascending=True, inplace=True)

    return df


def pull_daily_prices(database, user, password, host, port, query_type,
                      data_vendor_id, beg_date, end_date, adjust=True,
                      source='tsid', *args):
    """ Query the daily prices from the database for the tsid provided between
    the start and end dates. Return a DataFrame with the prices.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param query_type: String of which query to run
    :param data_vendor_id: Integer of which data vendor id to return prices for
    :param beg_date: String of the ISO date to start with
    :param end_date: String of the ISO date to end with
    :param adjust: Boolean of whether to adjust the values or not; default True
    :param source: String of the ticker's source
    :return: DataFrame of the returned prices
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    try:
        with conn:
            cur = conn.cursor()
            if query_type == 'ticker':
                tsid, = args
                print('Extracting the daily prices for %s' % (tsid,))

                cur.execute("""SELECT date, source_id AS tsid, open, high, low,
                                close, volume, dividend, split
                            FROM daily_prices
                            WHERE source_id=%s AND source=%s
                            AND data_vendor_id=%s
                            AND date>=%s AND date<=%s""",
                            (tsid, source, data_vendor_id, beg_date, end_date))

            else:
                raise NotImplementedError('Query type %s is not implemented '
                                          'within pull_daily_prices' %
                                          query_type)

            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows,
                                  columns=['date', 'tsid', 'open', 'high',
                                           'low', 'close', 'volume',
                                           'dividend', 'split'])
            else:
                raise SystemExit('No data returned from table query. Try '
                                 'adjusting the criteria for the query.')

            # The next two lines change the index of the df to be the date.
            df.set_index(['date'], inplace=True)
            df.index.name = 'date'

            df.sort_index(inplace=True)

            if adjust:
                # Change the columns from decimal to float
                df['dividend'] = df['dividend'].apply(lambda x: float(x))
                df['split'] = df['split'].apply(lambda x: float(x))
                df['close'] = df['close'].apply(lambda x: float(x))

                # Calculate the adjusted prices for the close column
                df = calculate_adjusted_prices(df=df, column='close')

            return df

    except psycopg2.Error as e:
        print('Error when trying to retrieve price data from the %s database '
              'in pull_daily_prices' % database)
        print(e)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'pull_daily_prices. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in pull_daily_prices')


def pull_minute_prices(database, user, password, host, port, query_type,
                       data_vendor_id, beg_date, end_date, source='tsid',
                       *args):
    """ Query the minute prices from the database for the tsid provided between
    the start and end dates. Return a DataFrame with the prices.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param query_type: String of which query to run
    :param data_vendor_id: Integer of which data vendor id to return prices for
    :param beg_date: String of the ISO date to start with
    :param end_date: String of the ISO date to end with
    :param source: String of the source
    :param args:
    :return: DataFrame of the returned prices
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    try:
        with conn:
            cur = conn.cursor()
            if query_type == 'ticker':
                tsid, = args
                print('Extracting the minute prices for %s' % (tsid,))

                cur.execute("""SELECT date, source_id AS tsid, open, high, low,
                                close, volume
                            FROM minute_prices
                            WHERE source_id=%s AND source=%s
                            AND data_vendor_id=%s
                            AND date>=%s AND date<=%s""",
                            (tsid, source, data_vendor_id, beg_date, end_date))
            else:
                raise NotImplementedError('Query type %s is not implemented '
                                          'within pull_minute_prices' %
                                          query_type)

            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows,
                                  columns=['date', 'tsid', 'open', 'high',
                                           'low', 'close', 'volume'])
            else:
                raise SystemExit('No data returned from table query. Try '
                                 'adjusting the criteria for the query.')

            # The next two lines change the index of the df to be the date.
            df.set_index(['date'], inplace=True)
            df.index.name = ['date']

            df.sort_index(inplace=True)

            return df

    except psycopg2.Error as e:
        print('Error when trying to retrieve price data from the %s database '
              'in pull_minute_prices' % database)
        print(e)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'pull_minute_prices. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in pull_minute_prices')


if __name__ == '__main__':

    from utilities.user_dir import user_dir

    userdir = user_dir()

    test_database = userdir['postgresql']['pysecmaster_db']
    test_user = userdir['postgresql']['pysecmaster_user']
    test_password = userdir['postgresql']['pysecmaster_password']
    test_host = userdir['postgresql']['pysecmaster_host']
    test_port = userdir['postgresql']['pysecmaster_port']

    test_query_type = 'ticker'     # index, ticker
    test_tsid = 'AAPL.Q.0'
    test_data_vendor_id = 1        # Quandl WIKi
    # test_data_vendor_id = 11        # Quandl EOD
    # test_data_vendor_id = 15        # pySecMaster_Consensus
    # test_data_vendor_id = 12        # Google_Finance
    test_beg_date = '1950-01-01 00:00:00'
    test_end_date = '2018-12-30 00:00:00'
    frequency = 'daily'    # daily, minute

    start_time = time.time()
    if test_query_type == 'ticker':
        if frequency == 'daily':
            prices_df = pull_daily_prices(test_database, test_user,
                                          test_password, test_host, test_port,
                                          test_query_type, test_data_vendor_id,
                                          test_beg_date, test_end_date,
                                          True, 'tsid', test_tsid)

        elif frequency == 'minute':
            prices_df = pull_minute_prices(test_database, test_user,
                                           test_password, test_host, test_port,
                                           test_query_type, test_data_vendor_id,
                                           test_beg_date, test_end_date,
                                           'tsid', test_tsid)

        else:
            raise NotImplementedError('Frequency %s is not implemented within '
                                      'query_data.py' % frequency)
    else:
        raise NotImplementedError('Query type %s is not implemented within '
                                  'query_data.py' % test_query_type)

    csv_friendly_tsid = re.sub('[.]', '_', test_tsid)
    print('Query took %0.2f seconds' % (time.time() - start_time))
    print(prices_df)
    #prices_df.to_csv('output/%s_%s_%s.csv' %
    #                 (csv_friendly_tsid, frequency,
    #                  datetime.today().strftime('%Y%m%d')))

    unique_codes = pd.unique((prices_df['tsid']).values)
    print('There are %i unique tsid codes' % (len(unique_codes)))
    print('There are %s rows' % ('{:,}'.format(len(prices_df.index))))
    print(datetime.today().strftime('%Y%m%d'))
