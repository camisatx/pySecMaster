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

import time
from functools import wraps
from datetime import datetime, timedelta
import pandas as pd

try:        # Python 3.4
    from urllib.request import urlopen
    from urllib.error import HTTPError
    from urllib.error import URLError
except ImportError:     # Python 2.7
    from urllib2 import urlopen
    from urllib2 import HTTPError
    from urllib2 import URLError


def rate_limit(rate=2000, period_sec=600, threads=1):
    """
    A decorator that limits the rate at which a function is run. If the function
    is run over that rate, a forced sleep will occur. The main purpose of this
    is to make sure an API is not overloaded with requests. For Quandl, the
    default API limit is 2,000 calls in a 10 minute time frame. If multiple
    threads are using the API concurrently, make sure to increase the threads 
    variable to the number of threads being used.

    :param rate: Integer of the number of items that are downloaded
    :param period_sec: Integer of the period (seconds) that the rate occurs in
    :param threads: Integer of the threads that will be running concurrently
    """

    optimal_rate = float((rate / period_sec) / threads)
    min_interval = 1.0 / optimal_rate

    def rate_decorator(func):
        last_check = [0.0]

        @wraps(func)
        def rate_limit_func(*args, **kargs):
            elapsed = time.time() - last_check[0]
            time_to_wait = min_interval - elapsed
            if time_to_wait > 0:
                time.sleep(time_to_wait)
                # print('Sleeping for %0.2f seconds' % int(time_to_wait))
            ret = func(*args, **kargs)
            last_check[0] = time.time()

            return ret
        return rate_limit_func
    return rate_decorator


def dt_to_iso(row, column):
    """
    Change the default date format of "YYYY-MM-DD" to an ISO 8601 format
    """
    raw_date = row[column]
    try:
        raw_date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
    except TypeError:   # Occurs if there is no date provided ("nan")
        raw_date_obj = datetime.today()
    return raw_date_obj.isoformat()


class QuandlDownload(object):
    """
    Downloads the CSV from the Quandl URL provide, and passes a DataFrame 
    back. Provides error handling of HTTP errors.
    """

    def __init__(self, token, db_url_comp, name):
        """ These variables are constant per class call

        :param token: String of the Quandl API token
        :param db_url_comp: List of strings of the Quandl API root
        :param name: String of the object being downloaded. It can be either
            the database name or a Quandl Code
        """

        self.token = token
        self.db_url_comp = db_url_comp
        self.name = name

    @rate_limit(rate=2000, period_sec=600, threads=8)
    def download_csv(self, page_num=None, beg_date=None, download_try=0):
        """ These variables changes with each method call. This method 
        downloads the CSV from Quandl. It is restricted by the rate 
        limit decorator.

        :param page_num: Integer used when downloading database Quandl Codes
        :param beg_date: String of the start date (YYYY-MM-DD) to download
        :return: A CSV file of the downloaded data
        """
        db_url = self.db_url_comp[0] + self.name + self.db_url_comp[1]
        download_try += 1
        # Only Quandl Code downloads have page numbers
        if page_num is not None:
            # There is no need for the Quandl Code queries to have dates
            url_var = str(page_num) + '&auth_token=' + self.token
        else:
            url_var = '?auth_token=' + self.token
            if beg_date is not None:
                url_var = url_var + '&trim_start=' + beg_date

        try:
            csv_file = urlopen(db_url + url_var)
            return csv_file

        except HTTPError as e:
            if str(e) == 'HTTP Error 403: Forbidden':
                raise OSError('HTTPError %s: Reached Quandl API call limit. '
                              'Make the RateLimit more restrictive.' % e.reason)
            elif str(e) == 'HTTP Error 404: Not Found':
                if page_num:
                    raise OSError('HTTPError %s: Quandl page %i for %s not '
                                  'found.' % (e.reason, page_num, self.name))
                else:
                    # Don't raise an exception, as this indicates the last page
                    print('HTTPError %s: %s not found.' % (e.reason, self.name))
            elif str(e) == 'HTTP Error 429: Too Many Requests':
                if download_try < 5:
                    print('HTTPError %s: Exceeded Quandl API limit. Make the '
                          'RateLimit more restrictive. Program will sleep for '
                          '11 minutes and will try again...' % (e.reason,))
                    time.sleep(11 * 60)
                    self.download_csv(page_num, beg_date, download_try)
                else:
                    raise OSError('HTTPError %s: Exceeded Quandl API limit. '
                                  'After trying 5 time, the download was still '
                                  'not successful. You could have hit the '
                                  '50,000 calls per day limit.' % (e.reason,))
            elif str(e) == 'HTTP Error 503: Service Unavailable':
                if download_try < 10:
                    print('HTTPError %s: Server is currently unavailable. '
                          'Maybe the network is down. Will sleep for 10 minutes'
                          % (e.reason,))
                    time.sleep(10 * 60)
                    self.download_csv(page_num, beg_date, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            else:
                print('Base URL used: %s' % (db_url + url_var,))
                if page_num:
                    raise OSError('HTTPError %s: Unknown error when '
                                  'downloading page %i for %s'
                                  % (e.reason, page_num, self.name))
                else:
                    raise OSError('HTTPError %s: Unknown error when '
                                  'downloading %s' % (e.reason, self.name))

        except URLError as e:
            if download_try <= 10:
                print('Warning: Experienced URL Error %s. Program will '
                      'sleep for 30 minutes and will then try again...' %
                      (e.reason,))
                time.sleep(30 * 60)
                self.download_csv(page_num, beg_date, download_try)
            else:
                raise URLError('Warning: Still experiencing URL Error %s. '
                               'After trying 10 times, the error remains. '
                               'Quitting for now, but you can try again later.'
                               % (e.reason,))


def download_quandl_codes(quandl_token, db_url, db_name, page_num):

    """ The token, database name, database url and page number are provided,
    and this downloads the metadata library for that particular page as a csv
    file. Quandl has a restriction where only 300 items can be downloaded at a
    time, thus multiple requests must be sent. This is handled by the page
    number variable.

    :param quandl_token: String of the sensitive Quandl API token
    :param db_url: String of the url address of the particular database's
        metadata to download
    :param db_name: String of the name of the database being downloaded
    :param page_num: Integer of the database's metadata page to download
    :return: A DataFrame with the Quandl database metadata
    """

    col_names = ['q_code', 'name', 'start_date', 'end_date', 'frequency',
                 'last_updated']
    csv_file = QuandlDownload(quandl_token, db_url, db_name)
    file = csv_file.download_csv(page_num)
    try:
        df = pd.read_csv(file, index_col=False, names=col_names,
                         encoding='utf-8')
    except TypeError:
        # When there are no more codes to download, the file object will be an
        #   empty CSV. This will cause the read_csv function to fail on a
        #   TypeError since it can't add column names to an empty DF. Return
        #   an empty DF, which will indicate the no more pages to download.
        return pd.DataFrame()

    df['start_date'] = df.apply(dt_to_iso, axis=1, args=('start_date',))
    df['end_date'] = df.apply(dt_to_iso, axis=1, args=('end_date',))
    df['last_updated'] = df.apply(dt_to_iso, axis=1, args=('last_updated',))

    df.insert(len(df.columns), 'page_num', page_num)
    df.insert(len(df.columns), 'created_date', datetime.utcnow().isoformat())
    df.insert(len(df.columns), 'updated_date', datetime.utcnow().isoformat())

    return df


def download_quandl_data(quandl_token, db_url, q_code, beg_date=None):
    """ Receives a Quandl Code as a string, and it calls the QuandlDownload
    class to actually download it. Once downloaded, this adds titles to the
    column headers, depending on what type of Quandl Code it is. Last, a
    column for the q_code is added to the DataFrame.

    :param q_code: A string of the Quandl Code.
    :param beg_date: String of the start date (YYYY-MM-DD) to download
    :return: A DataFrame with the data points for the Quandl Code.
    """

    # Download the data to a CSV file
    csv_file = QuandlDownload(quandl_token, db_url, q_code)
    if beg_date is not None:
        file = csv_file.download_csv(beg_date=beg_date)
    else:
        file = csv_file.download_csv()

    # Specify the column headers
    if q_code[:4] == 'WIKI':
        column_names = ['date', 'open', 'high', 'low', 'close', 'volume',
                        'ex_dividend', 'split_ratio', 'adj_open',
                        'adj_high', 'adj_low', 'adj_close', 'adj_volume']
    elif q_code[:4] == 'GOOG':
        column_names = ['date', 'open', 'high', 'low', 'close', 'volume']
    elif q_code[:5] == 'YAHOO':
        column_names = ['date', 'open', 'high', 'low', 'close',
                        'volume', 'adjusted_close']
    else:
        print('The chosen data source is not setup within the price '
              'extractor. Please define the columns in DownloadQData.')
        column_names = []

    df = pd.read_csv(file, index_col=False, names=column_names,
                     encoding='utf-8')

    if len(df.index) == 0:
        return df

    df = df[1:]     # Removes the column headers from the Quandl download
    df['date'] = df.apply(dt_to_iso, axis=1, args=('date',))
    df.insert(0, 'q_code', q_code)
    df.insert(len(df.columns), 'updated_date', datetime.utcnow().isoformat())

    return df


# Received captch after about 2000 queries (100 queries/60 sec; 4 threads)
@rate_limit(rate=25, period_sec=60, threads=4)
def download_google_data(db_url, q_code):
    """ Receives a Quandl Code as a string, splits the code into ticker and
    exchange, then passes it to the url to download the data. Once downloaded,
    this adds titles to the column headers.

    :param q_code: A string of the Quandl Code.
    :return: A DataFrame with the data points for the Quandl Code.
    """

    ticker = q_code[q_code.find('_') + 1:]
    exchange = q_code[q_code.find('/') + 1:q_code.find('_')]
    download_try = 0

    # Make the url string; aside from the root, the items can be in any order
    url_string = db_url['root']      # Establish the url root
    for key, item in db_url.items():
        if key == 'root':
            continue    # Already used above
        elif key == 'ticker':
            url_string += '&' + item + ticker
        elif key == 'exchange':
            url_string += '&' + item + exchange
        else:
            url_string += '&' + item

    def download_data(url, download_try):
        """ Downloads the text data from the url provided.

        :param url: String that contains the url of the data to download.
        :param download_try: Integer of the number of attempts to download data.
        :return: A list of bytes of the data downloaded.
        """

        download_try += 1
        try:
            # Download the data
            return urlopen(url).readlines()

        except HTTPError as e:
            if str(e) == 'HTTP Error 403: Forbidden':
                raise OSError('HTTPError %s: Reached API call limit. Make the '
                              'RateLimit more restrictive.' % (e.reason,))
            elif str(e) == 'HTTP Error 404: Not Found':
                raise OSError('HTTPError %s: %s not found' % (e.reason, q_code))
            elif str(e) == 'HTTP Error 429: Too Many Requests':
                if download_try <= 5:
                    print('HTTPError %s: Exceeded API limit. Make the '
                          'RateLimit more restrictive. Program will sleep for '
                          '11 minutes and will try again...' % (e.reason,))
                    time.sleep(11 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Exceeded API limit. After '
                                  'trying 5 time, the download was still not '
                                  'successful. You could have hit the per day '
                                  'call limit.' % (e.reason,))
            elif str(e) == 'HTTP Error 503: Service Unavailable':
                # Received this HTTP Error after 2000 queries. Browser showed
                #   captch message upon loading url.
                if download_try <= 10:
                    print('HTTPError %s: Server is currently unavailable. '
                          'Maybe the network is down or the server is blocking '
                          'you. Will sleep for 10 minutes...' % (e.reason,))
                    time.sleep(10 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            else:
                print('Base URL used: %s' % (url,))
                raise OSError('HTTPError %s: Unknown error when downloading %s'
                              % (e.reason, q_code))

        except URLError as e:
            if download_try <= 10:
                print('Warning: Experienced URL Error %s. Program will '
                      'sleep for 30 minutes and will then try again...' %
                      (e.reason,))
                time.sleep(30 * 60)
                download_data(url, download_try)
            else:
                raise URLError('Warning: Still experiencing URL Error %s. '
                               'After trying 10 times, the error remains. '
                               'Quitting for now, but you can try again later.'
                               % (e.reason,))

    def google_data_processing(url_obj):
        """ Takes the url object returned from Google, and format the text data
        into a DataFrame that can be saved to the SQL Database. Saves each
        processed line to a list as a tuple, with each element a piece of data.
        The list is changed to a DataFrame before being returned.

        :param url_obj: A text byte object that represents the downloaded data
        :return: A DataFrame of the processed minute data.
        """

        # Find the timezone of the exchange the ticker is traded on
        if url_obj[6][:15].decode('utf-8') == 'TIMEZONE_OFFSET':
            timezone = int(url_obj[6][16:].decode('utf-8'))
        # Timezone on the 7th line if receiving extended hours quotations
        elif url_obj[7][:15].decode('utf-8') == 'TIMEZONE_OFFSET':
            timezone = int(url_obj[7][16:].decode('utf-8'))
        else:
            timezone = -240     # Assume default of the east coast

        # Find the interval in seconds that the data was downloaded to
        if url_obj[3][:8].decode('utf-8') == 'INTERVAL':
            interval = int(url_obj[3][9:].decode('utf-8'))
        # Interval on the 4th line if receiving extended hours quotations
        elif url_obj[4][:8].decode('utf-8') == 'INTERVAL':
            interval = int(url_obj[4][9:].decode('utf-8'))
        else:
            interval = 60       # Assume default of 60 seconds

        data = []
        # From the text file downloaded, adding each line to a list as a tuple
        for line_num in range(7, len(url_obj)):
            line = url_obj[line_num].decode('utf-8')
            if line.count(',') == 5:
                date, close, high, low, open_, volume = line.split(',')
                if str(date[0]) == 'a':
                    date_obj = datetime.utcfromtimestamp(int(date[1:]) +
                                                         timezone)
                else:
                    # Get the number of seconds from the prior data line (unix)
                    prior_line = url_obj[line_num - 1].decode('utf-8')
                    if prior_line[0] == 'a':
                        prior_unix_sec = (prior_line[prior_line.find(',') - 2:
                                          prior_line.find(',')])
                    else:
                        prior_unix_sec = prior_line[:prior_line.find(',')]
                    # The difference between the current and the prior unix sec
                    unix_sec_diff = int(date) - int(prior_unix_sec)
                    # New date object, taking the prior date and adding the
                    #   second difference times the data interval used
                    date_obj = data[-1][0] + timedelta(seconds=unix_sec_diff *
                                                       interval)
                data.append(tuple((date_obj, float(close), float(high),
                                   float(low), float(open_), float(volume))))

        column_names = ['date', 'close', 'high', 'low', 'open', 'volume']
        min_df = pd.DataFrame(data, columns=column_names)
        return min_df

    url_obj = download_data(url_string, download_try)

    try:
        df = google_data_processing(url_obj)
        # Data successfully downloaded; check to see if code was on the list
        file_local = 'load_tables/goog_min_codes_wo_data.csv'
        codes_wo_data_df = pd.read_csv(file_local, index_col=False)
        if len(codes_wo_data_df.loc[codes_wo_data_df['q_code'] == q_code]) > 0:
            # The q_code that was downloaded had no data on the previous run.
            #   Remove the code from the CSV list.
            wo_data_df = codes_wo_data_df[codes_wo_data_df.q_code != q_code]
            wo_data_df.to_csv(file_local, index=False)
            print('%s was removed from the wo_data CSV file since data was '
                  'available for download.' % (q_code,))
    except IndexError:
        # There is no minute data for this code; add to CSV file via DF
        min_df = pd.DataFrame(data=[(q_code, datetime.utcnow().isoformat())],
                              columns=['q_code', 'date_tried'])
        with open('load_tables/goog_min_codes_wo_data.csv', 'a') as f:
            min_df.to_csv(f, mode='a', header=False, index=False)
        # print('Flag: Not able to process data for %s' % (q_code,))
        return pd.DataFrame()
    except Exception as e:
        print('Flag: Error occurred when processing data for %s' % (q_code,))
        print(e)
        return pd.DataFrame()

    if len(df.index) == 0:
        return df

    def datetime_to_iso(row, column):
        return row[column].isoformat()

    df['date'] = df.apply(datetime_to_iso, axis=1, args=('date',))
    df.insert(0, 'q_code', q_code)
    df.insert(len(df.columns), 'updated_date', datetime.utcnow().isoformat())

    return df
