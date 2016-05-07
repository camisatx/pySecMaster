from datetime import datetime, timedelta
from functools import wraps
import pandas as pd
import time
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

from utilities.date_conversions import date_to_iso


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


class QuandlDownload(object):

    def __init__(self, quandl_token, db_url):
        """ Items that are always required when downloading Quandl data.

        :param quandl_token: String of the sensitive Quandl API token
        :param db_url: String of the url address of the particular database's
            metadata to download
        """

        self.quandl_token = quandl_token
        self.db_url = db_url

    def download_quandl_codes(self, db_name, page_num, download_try=0):
        """ The token, database name, database url and page number are provided,
        and this downloads the metadata library for that particular page as a
        csv file. Quandl has a restriction where only 300 items can be
        downloaded at a time, thus multiple requests must be sent. This is
        handled by the page number variable.

        :param db_name: String of the name of the database being downloaded
        :param page_num: Integer of the database's metadata page to download
        :param download_try: Integer of the number of attempts to download data
        :return: A DataFrame with the Quandl database metadata
        """

        download_try += 1
        col_names = ['q_code', 'name', 'start_date', 'end_date', 'frequency',
                     'last_updated']
        file = self.download_data(db_name, page_num=page_num)

        try:
            df = pd.read_csv(file, index_col=False, names=col_names,
                             encoding='utf-8')
            if len(df) == 0:
                # When there are no more codes to download, the file object
                #   will be an empty CSV, and in turn, and empty DF. Return an
                #   empty DF, which will indicate the no more pages to download.
                return pd.DataFrame()
        except TypeError:
            # When there are no more codes to download, the file object will be
            #   an empty CSV. With pandas prior to 0.17, this will cause the
            #   read_csv function to fail on a TypeError since it's not able to
            #   add column names to an empty DF. Return an empty DF, which will
            #   indicate the no more pages to download.
            return pd.DataFrame()
        except Exception as e:
            print(e)
            if download_try <= 10:
                print('Error: An unknown issue occurred when downloading the '
                      'Quandl codes CSV. Will download the CSV file again.')
                df = self.download_quandl_codes(db_name, page_num, download_try)
                return df       # Stop the recursion
            else:
                raise OSError('Unknown error when downloading page %s of the '
                              '%s database. Quitting after 10 failed attempts.'
                              % (page_num, db_name))

        df['start_date'] = df.apply(date_to_iso, axis=1, args=('start_date',))
        df['end_date'] = df.apply(date_to_iso, axis=1, args=('end_date',))
        df['last_updated'] = df.apply(date_to_iso, axis=1, args=('last_updated',))

        df.insert(len(df.columns), 'page_num', page_num)
        df.insert(len(df.columns), 'created_date', datetime.now().isoformat())
        df.insert(len(df.columns), 'updated_date', datetime.now().isoformat())

        return df

    def download_quandl_data(self, q_code, csv_out, beg_date=None,
                             verbose=True):
        """ Receives a Quandl Code as a string, and it calls the QuandlDownload
        class to actually download it. Once downloaded, this adds titles to the
        column headers, depending on what type of Quandl Code it is. Last, a
        column for the q_code is added to the DataFrame.

        :param q_code: A string of the Quandl Code
        :param csv_out: String of directory and CSV file name; used to store
            the quandl codes that do not have any data
        :param beg_date: String of the start date (YYYY-MM-DD) to download
        :param verbose: Boolean
        :return: A DataFrame with the data points for the Quandl Code
        """

        # Download the data to a CSV file
        if beg_date is not None:
            file = self.download_data(q_code, beg_date=beg_date)
        else:
            file = self.download_data(q_code)

        # Specify the column headers
        if q_code[:4] == 'WIKI':
            column_names = ['date', 'open', 'high', 'low', 'close', 'volume',
                            'ex_dividend', 'split_ratio', 'adj_open',
                            'adj_high', 'adj_low', 'adj_close', 'adj_volume']
            columns_to_remove = ['adj_open', 'adj_high', 'adj_low', 'adj_close',
                                 'adj_volume']
        elif q_code[:4] == 'GOOG':
            column_names = ['date', 'open', 'high', 'low', 'close', 'volume']
            columns_to_remove = []
        elif q_code[:5] == 'YAHOO':
            column_names = ['date', 'open', 'high', 'low', 'close',
                            'volume', 'adjusted_close']
            columns_to_remove = ['adjusted_close']
        else:
            print('The data source for %s is not implemented in the price '
                  'extractor. Please define the columns in '
                  'QuandlDownload.download_quandl_data.' % q_code)
            return pd.DataFrame()

        if file:
            raw_df = pd.read_csv(file, index_col=False, names=column_names,
                                 encoding='utf-8')

            # Data successfully downloaded; check to see if code was on the list
            codes_wo_data_df = pd.read_csv(csv_out, index_col=False)
            if len(codes_wo_data_df.
                    loc[codes_wo_data_df['q_code'] == q_code]) > 0:
                # This q_code now has data whereas it didn't on that last run.
                #   Remove the code from the DataFrame
                wo_data_df = codes_wo_data_df[codes_wo_data_df.q_code != q_code]
                # Remove any duplicates (keeping the latest) and save to a CSV
                clean_wo_data_df = wo_data_df.drop_duplicates(subset='q_code',
                                                              keep='last')
                clean_wo_data_df.to_csv(csv_out, index=False)
                if verbose:
                    print('%s was removed from the wo_data CSV file since data '
                          'was available for download.' % (q_code,))
        else:
            # There is no minute data for this code so add it to the CSV file
            codes_wo_data_df = pd.read_csv(csv_out, index_col=False)
            cur_date = datetime.now().isoformat()
            if len(codes_wo_data_df.
                    loc[codes_wo_data_df['q_code'] == q_code]) > 0:
                # The code already exists within the CSV, so update the date
                codes_wo_data_df.set_value(codes_wo_data_df['q_code'] == q_code,
                                           'date_tried', cur_date)
                # Remove any duplicates (keeping the latest) and save to a CSV
                clean_wo_data_df = codes_wo_data_df.\
                    drop_duplicates(subset='q_code', keep='last')
                clean_wo_data_df.to_csv(csv_out, index=False)
                if verbose:
                    print('%s still did not have data. Date tried was updated '
                          'in the wo_data CSV file.' % (q_code,))
            else:
                # The code does not exists within the CSV, so create and append
                #   it to the CSV file. Do this via a DataFrame to CSV append
                no_data_df = pd.DataFrame(data=[(q_code, cur_date)],
                                          columns=['q_code', 'date_tried'])
                with open(csv_out, 'a') as f:
                    no_data_df.to_csv(f, mode='a', header=False, index=False)
                if verbose:
                    print('%s did not have data, thus it was added to the '
                          'wo_data CSV file.' % (q_code,))

            # Return an empty DF; QuandlDataExtractor will be able to handle it
            return pd.DataFrame()

        if len(raw_df.index) == 0:
            return raw_df

        raw_df = raw_df[1:]     # Removes the column headers from data download
        raw_df['date'] = raw_df.apply(date_to_iso, axis=1, args=('date',))
        # raw_df.insert(0, 'q_code', q_code)
        raw_df.insert(len(raw_df.columns), 'updated_date',
                      datetime.now().isoformat())

        # Remove all adjusted value columns
        raw_df.drop(columns_to_remove, inplace=True)

        return raw_df

    def download_data(self, name, page_num=None, beg_date=None, download_try=0):
        """
        Downloads the CSV from the Quandl URL provide, and passes a DataFrame
        back. Provides error handling of HTTP errors. It is restricted by the
        rate limit decorator.

        :param name: String of the object being downloaded. It can be either
            the database name or a Quandl Code
        :param page_num: Integer used when downloading database Quandl Codes
        :param beg_date: String of the start date (YYYY-MM-DD) to download
        :return: A CSV file of the downloaded data
        :param download_try: Optional integer that indicates a download
            retry; utilized after an HTTP error to try the download again
            recursively
        """

        db_url = self.db_url[0] + name + self.db_url[1]
        download_try += 1

        # Only Quandl Code downloads have page numbers
        if page_num is not None:
            # There is no need for the Quandl Code queries to have dates
            url_var = str(page_num) + '&auth_token=' + self.quandl_token
        else:
            url_var = '?auth_token=' + self.quandl_token
            if beg_date is not None:
                url_var = url_var + '&trim_start=' + beg_date

        try:
            csv_file = urlopen(db_url + url_var)
            return csv_file

        except HTTPError as e:
            if str(e) == 'HTTP Error 400: Bad Request':
                # Don't raise an exception; indicates a non existent code
                print('HTTPError %s: %s does not exist.' % (e.reason, name))
            elif str(e) == 'HTTP Error 403: Forbidden':
                raise OSError('HTTPError %s: Reached Quandl API call '
                              'limit. Make the RateLimit more restrictive.'
                              % e.reason)
            elif str(e) == 'HTTP Error 404: Not Found':
                if page_num:
                    raise OSError('HTTPError %s: Quandl page %i for %s not '
                                  'found.' % (e.reason, page_num, name))
                # else:
                #     # Don't raise an exception; indicates the last page
                #     print('HTTPError %s: %s not found.' % (e.reason, name))
            elif str(e) == 'HTTP Error 429: Too Many Requests':
                if download_try <= 5:
                    print('HTTPError %s: Exceeded Quandl API limit. Make '
                          'the rate_limit more restrictive. Program will '
                          'sleep for 11 minutes and will try again...'
                          % (e.reason,))
                    time.sleep(11 * 60)
                    self.download_data(name, download_try=download_try)
                else:
                    raise OSError('HTTPError %s: Exceeded Quandl API '
                                  'limit. After trying 5 time, the '
                                  'download was still not successful. You '
                                  'could have hit the 50,000 calls per '
                                  'day limit.' % (e.reason,))
            elif str(e) == 'HTTP Error 502: Bad Gateway':
                if download_try <= 10:
                    print('HTTPError %s: Encountered a bad gateway with '
                          'the server. Maybe the network is down. Will '
                          'sleep for 5 minutes' % (e.reason,))
                    time.sleep(5 * 60)
                    self.download_data(name, download_try=download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 times, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 503: Service Unavailable':
                if download_try <= 10:
                    print('HTTPError %s: Server is currently unavailable. '
                          'Maybe the network is down. Will sleep for 5 '
                          'minutes' % (e.reason,))
                    time.sleep(5 * 60)
                    self.download_data(name, download_try=download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 504: GATEWAY_TIMEOUT':
                if download_try <= 10:
                    print('HTTPError %s: Server connection timed out. '
                          'Maybe the network is down. Will sleep for 5 '
                          'minutes' % (e.reason,))
                    time.sleep(5 * 60)
                    self.download_data(name, download_try=download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            else:
                print('Base URL used: %s' % (db_url + url_var,))
                if page_num:
                    raise OSError('HTTPError %s: Unknown error when '
                                  'downloading page %i for %s'
                                  % (e.reason, page_num, name))
                else:
                    raise OSError('HTTPError %s: Unknown error when '
                                  'downloading %s' % (e.reason, name))
        except URLError as e:
            if download_try <= 10:
                print('Warning: Experienced URL Error %s. Program will '
                      'sleep for 5 minutes and will then try again...' %
                      (e.reason,))
                print('URL used: %s' % (db_url + url_var,))
                time.sleep(5 * 60)
                self.download_data(name, download_try=download_try)
            else:
                raise URLError('Warning: Still experiencing URL Error %s. '
                               'After trying 10 times, the error remains. '
                               'Quitting for now, but you can try again '
                               'later.' % (e.reason,))
        except Exception as e:
            print(e)
            raise OSError('Warning: Encountered an unknown error when '
                          'downloading %s in download_csv in download.py' %
                          (name,))


def download_google_data(db_url, tsid, exchanges_df, csv_out, verbose=True):
    """ Receives a tsid as a string, splits the code into ticker and
    exchange, then passes it to the url to download the data. Once downloaded,
    this adds titles to the column headers.

    :param db_url: Dictionary of google finance url components
    :param tsid: A string of the tsid
    :param exchanges_df: DataFrame with all exchanges and their symbols
    :param csv_out: String with the file directory for the CSV file that has
        all the codes that don't have any data
    :param verbose: Boolean of whether to print debugging statements
    :return: A DataFrame with the data points for the tsid.
    """

    ticker = tsid[:tsid.find('.')]
    exchange_symbol = tsid[tsid.find('.')+1:tsid.find('.', tsid.find('.')+1)]

    try:
        # Use the tsid exchange symbol to get the Google exchange symbol
        exchange = (exchanges_df.loc[exchanges_df['tsid_symbol'] ==
                                     exchange_symbol, 'goog_symbol'].values)
    except KeyError:
        exchange = None

    # Make the url string; aside from the root, the items can be in any order
    url_string = db_url['root']      # Establish the url root
    for key, item in db_url.items():
        if key == 'root':
            continue    # Already used above
        elif key == 'ticker':
            url_string += '&' + item + ticker
        elif key == 'exchange':
            if exchange:
                url_string += '&' + item + exchange[0]
        else:
            url_string += '&' + item

    def download_data(url, download_try=0):
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
                raise OSError('HTTPError %s: %s not found' % (e.reason, tsid))
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
            elif str(e) == 'HTTP Error 502: Bad Gateway':
                if download_try <= 10:
                    print('HTTPError %s: Encountered a bad gateway with the '
                          'server. Maybe the network is down. Will sleep for '
                          '5 minutes'
                          % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 times, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 503: Service Unavailable':
                # Received this HTTP Error after 2000 queries. Browser showed
                #   captcha message upon loading url.
                if download_try <= 10:
                    print('HTTPError %s: Server is currently unavailable. '
                          'Maybe the network is down or the server is blocking '
                          'you. Will sleep for 5 minutes...' % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 504: GATEWAY_TIMEOUT':
                if download_try <= 10:
                    print('HTTPError %s: Server connection timed out. Maybe '
                          'the network is down. Will sleep for 5 minutes'
                          % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            else:
                print('Base URL used: %s' % (url,))
                raise OSError('HTTPError %s: Unknown error when downloading %s'
                              % (e.reason, tsid))

        except URLError as e:
            if download_try <= 10:
                print('Warning: Experienced URL Error %s. Program will '
                      'sleep for 5 minutes and will then try again...' %
                      (e.reason,))
                time.sleep(5 * 60)
                download_data(url, download_try)
            else:
                raise URLError('Warning: Still experiencing URL Error %s. '
                               'After trying 10 times, the error remains. '
                               'Quitting for now, but you can try again later.'
                               % (e.reason,))

        except Exception as e:
            print(e)
            raise OSError('Warning: Encountered an unknown error when '
                          'downloading %s in download_data in download.py' %
                          (tsid,))

    def google_data_processing(url_obj):
        """ Takes the url object returned from Google, and formats the text data
        into a DataFrame that can be saved to the SQL Database. Saves each
        processed line to a list as a tuple, with each element a piece of data.
        The list is changed to a DataFrame before being returned.

        :param url_obj: A text byte object that represents the downloaded data
        :return: A DataFrame of the processed minute data.
        """

        # Find the interval in seconds that the data was downloaded to
        if url_obj[3][:8].decode('utf-8') == 'INTERVAL':
            interval = int(url_obj[3][9:].decode('utf-8'))
            # Normal trading hours: data starts on line 7
            data_start_line = 7
        # Interval on the 4th line if receiving extended hours quotations
        elif url_obj[4][:8].decode('utf-8') == 'INTERVAL':
            interval = int(url_obj[4][9:].decode('utf-8'))
            # Extended trading hours: data starts on line 8
            data_start_line = 8
        else:
            interval = 60           # Assume default of 60 seconds
            data_start_line = 7     # Assume data starts on line 7

        data = []
        # From the text file downloaded, adding each line to a list as a tuple
        for line_num in range(data_start_line, len(url_obj)):
            line = url_obj[line_num].decode('utf-8')
            if line.count(',') == 5:
                date, close, high, low, open_, volume = line.split(',')
                if str(date[0]) == 'a':
                    # The whole unix time
                    date_obj = datetime.utcfromtimestamp(int(date[1:]))
                else:
                    # Get the prior line's unix time/period
                    prior_line = url_obj[line_num - 1].decode('utf-8')
                    if prior_line[0] == 'a':
                        # The prior line had the entire unix time
                        prior_unix_time = prior_line[1:prior_line.find(',')]
                        # Add the product of the current date period and the
                        #   interval to the prior line's unix time
                        next_date = int(prior_unix_time) + (int(date)*interval)
                        date_obj = datetime.utcfromtimestamp(next_date)
                    else:
                        # The prior line is a date period, so find the delta
                        prior_unix_sec = prior_line[:prior_line.find(',')]
                        # Difference between the current and the prior unix sec
                        unix_sec_diff = int(date) - int(prior_unix_sec)
                        # Add the product of the time delta and the interval to
                        #   the prior bar's datetime
                        date_obj = (data[-1][0] +
                                    timedelta(seconds=unix_sec_diff*interval))
                data.append(tuple((date_obj, float(close), float(high),
                                   float(low), float(open_), float(volume))))

        column_names = ['date', 'close', 'high', 'low', 'open', 'volume']
        processed_df = pd.DataFrame(data, columns=column_names)
        return processed_df

    url_obj = download_data(url_string)

    try:
        raw_df = google_data_processing(url_obj)
    except IndexError:
        raw_df = pd.DataFrame()

    if len(raw_df.index) > 0:
        # Data successfully downloaded; check to see if code was on the list
        codes_wo_data_df = pd.read_csv(csv_out, index_col=False)
        if len(codes_wo_data_df.loc[codes_wo_data_df['tsid'] == tsid]) > 0:
            # This tsid now has data whereas it didn't on that last run.
            #   Remove the code from the DataFrame
            wo_data_df = codes_wo_data_df[codes_wo_data_df.tsid != tsid]
            # Remove any duplicates (keeping the latest) and save to a CSV
            clean_wo_data_df = wo_data_df.drop_duplicates(subset='tsid',
                                                          keep='last')
            clean_wo_data_df.to_csv(csv_out, index=False)
            if verbose:
                print('%s was removed from the wo_data CSV file since data '
                      'was available for download.' % (tsid,))
    else:
        # There is no price data for this code; add to CSV file via DataFrame
        try:
            codes_wo_data_df = pd.read_csv(csv_out, index_col=False)
            cur_date = datetime.now().isoformat()
            if len(codes_wo_data_df.loc[codes_wo_data_df['tsid'] == tsid]) > 0:
                # The code already exists within the CSV, so update the date
                codes_wo_data_df.set_value(codes_wo_data_df['tsid'] == tsid,
                                           'date_tried', cur_date)
                # Remove any duplicates (keeping the latest) and save to a CSV
                clean_wo_data_df = \
                    codes_wo_data_df.drop_duplicates(subset='tsid', keep='last')
                clean_wo_data_df.to_csv(csv_out, index=False)
                if verbose:
                    print('%s still did not have data. Date tried was updated '
                          'in the wo_data CSV file.' % (tsid,))
            else:
                # The code does not exists within the CSV, so create and append
                #   it to the CSV file. Do this via a DataFrame to CSV append
                no_data_df = pd.DataFrame(data=[(tsid, cur_date)],
                                          columns=['tsid', 'date_tried'])
                with open(csv_out, 'a') as f:
                    no_data_df.to_csv(f, mode='a', header=False, index=False)
                if verbose:
                    print('%s did not have data, thus it was added to the '
                          'wo_data CSV file.' % (tsid,))
        except Exception as e:
            print('Error occurred when trying to update %s CSV data for %s' %
                  (csv_out, tsid))
            print(e)

        # Return an empty DF; DataExtraction class will be able to handle it
        return pd.DataFrame()

    def datetime_to_iso(row, column):
        return row[column].isoformat()

    # google_data_processing method converts the string dates to datetimes
    raw_df['date'] = raw_df.apply(datetime_to_iso, axis=1, args=('date',))
    # raw_df.insert(0, 'tsid', tsid)
    raw_df.insert(len(raw_df.columns), 'updated_date',
                  datetime.now().isoformat())

    return raw_df


def download_yahoo_data(db_url, tsid, exchanges_df, csv_out, verbose=True):
    """ Receives a tsid as a string, splits the code into ticker and
    exchange, then passes it to the url to download the data. Once downloaded,
    this adds titles to the column headers.

    :param db_url: Dictionary of yahoo finance url components
    :param tsid: A string of the tsid
    :param exchanges_df: DataFrame with all exchanges and their symbols
    :param csv_out: String with the file directory for the CSV file that has
        all the codes that don't have any data
    :param verbose: Boolean of whether to print debugging statements
    :return: A DataFrame with the data points for the tsid.
    """

    ticker = tsid[:tsid.find('.')]
    exchange_symbol = tsid[tsid.find('.')+1:tsid.find('.', tsid.find('.')+1)]

    try:
        # Use the tsid exchange symbol to get the Yahoo exchange symbol
        exchange = (exchanges_df.loc[exchanges_df['tsid_symbol'] ==
                                     exchange_symbol, 'yahoo_symbol'].values)
    except KeyError:
        exchange = None

    # Make the url string; aside from the root, the items can be in any order
    url_string = db_url['root']      # Establish the url root
    for key, item in db_url.items():
        if key == 'root':
            continue    # Already used above
        elif key == 'ticker':
            if exchange:
                # If an exchange was found, Yahoo requires both ticker and
                #   exchange
                url_string += '&' + item + ticker + '.' + exchange
            else:
                # Ticker is in a major exchange and doesn't need exchange info
                url_string += '&' + item + ticker
        else:
            url_string += '&' + item

    def download_data(url, download_try=0):
        """ Downloads the CSV file from the url provided.

        :param url: String that contains the url of the data to download.
        :param download_try: Integer of the number of attempts to download data.
        :return: A list of bytes of the data downloaded.
        """

        download_try += 1
        try:
            # Download the csv file
            return urlopen(url)

        except HTTPError as e:
            if str(e) == 'HTTP Error 403: Forbidden':
                raise OSError('HTTPError %s: Reached API call limit. Make the '
                              'RateLimit more restrictive.' % (e.reason,))
            elif str(e) == 'HTTP Error 404: Not Found':
                # if verbose:
                #     print('HTTPError %s: %s not found' % (e.reason, tsid))
                return None
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
            elif str(e) == 'HTTP Error 502: Bad Gateway':
                if download_try <= 10:
                    print('HTTPError %s: Encountered a bad gateway with the '
                          'server. Maybe the network is down. Will sleep for '
                          '5 minutes'
                          % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 times, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 503: Service Unavailable':
                # Received this HTTP Error after 2000 queries. Browser showed
                #   captcha message upon loading url.
                if download_try <= 10:
                    print('HTTPError %s: Server is currently unavailable. '
                          'Maybe the network is down or the server is blocking '
                          'you. Will sleep for 5 minutes...' % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 504: GATEWAY_TIMEOUT':
                if download_try <= 10:
                    print('HTTPError %s: Server connection timed out. Maybe '
                          'the network is down. Will sleep for 5 minutes'
                          % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            else:
                print('Base URL used: %s' % (url,))
                raise OSError('HTTPError %s: Unknown error when downloading %s'
                              % (e.reason, tsid))

        except URLError as e:
            if download_try <= 10:
                print('Warning: Experienced URL Error %s. Program will '
                      'sleep for 5 minutes and will then try again...' %
                      (e.reason,))
                time.sleep(5 * 60)
                download_data(url, download_try)
            else:
                raise URLError('Warning: Still experiencing URL Error %s. '
                               'After trying 10 times, the error remains. '
                               'Quitting for now, but you can try again later.'
                               % (e.reason,))

        except Exception as e:
            print(e)
            print('Warning: Encountered an unknown error when downloading %s '
                  'in download_yahoo_data.download_data' % (tsid,))

    url_obj = download_data(url_string)

    column_names = ['date', 'open', 'high', 'low', 'close', 'volume',
                    'adj_close']

    try:
        raw_df = pd.read_csv(url_obj, index_col=False, names=column_names,
                             encoding='utf-8')
    except IndexError:
        raw_df = pd.DataFrame()
    except OSError:
        # Occurs when the url_obj is None, meaning the url returned a 404 error
        raw_df = pd.DataFrame()

    if len(raw_df.index) > 0:
        # Data successfully downloaded; check to see if code was on the list
        codes_wo_data_df = pd.read_csv(csv_out, index_col=False)
        if len(codes_wo_data_df.loc[codes_wo_data_df['tsid'] == tsid]) > 0:
            # This tsid now has data whereas it didn't on that last run.
            #   Remove the code from the DataFrame
            wo_data_df = codes_wo_data_df[codes_wo_data_df.tsid != tsid]
            # Remove any duplicates (keeping the latest) and save to a CSV
            clean_wo_data_df = wo_data_df.drop_duplicates(subset='tsid',
                                                          keep='last')
            clean_wo_data_df.to_csv(csv_out, index=False)
            if verbose:
                print('%s was removed from the wo_data CSV file since data '
                      'was available for download.' % (tsid,))
    else:
        # There is no price data for this code; add to CSV file via DataFrame
        try:
            codes_wo_data_df = pd.read_csv(csv_out, index_col=False)
            cur_date = datetime.now().isoformat()
            if len(codes_wo_data_df.loc[codes_wo_data_df['tsid'] == tsid]) > 0:
                # The code already exists within the CSV, so update the date
                codes_wo_data_df.set_value(codes_wo_data_df['tsid'] == tsid,
                                           'date_tried', cur_date)
                # Remove any duplicates (keeping the latest) and save to a CSV
                clean_wo_data_df = \
                    codes_wo_data_df.drop_duplicates(subset='tsid', keep='last')
                clean_wo_data_df.to_csv(csv_out, index=False)
                if verbose:
                    print('%s still did not have data. Date tried was updated '
                          'in the wo_data CSV file.' % (tsid,))
            else:
                # The code does not exists within the CSV, so create and append
                #   it to the CSV file. Do this via a DataFrame to CSV append
                no_data_df = pd.DataFrame(data=[(tsid, cur_date)],
                                          columns=['tsid', 'date_tried'])
                with open(csv_out, 'a') as f:
                    no_data_df.to_csv(f, mode='a', header=False, index=False)
                if verbose:
                    print('%s did not have data, thus it was added to the '
                          'wo_data CSV file.' % (tsid,))
        except Exception as e:
            print('Error occurred when trying to update %s CSV data for %s' %
                  (csv_out, tsid))
            print(e)

        # Return an empty DF; DataExtraction class will be able to handle it
        return pd.DataFrame()

    # Removes the column headers from data download
    raw_df = raw_df[1:]

    raw_df['date'] = raw_df.apply(date_to_iso, axis=1, args=('date',))
    # raw_df.insert(0, 'tsid', tsid)
    raw_df.insert(len(raw_df.columns), 'updated_date',
                  datetime.now().isoformat())

    # Remove the adjusted close column since this is calculated manually
    raw_df.drop('adj_close', axis=1, inplace=True)

    return raw_df


def download_csidata_factsheet(db_url, data_type, exchange_id=None,
                               data_format='csv'):
    """ Downloads the CSV factsheet for the provided data_type (stocks,
    commodities, currencies, etc.). A DataFrame is returned.

    http://www.csidata.com/factsheets.php?type=stock&format=csv

    :param db_url: String of the url root for the CSI Data website
    :param data_type: String of the data to download
    :param exchange_id: None or integer of the specific exchange to download
    :param data_format: String of the type of file that should be returned.
        Default as a CSV
    :return:
    """

    url_string = db_url + 'type=' + data_type + '&format=' + data_format
    if exchange_id:
        url_string += '&exchangeid=' + exchange_id

    download_try = 0

    def download_data(url, download_try):
        """ Downloads the data from the url provided.

        :param url: String that contains the url of the data to download.
        :param download_try: Integer of the number of attempts to download data.
        :return: A CSV file as a url object
        """

        download_try += 1
        try:
            # Download the data
            return urlopen(url)

        except HTTPError as e:
            if str(e) == 'HTTP Error 403: Forbidden':
                raise OSError('HTTPError %s: Reached API call limit. Make the '
                              'RateLimit more restrictive.' % (e.reason,))
            elif str(e) == 'HTTP Error 404: Not Found':
                raise OSError('HTTPError %s: %s not found' %
                              (e.reason, data_type))
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
            elif str(e) == 'HTTP Error 502: Bad Gateway':
                if download_try <= 10:
                    print('HTTPError %s: Encountered a bad gateway with the '
                          'server. Maybe the network is down. Will sleep for '
                          '5 minutes'
                          % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 times, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 503: Service Unavailable':
                # Received this HTTP Error after 2000 queries. Browser showed
                #   captch message upon loading url.
                if download_try <= 10:
                    print('HTTPError %s: Server is currently unavailable. '
                          'Maybe the network is down or the server is blocking '
                          'you. Will sleep for 5 minutes...' % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. '
                                  'Quitting for now.' % (e.reason,))
            elif str(e) == 'HTTP Error 504: GATEWAY_TIMEOUT':
                if download_try <= 10:
                    print('HTTPError %s: Server connection timed out. Maybe '
                          'the network is down. Will sleep for 5 minutes'
                          % (e.reason,))
                    time.sleep(5 * 60)
                    download_data(url, download_try)
                else:
                    raise OSError('HTTPError %s: Server is currently '
                                  'unavailable. After trying 10 time, the '
                                  'download was still not successful. Quitting '
                                  'for now.' % (e.reason,))
            else:
                print('Base URL used: %s' % (url,))
                raise OSError('HTTPError %s: Unknown error when downloading %s'
                              % (e.reason, data_type))

        except URLError as e:
            if download_try <= 10:
                print('Warning: Experienced URL Error %s. Program will '
                      'sleep for 5 minutes and will then try again...' %
                      (e.reason,))
                time.sleep(5 * 60)
                download_data(url, download_try)
            else:
                raise URLError('Warning: Still experiencing URL Error %s. '
                               'After trying 10 times, the error remains. '
                               'Quitting for now, but you can try again later.'
                               % (e.reason,))

        except Exception as e:
            print(e)
            raise OSError('Warning: Encountered an unknown error when '
                          'downloading %s in download_data in download.py' %
                          (data_type,))

    def datetime_to_iso(row, column):
        """
        Change the default date format of "YYYY-MM-DD" to an ISO 8601 format
        """
        raw_date = row[column]
        try:
            raw_date_obj = datetime.strptime(raw_date, '%Y-%m-%d').isoformat()
        except TypeError:   # Occurs if there is no date provided ("nan")
            raw_date_obj = None
        return raw_date_obj

    csv_file = download_data(url_string, download_try)

    try:
        df = pd.read_csv(csv_file, encoding='latin_1', low_memory=False)

        # Rename column headers to a standardized format
        df.rename(columns={'CsiNumber': 'csi_number', 'Symbol': 'symbol',
                           'Name': 'name', 'Exchange': 'exchange',
                           'IsActive': 'is_active', 'StartDate': 'start_date',
                           'EndDate': 'end_date', 'Sector': 'sector',
                           'Industry': 'industry',
                           'ConversionFactor': 'conversion_factor',
                           'SwitchCfDate': 'switch_cf_date',
                           'PreSwitchCf': 'pre_switch_cf',
                           'LastVolume': 'last_volume', 'Type': 'type',
                           'ChildExchange': 'child_exchange',
                           'Currency': 'currency'},
                  inplace=True)

        if data_type == 'stock':
            df['start_date'] = df.apply(datetime_to_iso, axis=1,
                                        args=('start_date',))
            df['end_date'] = df.apply(datetime_to_iso, axis=1,
                                      args=('end_date',))
            df['switch_cf_date'] = df.apply(datetime_to_iso, axis=1,
                                            args=('switch_cf_date',))

    except Exception as e:
        print('Error occurred when processing CSI %s data in '
              'download_csidata_factsheet' % data_type)
        print(e)
        return pd.DataFrame()

    # df.insert(len(df.columns), 'symbology_source', 'CSI_Data')
    df.insert(len(df.columns), 'created_date', datetime.now().isoformat())
    df.insert(len(df.columns), 'updated_date', datetime.now().isoformat())

    return df


if __name__ == '__main__':

    output_dir = 'C:/Users/Josh/Desktop/'

    url_root = 'http://www.csidata.com/factsheets.php?'
    csi_data_type = 'commodity'     # commodity, stock
    csi_exchange_id = '113'     # 113, 89
    df1 = download_csidata_factsheet(url_root, csi_data_type, csi_exchange_id)

    print(df1.head(10))
