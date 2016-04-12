import csv
from datetime import datetime, timedelta
import pandas as pd
import re
import sqlite3
import time

from download import QuandlDownload, download_google_data, \
    download_yahoo_data, download_csidata_factsheet
from utilities.database_queries import df_to_sql, delete_sql_table_rows, \
    retrieve_data_vendor_id, query_codes, query_last_price, query_q_codes
from utilities.date_conversions import dt_from_iso
from utilities.multithread import multithread

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


class QuandlCodeExtract(object):

    def __init__(self, db_location, quandl_token, database_list, database_url,
                 update_range, threads):
        self.db_location = db_location
        self.quandl_token = quandl_token
        self.db_list = database_list
        self.db_url = database_url
        self.update_range = update_range
        self.threads = threads

        # Rate limiter parameters based on Quandl API limitations
        rate = 2000
        period_sec = 600
        self.min_interval = float((period_sec/rate)*threads)

        self.main()

    def main(self):

        extractor_start = time.time()

        # Determine if any Quandl data sets never finished downloading all of
        #   their Quandl Codes. A complete data set has a page_num of -2 for all
        #   its Quandl Codes. Otherwise, a real number will exist for the items.
        data_sets = self.query_last_download_pg()

        # The quandl_codes table is empty, so all codes should be downloaded
        if len(data_sets) == 0:
            print('Downloading all Quandl Codes for the following databases: %s'
                  % (", ".join([db for db in self.db_list])))

            for db_name in self.db_list:
                self.extractor(db_name)

            print('Finished downloading Quandl Codes from the %i databases, '
                  'taking %0.1f seconds' %
                  # '{:,}'.format(len(q_code_df.index))
                  (len(self.db_list), time.time() - extractor_start))

        # Codes already exists in the quandl_codes table, so this will determine
        #   whether the codes need to be updated or if the download was
        #   incomplete.
        else:

            data_sets['updated_date'] = data_sets.apply(dt_from_iso, axis=1,
                                                        args=('updated_date',))

            # This for loop only provides program info; doesn't format anything
            for row in range(len(data_sets)):
                existing_vendor = data_sets.loc[row, 'data_vendor']
                # existing_page_num = data_sets.loc[row, 'page_num']
                existing_updated_date = data_sets.loc[row, 'updated_date']
                if existing_vendor not in self.db_list:
                    print('FLAG: Did not update the %s data set because it was '
                          'not included in the database_list variable. The '
                          'last update was on %s' %
                          (existing_vendor,
                           existing_updated_date.strftime('%Y-%m-%d')))

            for data_vendor in self.db_list:

                # Does the data vendor already exists? If not go to extractor.
                vendor_exist = data_sets.loc[data_sets['data_vendor'] ==
                                             data_vendor]
                if len(vendor_exist) == 0:
                    self.extractor(data_vendor)

                # Data vendor already exist. Check for other criteria
                else:
                    page_num = data_sets.loc[data_sets['data_vendor'] ==
                                             data_vendor, 'page_num']
                    page_num = page_num.iloc[0]
                    updated_date = data_sets.loc[data_sets['data_vendor'] ==
                                                 data_vendor, 'updated_date']
                    updated_date = updated_date.iloc[0]

                    beg_date_obj = (datetime.now() - timedelta(
                                    days=self.update_range))
                    # beg_date = beg_date_obj.strftime('%Y-%m-%d')

                    # Were the Quandl Codes downloaded within the update_range
                    #   and is the highest page_num for that data set greater
                    #   than zero?
                    if (updated_date > beg_date_obj) and page_num > 0:
                        print('The %s data set needs to finish downloading. '
                              'Will continue downloading codes starting from '
                              'page %i' % (data_vendor, page_num + 1))
                        self.extractor(data_vendor, page_num + 1)

                    # Quandl Codes have been updated in more days than update
                    #   range, thus the entire data set's codes should be
                    #   refreshed.
                    elif updated_date < beg_date_obj:
                        print('The Quandl Codes for %s were downloaded more '
                              'than %i days ago, the maximum update range. '
                              'New codes will now be downloaded to replace the '
                              'old codes' % (data_vendor, self.update_range))
                        query = ("""DELETE FROM quandl_codes
                                    WHERE data_vendor='%s'""" % (data_vendor,))
                        del_success = delete_sql_table_rows(self.db_location,
                                                            query,
                                                            'quandl_codes',
                                                            'UNUSED')
                        if del_success == 'success':
                            self.extractor(data_vendor)
                        elif del_success == 'failure':
                            continue
                    else:
                        print('All of the Quandl Codes for %s are up to date.'
                              % (data_vendor,))

    def query_last_download_pg(self):
        """ Find Quandl data sets that did not finished downloading all of their
        Quandl Codes. It is assumed that if only part of the data set was
        downloaded, the remainder of the download should continue with the
        last downloaded page. Uses the page_num column to mark this.

        :return: A DataFrame with the Quandl data set and the max page_num.
        """

        try:
            conn = sqlite3.connect(self.db_location)
            with conn:
                df = pd.read_sql("SELECT data_vendor, "
                                 "MAX(page_num) AS page_num, updated_date "
                                 "FROM  quandl_codes "
                                 "GROUP BY data_vendor ", conn)
                return df
        except sqlite3.Error as e:
            print('Error when trying to connect to the database quandl_codes '
                  'table in query_latest_codes in QuandlCodeExtract.')
            print(e)

    def extractor(self, db_name, page_num=1):
        """ For every database passed through, each page number will be
        incremented through, saving the downloaded data to the SQL table. If no
        data is returned from the download function, then all tables have been
        downloaded for that particular database.

        :param db_name: A string of the name that Quandl uses for the database
        :param page_num: An optional integer to indicate the page number to
        start on. If no page_num is provided, it is assumed that the entire
        data set needs to be downloaded. Otherwise, it is assumed that the
        data set download was interrupted and will continue downloading codes.
        :return: Nothing. All data is saved to the SQL quandl_codes table.
        """

        dl_csv_start_time = time.time()
        next_page = True
        while next_page:

            # Rate limit this function with non-reactive timer
            time.sleep(self.min_interval)

            quandl_download = QuandlDownload(self.quandl_token, self.db_url)
            db_pg_df = quandl_download.download_quandl_codes(db_name, page_num)

            if len(db_pg_df.index) == 0:  # finished downloading all pages
                next_page = False
            else:
                try:
                    db_pg_df.insert(0, 'data_vendor', 'Unknown')
                    db_pg_df.insert(1, 'data', 'Unknown')
                    db_pg_df.insert(2, 'component', 'Unknown')
                    db_pg_df.insert(3, 'period', 'Unknown')
                except Exception as e:
                    print('The columns for component, period, document and '
                          'data_vendor are already created.')
                    print(e)

                if db_name in ('EIA', 'JODI', 'ZFA', 'ZFB', 'RAYMOND'):
                    clean_df = self.process_3_item_q_codes(db_pg_df)
                elif db_name in ('GOOG', 'YAHOO', 'FINRA'):
                    clean_df = self.process_2_item_q_codes(db_pg_df)
                else:       # 'WIKI', 'EIA', 'ZEP', 'EOD', 'CURRFX'
                    clean_df = self.process_1_item_q_codes(db_pg_df)

                df_to_sql(clean_df, self.db_location, 'quandl_codes', 'append',
                          db_name)

                if page_num % 100 == 0:
                    print('Still downloading %s codes. Just finished page '
                          '%i...' % (db_name, page_num))
                page_num += 1

        # Remove duplicate q_codes
        conn = sqlite3.connect(self.db_location)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""DELETE FROM quandl_codes
                               WHERE rowid NOT IN
                               (SELECT min(rowid)
                               FROM quandl_codes
                               GROUP BY q_code)""")
                print('Successfully removed all duplicate q_codes from '
                      'quandl_codes')
        except sqlite3.Error as e:
            conn.rollback()
            print(e)
            print('Error: Not able to remove duplicate q_codes in the %s data '
                  'set while running the extractor.' % (db_name,))
        except conn.OperationalError:
            print('Unable to connect to the SQL Database in extractor. Make '
                  'sure the database address/name are correct.')
        except Exception as e:
            print('Error: An unknown issue occurred when removing all '
                  'duplicate q_codes in the %s data set.' % (db_name,))
            print(e)

        # Change the data set page_num variable to -2, indicating it finished
        conn = sqlite3.connect(self.db_location)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""UPDATE quandl_codes
                               SET page_num=-2
                               WHERE data_vendor=?""", (db_name,))
                print('Successfully updated %s codes with final page_num '
                      'variable.' % (db_name,))
        except sqlite3.Error as e:
            conn.rollback()
            print(e)
            print('Error: Not able to update the page_num rows for codes in '
                  'the %s data set while running the extractor.' % (db_name,))
        except conn.OperationalError:
            print('Unable to connect to the SQL Database in extractor. Make '
                  'sure the database address/name are correct.')
        except Exception as e:
            print('Error: An unknown issue occurred when changing the page_num'
                  'rows for codes in the %s data set.' % (db_name,))
            print(e)

        print('The %s database took %0.1f seconds to download'
              % (db_name, time.time() - dl_csv_start_time))

    @staticmethod
    def process_3_item_q_codes(df):

        # Each EIA q_code structure: EIA/[document]_[component]_[period]
        #   NOTE: EIA/IES database does not follow this structure
        # JODI q_code structure: JODI/[type]_[product][flow][unit]_[country]

        def strip_q_code(row, column):
            q_code = row['q_code']
            if column == 'data_vendor':
                return q_code[:q_code.find('/')]
            elif column == 'data':
                # ToDo: Find a way to include items with an underscore in name
                # Example: 'EIA/AEO_2014_{Component}_A' --> 'AEO_2014'
                # If block handles 1 item codes that are in 3 item data sets
                if q_code.find('_') != -1:
                    return q_code[q_code.find('/') + 1:q_code.find('_')]
                else:
                    return 'Unknown'
            elif column == 'component':
                # If block handles 1 item codes that are in 3 item data sets
                if q_code.find('_') != -1:
                    return q_code[q_code.find('_') + 1:q_code.rfind('_')]
                else:
                    return q_code[q_code.find('/') + 1:]
            elif column == 'period':
                # If block handles 1 item codes that are in 3 item data sets
                if q_code.find('_') != -1:
                    return q_code[q_code.rfind('_') + 1:]
                else:
                    return 'Unknown'
            else:
                print('Error: Unknown column [%s] passed in to strip_q_code in '
                      'process_3_item_q_codes' % (column,))

        df['data_vendor'] = df.apply(strip_q_code,
                                     axis=1, args=('data_vendor',))
        df['data'] = df.apply(strip_q_code, axis=1, args=('data',))
        df['component'] = df.apply(strip_q_code, axis=1, args=('component',))
        df['period'] = df.apply(strip_q_code, axis=1, args=('period',))
        return df

    @staticmethod
    def process_2_item_q_codes(df):

        def strip_q_code(row, column):
            q_code = row['q_code']
            if column == 'data_vendor':
                return q_code[:q_code.find('/')]
            elif column == 'data':
                # data -> exchange
                # If block handles 1 item codes that are in 2 item data sets
                if q_code.find('_') != -1:
                    return q_code[q_code.find('/') + 1:q_code.find('_')]
                else:
                    return 'Unknown'
            elif column == 'component':
                # component -> ticker
                # If block handles 1 item codes that are in 2 item data sets
                if q_code.find('_') != -1:
                    return q_code[q_code.find('_') + 1:]
                else:
                    return q_code[q_code.find('/') + 1:]
            else:
                print('Error: Unknown column [%s] passed in to strip_q_code in '
                      'process_2_item_q_codes' % (column,))

        df['data_vendor'] = df.apply(strip_q_code,
                                     axis=1, args=('data_vendor',))
        df['data'] = df.apply(strip_q_code, axis=1, args=('data',))
        df['component'] = df.apply(strip_q_code, axis=1, args=('component',))
        return df

    @staticmethod
    def process_1_item_q_codes(df):

        def strip_q_code(row, column):
            q_code = row['q_code']
            if column == 'data_vendor':
                return q_code[:q_code.find('/')]
            elif column == 'component':
                return q_code[q_code.find('/') + 1:]
            else:
                print('Error: Unknown column [%s] passed in to strip_q_code in '
                      'process_1_item_q_codes' % (column,))

        df['data_vendor'] = df.apply(strip_q_code, axis=1,
                                     args=('data_vendor',))
        df['component'] = df.apply(strip_q_code, axis=1, args=('component',))
        return df


class QuandlDataExtraction(object):

    def __init__(self, db_location, quandl_token, db_url, download_selection,
                 redownload_time, data_process, days_back, threads, table,
                 verbose=False):
        """
        :param db_location: String of the database directory location
        :param quandl_token: String of the Quandl API token
        :param db_url: List of Quandl API url components
        :param download_selection: String indicating what selection of codes
            should be downloaded from Quandl
        :param redownload_time: Integer of the time in seconds before the
            data can be downloaded again. Allows the extractor to be restarted
            without downloading the same data again.
        :param data_process: String of how the new values will interact with
            the existing database table values. Options include either
            'append' or 'replace'.
        :param days_back: Integer of the number of days where any existing
            data should be replaced with newer prices
        :param threads: Integer of the number of threads the current process
            is using; used for rate limiter
        :param table: String indicating which table the DataFrame should be
            put into.
        :param verbose: Boolean of whether debugging prints should occur.
        """

        self.db_location = db_location
        self.quandl_token = quandl_token
        self.db_url = db_url
        self.download_selection = download_selection
        self.redownload_time = redownload_time
        self.data_process = data_process
        self.days_back = days_back
        self.threads = threads
        self.table = table
        self.verbose = verbose

        # Rate limiter parameters based on Quandl API limitations
        rate = 2000
        period_sec = 600
        self.min_interval = float((period_sec/rate)*threads)

        self.csv_wo_data = 'load_tables/quandl_' + self.table + '_wo_data.csv'

        # Retrieve the Quandl data vendor IDs. Not able to use a list of all
        #   Quandl data vendor IDs because that prevents data being downloaded
        #   for the same tsid but from different Quandl sources (ie wiki % goog)
        # vendor_id = retrieve_data_vendor_id(
        #     db_location=self.db_location, name='Quandl_%')
        if self.download_selection[:4] == 'wiki':
            self.vendor_id = retrieve_data_vendor_id(
                db_location=self.db_location, name='Quandl_WIKI')
        elif self.download_selection[:4] == 'goog':
            self.vendor_id = retrieve_data_vendor_id(
                db_location=self.db_location, name='Quandl_GOOG')
        else:
            raise NotImplementedError('The %s Quandl source is not implemented '
                                      'in the init within QuandlDataExtraction'
                                      % self.download_selection)

        print('Retrieving dates of the last price per ticker for all Quandl '
              'values.')
        # Creates a DataFrame with the last price for each Quandl code
        self.latest_prices = query_last_price(db_location=self.db_location,
                                              table=self.table,
                                              vendor_id=self.vendor_id)

        self.main()

    def main(self):
        """
        The main QuandlDataExtraction method is used to execute subsequent
        methods in the correct order.
        """

        start_time = time.time()

        print('Analyzing the Quandl Codes that will be downloaded...')
        # Create a list of securities to download
        q_code_df = query_q_codes(db_location=self.db_location,
                                  download_selection=self.download_selection)
        # Get DF of selected codes plus when (if ever) they were last updated
        q_codes_df = pd.merge(q_code_df, self.latest_prices,
                              left_on='tsid', right_index=True, how='left')
        # Sort the DF with un-downloaded items first, then based on last update
        try:
            # df.sort_values introduced in 0.17.0
            q_codes_df.sort_values('updated_date', axis=0, ascending=True,
                                   na_position='first', inplace=True)
        except:
            # df.sort() depreciated in 0.17.0
            q_codes_df.sort('updated_date', ascending=True, na_position='first',
                            inplace=True)

        try:
            # Load the codes that did not have data from the last extractor run
            codes_wo_data_df = pd.read_csv(self.csv_wo_data, index_col=False)
            # Exclude these codes that are within the 15 day re-download period
            beg_date_ob_wo_data = (datetime.utcnow() - timedelta(days=15))
            exclude_codes_df = codes_wo_data_df[codes_wo_data_df['date_tried'] >
                                                beg_date_ob_wo_data.isoformat()]
            # Change DF to a list of only the q_codes
            list_to_exclude = exclude_codes_df['q_code'].values.flatten()
            # Create a temp DF from q_codes_df with only the codes to exclude
            q_codes_to_exclude = q_codes_df['q_code'].isin(list_to_exclude)
            # From the main DF, remove any of the codes that are in the temp DF
            # NOTE: Might be able to just use exclude_codes_df instead
            q_codes_df = q_codes_df[~q_codes_to_exclude]
        except IOError:
            # The CSV file doesn't exist; create a file that will be appended to
            with open(self.csv_wo_data, 'a', newline='') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(('q_code', 'date_tried'))

        # The cut-off time for when code data can be re-downloaded
        beg_date_obj = (datetime.utcnow() -
                        timedelta(seconds=self.redownload_time))
        # For the final download list, only include new and non-recent codes
        q_codes_final = q_codes_df[(q_codes_df['updated_date'] < beg_date_obj) |
                                   (q_codes_df['updated_date'].isnull())]

        # Change the DF to a list of tuples containing tsid and q_code
        q_code_set = q_codes_final[['tsid', 'q_code']]
        q_code_list = [tuple(x) for x in q_code_set.values]

        # Inform the user how many codes will be updated
        dl_codes = len(q_codes_final.index)
        total_codes = len(q_codes_df.index)
        print('%s Quandl Codes out of %s requested codes will be downloaded.\n'
              '%s codes were last updated within the %s second limit.'
              % ('{:,}'.format(dl_codes), '{:,}'.format(total_codes),
                 '{:,}'.format(total_codes - dl_codes),
                 '{:,}'.format(self.redownload_time)))

        """ This runs the program with no multiprocessing or threading.
        To run, make sure to comment out all pool processes.
        Takes about 55 seconds for 10 tickers; 5.5 seconds per ticker. """
        # [self.extractor(q_code) for q_code in q_code_list]

        """ This runs the program with multiprocessing or threading.
        Comment and uncomment the type of multiprocessing in the
        multi-thread function above to change the type. Change the number
        of threads below to alter the speed of the downloads. If the
        query runs out of items, try lowering the number of threads.
        22 threads -> 932.65 seconds"""
        multithread(self.extractor, q_code_list, threads=self.threads)

        print('The price extraction took %0.2f seconds to complete' %
              (time.time() - start_time))

    def extractor(self, codes):
        """ Takes the Quandl ticker quote, downloads the historical data,
        and then saves the data into the SQLite database.

        :param codes: String of tuples containing the tsid and Quandl code
        :return: Nothing. It saves the price data in the SQLite Database.
        """

        main_time_start = time.time()

        tsid = codes[0]
        q_code = codes[1]

        # Rate limit this function with non-reactive timer
        time.sleep(self.min_interval)

        quandl_download = QuandlDownload(self.quandl_token, self.db_url)

        # The ticker has no prior price; add all the downloaded data
        if tsid not in self.latest_prices.index:

            # ToDo: Use the start date from CSI within this function for stocks
            clean_data = quandl_download.download_quandl_data(
                q_code=q_code, csv_out=self.csv_wo_data)

            # There is not new data, so do nothing to the database
            if len(clean_data.index) == 0:
                print('No update for %s | %0.1f seconds' %
                      (q_code, time.time() - main_time_start))
            # There is new data to add to the database
            else:
                # Find the data vendor of the q_code; add it to the DataFrame
                # vendor_name = 'Quandl_' + q_code[:q_code.find('/')]
                # data_vendor = retrieve_data_vendor_id(
                #     db_location=self.db_location, name=vendor_name)
                clean_data.insert(0, 'data_vendor_id', self.vendor_id)

                # Add the tsid into the DataFrame, and then remove the q_code
                clean_data.insert(1, 'tsid', tsid)
                clean_data.drop('q_code', axis=1, inplace=True)

                df_to_sql(clean_data, self.db_location, 'daily_prices',
                          'append', tsid)
                print('Updated %s | %0.1f seconds' %
                      (q_code, time.time() - main_time_start))

        # The pricing database has prior values; append/replace new data points
        else:
            try:
                last_date = self.latest_prices.loc[tsid, 'date']

                # This will only download the data for the past x days.
                if self.data_process == 'replace' and self.days_back:
                    beg_date_obj = (last_date - timedelta(days=self.days_back))
                    # YYYY-MM-DD format needed for Quandl API download
                    beg_date = beg_date_obj.strftime('%Y-%m-%d')
                    clean_data = quandl_download.download_quandl_data(
                        q_code=q_code, csv_out=self.csv_wo_data,
                        beg_date=beg_date)

                # This will download the entire data set, but only keep new
                #   data after the latest existing data point.
                else:
                    raw_data = quandl_download.download_quandl_data(
                        q_code=q_code, csv_out=self.csv_wo_data)
                    # DataFrame of only the new data
                    clean_data = raw_data[raw_data.date > last_date.isoformat()]

            except Exception as e:
                print('Failed to determine what data is new for %s in extractor'
                      % q_code)
                print(e)
                return

            # There is not new data, so do nothing to the database
            if len(clean_data.index) == 0:
                print('No update for %s | %0.1f seconds' %
                      (q_code, time.time() - main_time_start))
            # There is new data to add to the database
            else:
                # Find the data vendor of the q_code; add it to the DataFrame
                # vendor_name = 'Quandl_' + q_code[:q_code.find('/')]
                # data_vendor = retrieve_data_vendor_id(
                #     db_location=self.db_location, name=vendor_name)
                clean_data.insert(0, 'data_vendor_id', self.vendor_id)

                # Add the tsid into the DataFrame, and then remove the q_code
                clean_data.insert(1, 'tsid', tsid)
                clean_data.drop('q_code', axis=1, inplace=True)

                # If replacing existing data, delete the overlapping data points
                if self.data_process == 'replace' and self.days_back:
                    # Data should be newest to oldest; gets the oldest date, as
                    #   any date between that and the latest date need to be
                    #   deleted before the new data can be added.
                    first_date_iso = clean_data['date'].min()
                    query = ("""DELETE FROM daily_prices
                                WHERE tsid='%s'
                                AND date>='%s'""" % (tsid, first_date_iso))

                    del_success = 'failure'
                    retry_count = 5
                    while retry_count > 0:
                        del_success = delete_sql_table_rows(
                            db_location=self.db_location, query=query,
                            table='daily_prices', tsid=tsid)

                        if del_success == 'failure':
                            retry_count -= 1
                        elif del_success == 'success':
                            break

                    # Not able to delete existing data, so skip ticker for now
                    if del_success == 'failure':
                        return

                # Append the new data to the end, regardless of replacement
                df_to_sql(clean_data, self.db_location, 'daily_prices',
                          'append', tsid)
                print('Updated %s | %0.1f seconds' %
                      (q_code, time.time() - main_time_start))


class GoogleFinanceDataExtraction(object):

    def __init__(self, db_location, db_url, download_selection, redownload_time,
                 data_process, days_back, threads, table, verbose=True):
        """
        :param db_location: String of the database directory location
        :param db_url: Dictionary of Google Finance url components
        :param download_selection: String indicating what selection of codes
            should be downloaded from Google
        :param redownload_time: Integer of the time in seconds before the
            data can be downloaded again. Allows the extractor to be restarted
            without downloading the same data again.
        :param data_process: String of how the new values will interact with
            the existing database table values. Options include either
            'append' or 'replace'.
        :param days_back: Integer of the number of days where any existing
            data should be replaced with newer prices
        :param threads: Integer of the number of threads the current process
            is using; used for rate limiter
        :param table: String indicating which table the DataFrame should be
            put into.
        :param verbose: Boolean of whether debugging prints should occur.
        """

        self.db_location = db_location
        self.db_url = db_url
        self.download_selection = download_selection
        self.redownload_time = redownload_time
        self.data_process = data_process
        self.days_back = days_back
        self.threads = threads
        self.table = table
        self.verbose = verbose

        # Rate limiter parameters based on guessed Google Finance limitations
        # Received captcha after about 2000 queries
        rate = 70
        period_sec = 60
        self.min_interval = float((period_sec/rate)*threads)

        self.vendor_id = retrieve_data_vendor_id(db_location=self.db_location,
                                                 name='Google_Finance')
        self.csv_wo_data = 'load_tables/goog_' + self.table + '_wo_data.csv'

        print('Retrieving dates for the last Google prices per ticker...')
        # Creates a DataFrame with the last price for each security
        self.latest_prices = query_last_price(db_location=self.db_location,
                                              table=self.table,
                                              vendor_id=self.vendor_id)

        # Build a DataFrame with all the exchange symbols
        self.exchanges_df = self.query_exchanges()

        self.main()

    def main(self):
        """
        The main GoogleFinanceDataExtraction method is used to execute
        subsequent methods in the correct order.
        """

        start_time = time.time()

        print('Analyzing the tsid codes that will be downloaded...')
        # Create a list of securities to download
        code_df = query_codes(db_location=self.db_location,
                              download_selection=self.download_selection)
        # Get DF of selected codes plus when (if ever) they were last updated
        codes_df = pd.merge(code_df, self.latest_prices, left_on='tsid',
                            right_index=True, how='left')
        # Sort the DF with un-downloaded items first, then based on last update
        try:
            # df.sort_values introduced in 0.17.0
            codes_df.sort_values('updated_date', axis=0, ascending=True,
                                 na_position='first', inplace=True)
        except:
            print('extractor.py is using the depreciated DataFrame sort '
                  'function. You should update Pandas. (line 860)')
            # df.sort() was depreciated in 0.17.0
            codes_df.sort('updated_date', ascending=True, na_position='first',
                          inplace=True)

        try:
            # Load the codes that did not have data from the last extractor run
            # for this interval
            codes_wo_data_df = pd.read_csv(self.csv_wo_data, index_col=False)
            # Exclude these codes that are within a 15 day period
            beg_date_ob_wo_data = (datetime.utcnow() - timedelta(days=15))
            exclude_codes_df = codes_wo_data_df[codes_wo_data_df['date_tried'] >
                                                beg_date_ob_wo_data.isoformat()]
            # Change DF to a list of only the codes
            list_to_exclude = exclude_codes_df['tsid'].values.flatten()
            # Create a temp DF from codes_df with only the codes to exclude
            codes_to_exclude = codes_df['tsid'].isin(list_to_exclude)
            # From the main DF, remove any of the codes that are in the temp DF
            # NOTE: Might be able to just use exclude_codes_df instead
            codes_df = codes_df[~codes_to_exclude]
        except IOError:
            # The CSV file doesn't exist; create a file that will be appended to
            with open(self.csv_wo_data, 'a', newline='') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(('tsid', 'date_tried'))

        # The cut-off time for when code data can be re-downloaded
        beg_date_obj = (datetime.utcnow() -
                        timedelta(seconds=self.redownload_time))
        # Final download list should include both new/null and non-recent codes
        codes_final = codes_df[(codes_df['updated_date'] < beg_date_obj) |
                               (codes_df['updated_date'].isnull())]

        # Change the DF to a list
        code_list = codes_final['tsid'].values.flatten()

        # Inform the user how many codes will be updated
        dl_codes = len(codes_final.index)
        total_codes = len(codes_df.index)
        print('%s tsid codes out of %s requested codes will have Google '
              'finance %s data downloaded.\n%s codes were last updated within '
              'the %s second limit.'
              % ('{:,}'.format(dl_codes), '{:,}'.format(total_codes),
                 self.table, '{:,}'.format(total_codes - dl_codes),
                 '{:,}'.format(self.redownload_time)))

        """This runs the program with no multiprocessing or threading.
        To run, make sure to comment out all pool processes below.
        Takes about 55 seconds for 10 tickers; 5.5 seconds per ticker."""
        # [self.extractor(tsid) for tsid in code_list]

        """This runs the program with multiprocessing or threading.
        Comment and uncomment the type of multiprocessing in the
        multi-thread function above to change the type. Change the number
        of threads below to alter the speed of the downloads. If the
        query runs out of items, try lowering the number of threads."""
        multithread(self.extractor, code_list, threads=self.threads)

        print('The price extraction took %0.2f seconds to complete' %
              (time.time() - start_time))

    def query_exchanges(self):
        """ Retrieve the exchange symbols for goog and tsid, which will be used
        to translate the tsid symbols to goog symbols. Remove the symbols for
        which there are no goog symbols.

        :return: DataFrame with exchange symbols
        """

        conn = sqlite3.connect(self.db_location)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""SELECT symbol, goog_symbol, tsid_symbol
                            FROM exchange
                            WHERE goog_symbol NOT NULL
                            GROUP BY tsid_symbol""")
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=['symbol', 'goog_symbol',
                                                 'tsid_symbol'])
                return df
        except sqlite3.Error as e:
            print(e)
            raise SystemError('Failed to query the data from the exchange '
                              'table within query_exchanges in '
                              'GoogleFinanceExtraction')
        except conn.OperationalError:
            raise SystemError('Unable to connect to the SQL Database in '
                              'query_exchanges in GoogleFinanceExtraction. '
                              'Make sure the database address is correct.')
        except Exception as e:
            print(e)
            raise SystemError('Error: Unknown issue occurred in '
                              'query_exchanges in GoogleFinanceExtraction.')

    def extractor(self, tsid):
        """ Takes the tsid symbol, downloads the historical data, and then
        saves the data into the SQLite database.

        :param tsid: String of the tsid
        :return: Nothing. It saves the price data in the SQLite Database.
        """

        main_time_start = time.time()

        # Rate limit this function with non-reactive timer
        time.sleep(self.min_interval)

        # The ticker has no prior price; add all the downloaded data
        if tsid not in self.latest_prices.index:
            clean_data = download_google_data(db_url=self.db_url, tsid=tsid,
                                              exchanges_df=self.exchanges_df,
                                              csv_out=self.csv_wo_data)

            # There is no new data, so do nothing to the database
            if len(clean_data.index) == 0:
                if self.verbose:
                    print('No data for %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))
            # There is new data to add to the database
            else:
                clean_data.insert(0, 'data_vendor_id', self.vendor_id)

                df_to_sql(df=clean_data, db_location=self.db_location,
                          sql_table=self.table, exists='append', item=tsid)

                if self.verbose:
                    print('Updated %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))

        # The pricing database has prior values; append/replace new data points
        else:
            try:
                last_date = self.latest_prices.loc[tsid, 'date']
                raw_data = download_google_data(db_url=self.db_url, tsid=tsid,
                                                exchanges_df=self.exchanges_df,
                                                csv_out=self.csv_wo_data)

                # Only keep data that is after the days_back period
                if self.data_process == 'replace' and self.days_back:
                    beg_date = (last_date - timedelta(days=self.days_back))
                    clean_data = raw_data[raw_data.date > beg_date.isoformat()]

                # Only keep data that is after the latest existing data point
                else:
                    clean_data = raw_data[raw_data.date > last_date.isoformat()]
            except Exception as e:
                print('Failed to determine what data is new for %s in extractor'
                      % tsid)
                print(e)
                return

            # There is not new data, so do nothing to the database
            if len(clean_data.index) == 0:
                if self.verbose:
                    print('No update for %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))
            # There is new data to add to the database
            else:
                clean_data.insert(0, 'data_vendor_id', self.vendor_id)

                # If replacing existing data, delete the overlapping data points
                if self.data_process == 'replace' and self.days_back:
                    # Data should be oldest to newest; gets the oldest date, as
                    #   any date between that and the latest date need to be
                    #   deleted before the new data can be added.
                    first_date_iso = clean_data['date'].min()

                    query = ("""DELETE FROM %s
                                WHERE tsid='%s'
                                AND date>='%s'
                                AND data_vendor_id='%s'""" %
                             (self.table, tsid, first_date_iso, self.vendor_id))

                    del_success = 'failure'
                    retry_count = 5
                    while retry_count > 0:
                        del_success = delete_sql_table_rows(
                            db_location=self.db_location, query=query,
                            table=self.table, tsid=tsid)

                        if del_success == 'failure':
                            retry_count -= 1
                            time.sleep(5)
                        elif del_success == 'success':
                            break

                    # If unable to delete existing data, skip ticker
                    if del_success == 'failure':
                        if self.verbose:
                            print('Unable to delete existing data for %s '
                                  'in the GoogleFinanceDataExtraction. '
                                  'Skipping it for now.' % tsid)
                        return

                # Append the new data to the end, regardless of replacement
                df_to_sql(df=clean_data, db_location=self.db_location,
                          sql_table=self.table, exists='append', item=tsid)

                if self.verbose:
                    print('Updated %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))


class YahooFinanceDataExtraction(object):

    def __init__(self, db_location, db_url, download_selection, redownload_time,
                 data_process, days_back, threads, table, verbose=True):
        """
        :param db_location: String of the database directory location
        :param db_url: Dictionary of Yahoo Finance url components
        :param download_selection: String indicating what selection of codes
            should be downloaded from Yahoo
        :param redownload_time: Integer of the time in seconds before the
            data can be downloaded again. Allows the extractor to be restarted
            without downloading the same data again.
        :param data_process: String of how the new values will interact with
            the existing database table values. Options include either
            'append' or 'replace'.
        :param days_back: Integer of the number of days where any existing
            data should be replaced with newer prices
        :param threads: Integer of the number of threads the current process
            is using; used for rate limiter
        :param table: String indicating which table the DataFrame should be
            put into.
        :param verbose: Boolean of whether debugging prints should occur.
        """

        self.db_location = db_location
        self.db_url = db_url
        self.download_selection = download_selection
        self.redownload_time = redownload_time
        self.data_process = data_process
        self.days_back = days_back
        self.threads = threads
        self.table = table
        self.verbose = verbose

        # Rate limiter parameters based on guessed Google Finance limitations
        # Received captcha after about 2000 queries
        rate = 70
        period_sec = 60
        self.min_interval = float((period_sec / rate) * threads)

        self.vendor_id = retrieve_data_vendor_id(db_location=self.db_location,
                                                 name='Yahoo_Finance')
        self.csv_wo_data = 'load_tables/yahoo_' + self.table + '_wo_data.csv'

        print('Retrieving dates for the last Yahoo prices per ticker...')
        # Creates a DataFrame with the last price for each security
        self.latest_prices = query_last_price(db_location=self.db_location,
                                              table=self.table,
                                              vendor_id=self.vendor_id)

        # Build a DataFrame with all the exchange symbols
        self.exchanges_df = self.query_exchanges()

        self.main()

    def main(self):
        """
        The main YahooFinanceDataExtraction method is used to execute
        subsequent methods in the correct order.
        """

        start_time = time.time()

        print('Analyzing the tsid codes that will be downloaded...')
        # Create a list of tsids to download
        code_df = query_codes(db_location=self.db_location,
                              download_selection=self.download_selection)
        # Get DF of selected codes plus when (if ever) they were last updated
        codes_df = pd.merge(code_df, self.latest_prices, left_on='tsid',
                            right_index=True, how='left')
        # Sort the DF with un-downloaded items first, then based on last update
        try:
            # df.sort_values introduced in 0.17.0
            codes_df.sort_values('updated_date', axis=0, ascending=True,
                                 na_position='first', inplace=True)
        except:
            print('The YahooFinanceDataExtraction.main is using the '
                  'depreciated DataFrame sort function. Update Pandas.')
            # df.sort() was depreciated in 0.17.0
            codes_df.sort('updated_date', ascending=True, na_position='first',
                          inplace=True)

        try:
            # Load the codes that did not have data from the last extractor run
            #   for this interval
            codes_wo_data_df = pd.read_csv(self.csv_wo_data, index_col=False)
            # Exclude these codes that are within a 15 day period
            beg_date_ob_wo_data = (datetime.utcnow() - timedelta(days=15))
            exclude_codes_df = codes_wo_data_df[codes_wo_data_df['date_tried'] >
                                                beg_date_ob_wo_data.isoformat()]
            # Change DF to a list of only the codes
            list_to_exclude = exclude_codes_df['tsid'].values.flatten()
            # Create a temp DF from codes_df with only the codes to exclude
            codes_to_exclude = codes_df['tsid'].isin(list_to_exclude)
            # From the main DF, remove any of the codes that are in the temp DF
            # NOTE: Might be able to just use exclude_codes_df instead
            codes_df = codes_df[~codes_to_exclude]
        except IOError:
            # The CSV file doesn't exist; create a file that will be appended to
            with open(self.csv_wo_data, 'a', newline='') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(('tsid', 'date_tried'))

        # The cut-off time for when code data can be re-downloaded
        beg_date_obj = (datetime.utcnow() -
                        timedelta(seconds=self.redownload_time))
        # Final download list should include both new/null and non-recent codes
        codes_final = codes_df[(codes_df['updated_date'] < beg_date_obj) |
                               (codes_df['updated_date'].isnull())]

        # Change the DF to a list
        code_list = codes_final['tsid'].values.flatten()

        # Inform the user how many codes will be updated
        dl_codes = len(codes_final.index)
        total_codes = len(codes_df.index)
        print('%s tsid codes out of %s requested codes will have Yahoo '
              'finance %s data downloaded.\n%s codes were last updated within '
              'the %s second limit.'
              % ('{:,}'.format(dl_codes), '{:,}'.format(total_codes),
                 self.table, '{:,}'.format(total_codes - dl_codes),
                 '{:,}'.format(self.redownload_time)))

        """This runs the program with no multiprocessing or threading.
        To run, make sure to comment out all pool processes below.
        Takes about 55 seconds for 10 tickers; 5.5 seconds per ticker."""
        # [self.extractor(tsid) for tsid in code_list]

        """This runs the program with multiprocessing or threading.
        Comment and uncomment the type of multiprocessing in the
        multi-thread function above to change the type. Change the number
        of threads below to alter the speed of the downloads. If the
        query runs out of items, try lowering the number of threads."""
        multithread(self.extractor, code_list, threads=self.threads)

        print('The price extraction took %0.2f seconds to complete' %
              (time.time() - start_time))

    def query_exchanges(self):
        """ Retrieve the exchange symbols for yahoo and tsid, which will be used
        to translate the tsid symbols to yahoo symbols. Remove the symbols for
        which there are no yahoo symbols.

        :return: DataFrame with exchange symbols
        """

        conn = sqlite3.connect(self.db_location)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""SELECT symbol, yahoo_symbol, tsid_symbol
                            FROM exchange
                            WHERE yahoo_symbol NOT NULL
                            GROUP BY tsid_symbol""")
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=['symbol', 'yahoo_symbol',
                                                 'tsid_symbol'])
                return df
        except sqlite3.Error as e:
            print(e)
            raise SystemError('Failed to query the data from the exchange '
                              'table within query_exchanges in '
                              'YahooFinanceDataExtraction')
        except conn.OperationalError:
            raise SystemError('Unable to connect to the SQL Database in '
                              'query_exchanges in YahooFinanceDataExtraction. '
                              'Make sure the database address is correct.')
        except Exception as e:
            print(e)
            raise SystemError('Error: Unknown issue occurred in '
                              'query_exchanges in YahooFinanceDataExtraction.')

    def extractor(self, tsid):
        """ Takes the tsid symbol, downloads the historical data, and then
        saves the data into the SQLite database.

        :param tsid: String of the tsid
        :return: Nothing. It saves the price data in the SQLite Database.
        """

        main_time_start = time.time()

        # Rate limit this function with non-reactive timer
        time.sleep(self.min_interval)

        # The ticker has no prior price; add all the downloaded data
        if tsid not in self.latest_prices.index:
            clean_data = download_yahoo_data(db_url=self.db_url, tsid=tsid,
                                             exchanges_df=self.exchanges_df,
                                             csv_out=self.csv_wo_data)

            # There is no new data, so do nothing to the database
            if len(clean_data.index) == 0:
                if self.verbose:
                    print('No data for %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))
            # There is new data to add to the database
            else:
                clean_data.insert(0, 'data_vendor_id', self.vendor_id)

                df_to_sql(df=clean_data, db_location=self.db_location,
                          sql_table=self.table, exists='append', item=tsid)

                if self.verbose:
                    print('Updated %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))

        # The pricing database has prior values; append/replace new data points
        else:
            try:
                last_date = self.latest_prices.loc[tsid, 'date']
                raw_data = download_yahoo_data(db_url=self.db_url, tsid=tsid,
                                               exchanges_df=self.exchanges_df,
                                               csv_out=self.csv_wo_data)

                # Only keep data that is after the days_back period
                if self.data_process == 'replace' and self.days_back:
                    beg_date = (last_date - timedelta(days=self.days_back))
                    clean_data = raw_data[raw_data.date > beg_date.isoformat()]

                # Only keep data that is after the latest existing data point
                else:
                    clean_data = raw_data[raw_data.date > last_date.isoformat()]
            except Exception as e:
                print('Failed to determine what data is new for %s in extractor'
                      % tsid)
                print(e)
                return

            # There is not new data, so do nothing to the database
            if len(clean_data.index) == 0:
                if self.verbose:
                    print('No update for %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))
            # There is new data to add to the database
            else:
                clean_data.insert(0, 'data_vendor_id', self.vendor_id)

                # If replacing existing data, delete the overlapping data points
                if self.data_process == 'replace' and self.days_back:
                    # Data should be oldest to newest; gets the oldest date, as
                    #   any date between that and the latest date need to be
                    #   deleted before the new data can be added.
                    first_date_iso = clean_data['date'].min()

                    query = ("""DELETE FROM %s
                                WHERE tsid='%s'
                                AND date>='%s'
                                AND data_vendor_id='%s'""" %
                             (self.table, tsid, first_date_iso, self.vendor_id))

                    del_success = 'failure'
                    retry_count = 5
                    while retry_count > 0:
                        del_success = delete_sql_table_rows(
                            db_location=self.db_location, query=query,
                            table=self.table, tsid=tsid)

                        if del_success == 'failure':
                            retry_count -= 1
                            time.sleep(5)
                        elif del_success == 'success':
                            break

                    # If unable to delete existing data, skip ticker
                    if del_success == 'failure':
                        if self.verbose:
                            print('Unable to delete existing data for %s '
                                  'in the YahooFinanceDataExtraction. Skipping '
                                  'it for now.' % tsid)
                        return

                # Append the new data to the end, regardless of replacement
                df_to_sql(df=clean_data, db_location=self.db_location,
                          sql_table=self.table, exists='append', item=tsid)

                if self.verbose:
                    print('Updated %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))


class CSIDataExtractor(object):

    def __init__(self, db_location, db_url, data_type, redownload_time,
                 exchange_id=None):

        self.db_location = db_location
        self.db_url = db_url
        self.data_type = data_type
        self.exchange_id = exchange_id
        self.redownload_time = redownload_time

        self.main()

    def main(self):

        start_time = time.time()

        # Add new CSI Data tables to this if block
        if self.data_type == 'stock':
            table = 'csidata_stock_factsheet'
        else:
            raise SystemError('No table exists for the CSI Data %s '
                              'factsheet. Once the table has been added to '
                              'the create_tables.py file, add an elif '
                              'block to the main and query_existing_data '
                              'methods in the CSIDataExtractor class of '
                              'extractor.py.' % (self.data_type,))

        existing_data = self.query_existing_data(table)

        if len(existing_data) == 0:
            # The csidata_stock_factsheet table is empty; download new data

            print('Downloading the CSI Data factsheet for %s' %
                  (self.data_type,))
            data = download_csidata_factsheet(self.db_url, self.data_type,
                                              self.exchange_id)

            if len(data.index) == 0:
                print('No data returned for %s | %0.1f seconds' %
                      (self.data_type, time.time() - start_time))
                return

        else:
            # If there is existing data, check if the data is older than the
            #   specified update range. If so, ensure that data looks
            #   reasonable, and then delete the existing data.

            beg_date_obj = (datetime.utcnow() -
                            timedelta(days=self.redownload_time))
            if existing_data.loc[0, 'updated_date'] < beg_date_obj.isoformat():

                # Download the latest data
                print('Downloading the CSI Data factsheet for %s' %
                      (self.data_type,))
                data = download_csidata_factsheet(self.db_url, self.data_type,
                                                  self.exchange_id)

                if len(data.index) == 0:
                    print('No data returned for %s | %0.1f seconds' %
                          (self.data_type, time.time() - start_time))
                    return

                else:
                    if ((len(data) >= 1.2 * len(existing_data)) |
                            (len(data) <= len(existing_data) / 1.2)):
                        # Check to make sure new data is within 20% of the
                        #   existing data
                        print('The new data was outside of the 20% band from '
                              'the existing data')
                        return

                    # Delete old data
                    query = ('DELETE FROM %s' % (table,))
                    del_success = delete_sql_table_rows(self.db_location, query,
                                                        table, self.data_type)

                    if del_success == 'success':
                        print('The data in the %s table was successfully '
                              'deleted. Will now repopulate it...' % (table,))
                    elif del_success == 'failure':
                        print('There was an error deleting the data from %s' %
                              (table,))
                        return

            else:
                # The last update to the data was within the update window
                print('The downloaded data is within the update window, '
                      'thus the existing data will not be replaced')
                return

        # Add the new data to the specified table
        df_to_sql(data, self.db_location, table, 'append', self.data_type)
        print('Updated %s | %0.1f seconds' %
              (self.data_type, time.time() - start_time))

    def query_existing_data(self, table):
        """ Determine what prior CSI Data codes are in the database for the
        current data type (stock, commodity, etc.).

        :return: A DataFrame with the Quandl data set and the max page_num.
        """

        try:
            conn = sqlite3.connect(self.db_location)
            with conn:
                # Add new CSI Data tables to this if block
                if self.data_type == 'stock':
                    df = pd.read_sql("SELECT CsiNumber, updated_date "
                                     "FROM csidata_stock_factsheet", conn)
                else:
                    print('No table exists for the CSI Data %s factsheet. Once '
                          'the table has been added to the create_tables.py'
                          'file, add an elif block to the main and '
                          'query_existing_data methods in the CSIDataExtractor '
                          'class of extractor.py.' % (self.data_type,))
                    df = pd.DataFrame()
                return df
        except sqlite3.Error as e:
            print('Error when trying to connect to the database %s table in '
                  'query_existing_data.' % (table,))
            print(e)
