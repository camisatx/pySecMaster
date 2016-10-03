import csv
from datetime import datetime, timedelta, timezone
import pandas as pd
import psycopg2
import re
from sqlalchemy import create_engine
import time

from download import QuandlDownload, download_google_data, \
    download_yahoo_data, download_csidata_factsheet,\
    download_nasdaq_industry_sector
from utilities.database_queries import df_to_sql, delete_sql_table_rows, \
    query_data_vendor_id, query_codes, query_csi_stock_start_date,\
    query_last_price, query_q_codes, query_tsid_based_on_exchanges,\
    update_classification_values
from utilities.multithread import multithread

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.4.3'

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

    def __init__(self, database, user, password, host, port, quandl_token,
                 database_list, database_url, update_range, threads):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
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
                  (len(self.db_list), time.time() - extractor_start))

        # Codes already exists in the quandl_codes table; this will determine
        #   if the codes need to be updated or if the download was incomplete.
        else:

            # This for loop only provides program info; doesn't format anything
            for row in range(len(data_sets)):
                existing_vendor = data_sets.loc[row, 'data_vendor']
                # existing_page_num = data_sets.loc[row, 'page_num']
                existing_updated_date = data_sets.loc[row, 'updated_date']
                if (existing_vendor[existing_vendor.find('_')+1] not in
                        self.db_list):
                    print('FLAG: Did not update the %s data set because it was '
                          'not included in the database_list variable. The '
                          'last update was on %s' %
                          (existing_vendor, existing_updated_date))

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

                    beg_date_obj = (datetime.now(timezone.utc) - timedelta(
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
                        del_success = delete_sql_table_rows(
                            database=self.database, user=self.user,
                            password=self.password, host=self.host,
                            port=self.port, query=query, table='quandl_codes',
                            item='quandl_code_download')
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

        engine = create_engine('postgresql://%s:%s@%s:%s/%s' %
                               (self.user, self.password, self.host, self.port,
                                self.database))
        conn = engine.connect()
        df = pd.DataFrame()

        try:
            with conn:
                df = pd.read_sql("""SELECT DISTINCT ON (data_vendor)
                                     data_vendor, page_num, updated_date
                                 FROM quandl_codes
                                 ORDER BY data_vendor, page_num DESC
                                     NULLS LAST""", conn)
        except Exception as e:
            print('Error when querying the quandl code download page from the '
                  '%s database inQuandlCodeExtract.query_last_download_pg' %
                  self.database)
            print(e)

        conn.close()
        return df

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
                    db_pg_df.insert(4, 'symbology_source', 'Unknown')
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

                # Find and add the source_name and source_id to the clean_df
                clean_df['data_vendor'] = 'Quandl_' + db_name
                clean_df['symbology_source'] = 'quandl_' + db_name.lower()

                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_df, sql_table='quandl_codes',
                          exists='append', item=db_name)

                if page_num % 100 == 0:
                    print('Still downloading %s codes. Just finished page '
                          '%i...' % (db_name, page_num))
                page_num += 1

        # Remove duplicate q_codes
        conn = psycopg2.connect(database=self.database, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""DELETE FROM quandl_codes qc1
                            USING quandl_codes qc2
                            WHERE qc1.q_code = qc2.q_code
                            AND qc1.q_code_id < qc2.q_code_id""")
                conn.commit()
                print('Successfully removed all duplicate q_codes from '
                      'quandl_codes')
        except psycopg2.Error as e:
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
        conn.close()

        # Change the data set page_num variable to -2, indicating it finished
        conn = psycopg2.connect(database=self.database, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""UPDATE quandl_codes
                               SET page_num=-2
                               WHERE data_vendor=%s""", ('Quandl_' + db_name,))
                conn.commit()
                print('Successfully updated %s codes with final page_num '
                      'variable.' % (db_name,))
        except psycopg2.Error as e:
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
        conn.close()

        print('The %s database took %0.1f seconds to download'
              % (db_name, time.time() - dl_csv_start_time))

    @staticmethod
    def process_3_item_q_codes(df):

        # Each EIA q_code structure: EIA/[document]_[component]_[period]
        #   NOTE: EIA/IES database does not follow this structure
        # JODI q_code structure: JODI/[type]_[product][flow][unit]_[country]

        def strip_q_code(row, column):
            q_code = row['q_code']
            if column == 'data':
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

        df['data'] = df.apply(strip_q_code, axis=1, args=('data',))
        df['component'] = df.apply(strip_q_code, axis=1, args=('component',))
        df['period'] = df.apply(strip_q_code, axis=1, args=('period',))
        return df

    @staticmethod
    def process_2_item_q_codes(df):

        def strip_q_code(row, column):
            q_code = row['q_code']
            if column == 'data':
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

        df['data'] = df.apply(strip_q_code, axis=1, args=('data',))
        df['component'] = df.apply(strip_q_code, axis=1, args=('component',))
        return df

    @staticmethod
    def process_1_item_q_codes(df):

        def strip_q_code(row, column):
            q_code = row['q_code']
            if column == 'component':
                return q_code[q_code.find('/') + 1:]
            else:
                print('Error: Unknown column [%s] passed in to strip_q_code in '
                      'process_1_item_q_codes' % (column,))

        df['component'] = df.apply(strip_q_code, axis=1, args=('component',))
        return df


class QuandlDataExtraction(object):

    def __init__(self, database, user, password, host, port, quandl_token,
                 db_url, download_selection, redownload_time, data_process,
                 days_back, threads, table, load_tables='load_tables',
                 verbose=False):
        """
        :param database: String of the directory location for the SQL database.
        :param user: String of the username used to login to the database
        :param password: String of the password used to login to the database
        :param host: String of the database address (localhost, url, ip, etc.)
        :param port: Integer of the database port number (5432)
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
        :param load_tables: String of the directory location for the load tables
        :param verbose: Boolean of whether debugging prints should occur.
        """

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
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

        self.csv_wo_data = load_tables + '/quandl_' + self.table+'_wo_data.csv'

        # Retrieve the Quandl data vendor IDs. Not able to use a list of all
        #   Quandl data vendor IDs because that prevents data being downloaded
        #   for the same tsid but from different Quandl sources (ie wiki % goog)
        # vendor_id = retrieve_data_vendor_id(
        #     database=self.database, user=self.user, password=self.password,
        #     host=self.host, port=self.port, name='Quandl_%')
        if self.download_selection[:4] == 'wiki':
            self.vendor_id = query_data_vendor_id(
                database=self.database, user=self.user, password=self.password,
                host=self.host, port=self.port, name='Quandl_WIKI')
        elif self.download_selection[:4] == 'goog':
            self.vendor_id = query_data_vendor_id(
                database=self.database, user=self.user, password=self.password,
                host=self.host, port=self.port, name='Quandl_GOOG')
        else:
            raise NotImplementedError('The %s Quandl source is not implemented '
                                      'in the init within QuandlDataExtraction'
                                      % self.download_selection)

        print('Retrieving dates of the last price per ticker for all Quandl '
              'values.')
        # Creates a DataFrame with the last price for each Quandl code
        self.latest_prices = query_last_price(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, table=self.table,
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
        q_code_df = query_q_codes(database=self.database, user=self.user,
                                  password=self.password, host=self.host,
                                  port=self.port,
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
            beg_date_ob_wo_data = (datetime.now(timezone.utc) -
                                   timedelta(days=15))
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
        beg_date_obj = (datetime.now(timezone.utc) -
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

            # Retrieve the stock's start date from the csi table, and use it
            #   as the beg_date when downloading the Quandl data
            start_date = query_csi_stock_start_date(
                database=self.database, user=self.user, password=self.password,
                host=self.host, port=self.port, tsid=tsid)

            clean_data = quandl_download.download_quandl_data(
                q_code=q_code, csv_out=self.csv_wo_data, beg_date=start_date)

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
                clean_data.insert(1, 'source', 'tsid')
                clean_data.insert(2, 'source_id', tsid)
                # clean_data.drop('q_code', axis=1, inplace=True)

                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_data,
                          sql_table='daily_prices', exists='append', item=tsid)
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
                    clean_data = raw_data[raw_data.date > last_date]

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
                clean_data.insert(1, 'source', 'tsid')
                clean_data.insert(2, 'source_id', tsid)
                # clean_data.drop('q_code', axis=1, inplace=True)

                # If replacing existing data, delete the overlapping data points
                if self.data_process == 'replace' and self.days_back:
                    # Data should be newest to oldest; gets the oldest date, as
                    #   any date between that and the latest date need to be
                    #   deleted before the new data can be added.
                    first_date_iso = clean_data['date'].min()
                    query = ("""DELETE FROM daily_prices
                                WHERE source_id='%s'
                                AND date>='%s'""" % (tsid, first_date_iso))

                    del_success = 'failure'
                    retry_count = 5
                    while retry_count > 0:
                        del_success = delete_sql_table_rows(
                            database=self.database, user=self.user,
                            password=self.password, host=self.host,
                            port=self.port, query=query, table='daily_prices',
                            item=tsid)

                        if del_success == 'failure':
                            retry_count -= 1
                        elif del_success == 'success':
                            break

                    # Not able to delete existing data, so skip ticker for now
                    if del_success == 'failure':
                        return

                # Append the new data to the end, regardless of replacement
                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_data,
                          sql_table='daily_prices', exists='append', item=tsid)
                print('Updated %s | %0.1f seconds' %
                      (q_code, time.time() - main_time_start))


class GoogleFinanceDataExtraction(object):

    def __init__(self, database, user, password, host, port, db_url,
                 download_selection, redownload_time, data_process, days_back,
                 threads, table, load_tables='load_tables', verbose=True):
        """
        :param database: String of the directory location for the SQL database.
        :param user: String of the username used to login to the database
        :param password: String of the password used to login to the database
        :param host: String of the database address (localhost, url, ip, etc.)
        :param port: Integer of the database port number (5432)
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
        :param load_tables: String of the directory location for the load tables
        :param verbose: Boolean of whether debugging prints should occur.
        """

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_url = db_url
        self.download_selection = download_selection
        self.redownload_time = redownload_time
        self.data_process = data_process
        self.days_back = days_back
        self.threads = threads
        self.table = table
        self.verbose = verbose

        # Rate limiter parameters based on guessed Google Finance limitations
        # Received captcha if too fast (about 2000 queries within x seconds)
        rate = 60       # Received captcha at 70/60s
        period_sec = 60
        self.min_interval = float((period_sec/rate)*threads)

        self.vendor_id = query_data_vendor_id(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, name='Google_Finance')

        self.csv_wo_data = load_tables + '/goog_' + self.table + '_wo_data.csv'

        print('Retrieving dates for the last Google prices per ticker...')
        # Creates a DataFrame with the last price for each security
        self.latest_prices = query_last_price(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, table=self.table,
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
        code_df = query_codes(database=self.database, user=self.user,
                              password=self.password, host=self.host,
                              port=self.port,
                              download_selection=self.download_selection)
        # Get DF of selected codes plus when (if ever) they were last updated
        codes_df = pd.merge(code_df, self.latest_prices, left_on='tsid',
                            right_index=True, how='left')
        # Sort the DF with un-downloaded items first, then based on last update.
        # df.sort_values introduced in 0.17.0
        codes_df.sort_values('updated_date', axis=0, ascending=True,
                             na_position='first', inplace=True)

        try:
            # Load the codes that did not have data from the last extractor run
            # for this interval
            codes_wo_data_df = pd.read_csv(self.csv_wo_data, index_col=False)
            # Exclude these codes that are within a 15 day period
            beg_date_ob_wo_data = (datetime.now(timezone.utc) -
                                   timedelta(days=15))
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
        beg_date_obj = (datetime.now(timezone.utc) -
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

        conn = psycopg2.connect(database=self.database, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""SELECT DISTINCT ON (tsid_symbol)
                                symbol, goog_symbol, tsid_symbol
                            FROM exchanges
                            WHERE goog_symbol IS NOT NULL
                            AND goog_symbol != 'NaN'
                            ORDER BY tsid_symbol ASC NULLS LAST""")
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=['symbol', 'goog_symbol',
                                                 'tsid_symbol'])
        except psycopg2.Error as e:
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

        conn.close()
        return df

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
                clean_data.insert(1, 'source', 'tsid')
                clean_data.insert(2, 'source_id', tsid)

                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_data, sql_table=self.table,
                          exists='append', item=tsid)

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
                    clean_data = raw_data[raw_data.date > last_date]
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
                clean_data.insert(1, 'source', 'tsid')
                clean_data.insert(2, 'source_id', tsid)

                # If replacing existing data, delete the overlapping data points
                if self.data_process == 'replace' and self.days_back:
                    # Data should be oldest to newest; gets the oldest date, as
                    #   any date between that and the latest date need to be
                    #   deleted before the new data can be added.
                    first_date_iso = clean_data['date'].min()

                    query = ("""DELETE FROM %s
                                WHERE source_id='%s'
                                AND date>='%s'
                                AND data_vendor_id='%s'""" %
                             (self.table, tsid, first_date_iso, self.vendor_id))

                    del_success = 'failure'
                    retry_count = 5
                    while retry_count > 0:
                        del_success = delete_sql_table_rows(
                            database=self.database, user=self.user,
                            password=self.password, host=self.host,
                            port=self.port, query=query, table=self.table,
                            item=tsid)

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
                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_data, sql_table=self.table,
                          exists='append', item=tsid)

                if self.verbose:
                    print('Updated %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))


class YahooFinanceDataExtraction(object):

    def __init__(self, database, user, password, host, port, db_url,
                 download_selection, redownload_time, data_process, days_back,
                 threads, table, load_tables='load_tables', verbose=True):
        """
        :param database: String of the directory location for the SQL database.
        :param user: String of the username used to login to the database
        :param password: String of the password used to login to the database
        :param host: String of the database address (localhost, url, ip, etc.)
        :param port: Integer of the database port number (5432)
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
        :param load_tables: String of the directory location for the load tables
        :param verbose: Boolean of whether debugging prints should occur.
        """

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
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

        self.vendor_id = query_data_vendor_id(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, name='Yahoo_Finance')

        self.csv_wo_data = load_tables + '/yahoo_' + self.table + '_wo_data.csv'

        print('Retrieving dates for the last Yahoo prices per ticker...')
        # Creates a DataFrame with the last price for each security
        self.latest_prices = query_last_price(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, table=self.table,
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
        code_df = query_codes(database=self.database, user=self.user,
                              password=self.password, host=self.host,
                              port=self.port,
                              download_selection=self.download_selection)
        # Get DF of selected codes plus when (if ever) they were last updated
        codes_df = pd.merge(code_df, self.latest_prices, left_on='tsid',
                            right_index=True, how='left')
        # Sort the DF with un-downloaded items first, then based on last update.
        # df.sort_values introduced in 0.17.0
        codes_df.sort_values('updated_date', axis=0, ascending=True,
                             na_position='first', inplace=True)

        try:
            # Load the codes that did not have data from the last extractor run
            #   for this interval
            codes_wo_data_df = pd.read_csv(self.csv_wo_data, index_col=False)
            # Exclude these codes that are within a 15 day period
            beg_date_ob_wo_data = (datetime.now(timezone.utc) -
                                   timedelta(days=15))
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
        beg_date_obj = (datetime.now(timezone.utc) -
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

        conn = psycopg2.connect(database=self.database, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)
        try:
            with conn:
                cur = conn.cursor()
                cur.execute("""SELECT DISTINCT ON (tsid_symbol)
                                symbol, yahoo_symbol, tsid_symbol
                            FROM exchanges
                            WHERE yahoo_symbol IS NOT NULL
                            AND yahoo_symbol != 'NaN'
                            ORDER BY tsid_symbol ASC NULLS LAST""")
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=['symbol', 'yahoo_symbol',
                                                 'tsid_symbol'])
        except psycopg2.Error as e:
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

        conn.close()
        return df

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
                clean_data.insert(1, 'source', 'tsid')
                clean_data.insert(2, 'source_id', tsid)

                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_data, sql_table=self.table,
                          exists='append', item=tsid)

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
                    clean_data = raw_data[raw_data.date > last_date]
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
                clean_data.insert(1, 'source', 'tsid')
                clean_data.insert(2, 'source_id', tsid)

                # If replacing existing data, delete the overlapping data points
                if self.data_process == 'replace' and self.days_back:
                    # Data should be oldest to newest; gets the oldest date, as
                    #   any date between that and the latest date need to be
                    #   deleted before the new data can be added.
                    first_date_iso = clean_data['date'].min()

                    query = ("""DELETE FROM %s
                                WHERE source_id='%s'
                                AND date>='%s'
                                AND data_vendor_id='%s'""" %
                             (self.table, tsid, first_date_iso, self.vendor_id))

                    del_success = 'failure'
                    retry_count = 5
                    while retry_count > 0:
                        del_success = delete_sql_table_rows(
                            database=self.database, user=self.user,
                            password=self.password, host=self.host,
                            port=self.port, query=query, table=self.table,
                            item=tsid)

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
                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=clean_data, sql_table=self.table,
                          exists='append', item=tsid)

                if self.verbose:
                    print('Updated %s | %0.1f seconds' %
                          (tsid, time.time() - main_time_start))


class CSIDataExtractor(object):

    def __init__(self, database, user, password, host, port, db_url, data_type,
                 redownload_time, exchange_id=None):

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
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

        existing_data = self.query_existing_data()

        if len(existing_data.index) == 0:
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

            beg_date_obj = (datetime.now(timezone.utc) -
                            timedelta(days=self.redownload_time))

            if existing_data.loc[0, 'updated_date'] < beg_date_obj:

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
                    data_col = list(data.columns.values)
                    existing_data_col = list(existing_data.columns.values)
                    if ((len(data) >= 1.2 * len(existing_data)) |
                            (len(data) <= len(existing_data) / 1.2) |
                            (data_col == existing_data_col)):
                        # Check to make sure new data is within 20% of the
                        #   existing data, along with the columns being the same
                        print('The new data was either outside of the 20% band '
                              'or it had different column names from the '
                              'existing data. The existing values were NOT '
                              'replaced.')
                        return

                    # Delete old data [since not referenced in a foreign key]
                    query = ('DELETE FROM %s' % (table,))
                    del_success = delete_sql_table_rows(
                        database=self.database, user=self.user,
                        password=self.password, host=self.host, port=self.port,
                        query=query, table=table, item=self.data_type)

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
        df_to_sql(database=self.database, user=self.user,
                  password=self.password, host=self.host, port=self.port,
                  df=data, sql_table=table, exists='append',
                  item=self.data_type)
        print('Updated %s | %0.1f seconds' %
              (self.data_type, time.time() - start_time))

    def query_existing_data(self):
        """ Determine what prior CSI Data codes are in the database for the
        current data type (stock, commodity, etc.).

        :return: DataFrame
        """

        conn = psycopg2.connect(database=self.database, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)
        df = pd.DataFrame()

        try:
            with conn:
                # Add new CSI Data tables to this if block
                if self.data_type == 'stock':
                    df = pd.read_sql("SELECT csi_number, updated_date "
                                     "FROM csidata_stock_factsheet", conn)
                else:
                    print('No table exists for the CSI Data %s factsheet. Once '
                          'the table has been added to the create_tables.py'
                          'file, add an elif block to the main and '
                          'query_existing_data methods in the CSIDataExtractor '
                          'class of extractor.py.' % (self.data_type,))

        except psycopg2.Error as e:
            print('Error when trying to connect to the %s database in '
                  'query_existing_data.' % (self.database,))
            print(e)

        conn.close()
        return df


class NASDAQSectorIndustryExtractor(object):
    """ Download and store the sector and industry data for companies traded
    on the NASDAQ, NYSE and AMEX exchanges.

    http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ
        &render=download
    """

    def __init__(self, database, user, password, host, port, db_url,
                 exchange_list, redownload_time):
        """
        :param database: String of the database name
        :param user: String of the username used to login to the database
        :param password: String of the password used to login to the database
        :param host: String of the database address (localhost, url, ip, etc.)
        :param port: Integer of the database port number (5432)
        :param db_url: String of the root URL for downloading this data
        :param exchange_list: List of the exchanges whose values should be
            downloaded; valid exchanges include NASDAQ, NYSE and AMEX
        :param redownload_time: Integer of the number of days to pass before
            the existing data should be replaced
        """

        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_url = db_url
        self.exchange_list = exchange_list
        self.redownload_time = redownload_time

        self.main()

    def main(self):

        start_time = time.time()

        existing_data_df = self.query_existing_data()

        if len(existing_data_df.index) == 0:
            # There are no sector or industry values from NASDAQ within the
            #   classification table; download new data

            print('Downloading the NASDAQ sector and industry data for the '
                  'following exchanges: %s' % self.exchange_list)

            raw_df = download_nasdaq_industry_sector(self.db_url,
                                                     self.exchange_list)

            if len(raw_df.index) == 0:
                print('No data returned for these exchange: %s' %
                      self.exchange_list)
                return

        else:
            # There is existing data, so check if the data is older than the
            #   specified update range. If so, ensure that data looks
            #   reasonable, and then delete the existing data.

            beg_date_obj = (datetime.now(timezone.utc) -
                            timedelta(days=self.redownload_time))

            if existing_data_df.loc[0, 'updated_date'] < beg_date_obj:

                # Download the latest data
                print('Downloading the NASDAQ sector and industry data for '
                      'the following exchanges: %s' % self.exchange_list)

                raw_df = download_nasdaq_industry_sector(self.db_url,
                                                         self.exchange_list)

                if len(raw_df.index) == 0:
                    print('No data returned for these exchanges: %s' %
                          self.exchange_list)
                    return

            else:
                # The last update to the data was within the update window
                print('The downloaded sector and industry data is within the '
                      'update window, thus the existing data will not be '
                      'replaced.')
                return

        raw_df.drop_duplicates(subset=['symbol', 'exchange'], inplace=True)

        # Replace the exchange name with the tsid exchange abbreviation
        raw_df.replace(to_replace={'exchange': {'NASDAQ': 'Q', 'NYSE': 'N'}},
                       inplace=True)

        # Build a faux tsid from the symbol and exchange values, which will be
        #   compared against the real tsid values for matches
        raw_df['temp_tsid'] = raw_df['symbol'] + '.' + raw_df['exchange'] + '.0'

        exchange_abbrev_list = ['AMEX', 'N', 'Q']   # tsid exchange abbrev
        existing_tsid_df = query_tsid_based_on_exchanges(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, exchanges_list=exchange_abbrev_list)

        # Only want to keep the values that have a temp_tsid match with an
        #   existing tsid. Not able to insert a tsid that doesn't already
        #   exist within the symbology table because that would raise a foreign
        #   key error. Assume that symbology contains all possible tsids.
        raw_df = pd.merge(left=raw_df, right=existing_tsid_df, how='inner',
                          left_on='temp_tsid', right_on='source_id')
        raw_df.drop(['symbol', 'exchange', 'temp_tsid'], axis=1, inplace=True)

        # Compare the existing values with the new values, only keeping the
        #   altered and new values
        altered_values_df = self.altered_values(existing_df=existing_data_df,
                                                new_df=raw_df)
        if len(altered_values_df.index) == 0:
            print('No new items in the NASDAQ sector and industry extractor.')
            return

        clean_df = pd.DataFrame()
        clean_df.insert(0, 'source_id', altered_values_df['source_id'])
        clean_df.insert(0, 'source', 'tsid')    # Must be after DF is populated
        clean_df.insert(2, 'standard', 'NASDAQ')
        clean_df.insert(3, 'code', None)
        clean_df.insert(4, 'level_1', altered_values_df['sector'])
        clean_df.insert(5, 'level_2', altered_values_df['industry'])
        clean_df.insert(6, 'level_3', None)
        clean_df.insert(7, 'level_4', None)
        clean_df.insert(len(clean_df.columns), 'created_date',
                        datetime.now().isoformat())
        clean_df.insert(len(clean_df.columns), 'updated_date',
                        datetime.now().isoformat())

        # Separate out the updated values from the altered_df
        updated_symbols_df = (clean_df[clean_df['source_id'].
                              isin(existing_data_df['tsid'])])
        # Update all modified values in the database
        update_classification_values(
            database=self.database, user=self.user, password=self.password,
            host=self.host, port=self.port, values_df=updated_symbols_df)

        # Separate out the new values from the altered_df
        new_symbols_df = (clean_df[~clean_df['source_id'].
                          isin(existing_data_df['tsid'])])

        # Append the new values to the existing classification table
        df_to_sql(database=self.database, user=self.user,
                  password=self.password, host=self.host, port=self.port,
                  df=new_symbols_df, sql_table='classification',
                  exists='append', item='NASDAQ exchanges')

        print('Updated these NASDAQ exchanges: %s | %0.1f seconds' %
              (self.exchange_list, time.time() - start_time))

    def query_existing_data(self):
        """ Determine which tsids have existing sector and industry data from
        NASDAQ.

        :return: DataFrame
        """

        conn = psycopg2.connect(database=self.database, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)
        df = pd.DataFrame()

        try:
            with conn:
                df = pd.read_sql("""SELECT source_id AS tsid, level_1 AS sector,
                                     level_2 AS industry, updated_date
                                 FROM classification
                                 WHERE standard='NASDAQ'""", conn)

        except psycopg2.Error as e:
            print('Error when trying to connect to the %s database in '
                  'NASDAQSectorIndustryExtractor.query_existing_data.' %
                  (self.database,))
            print(e)

        conn.close()
        return df

    @staticmethod
    def altered_values(existing_df, new_df):
        """ Compare the two provided DataFrames, returning a new DataFrame
        that only includes rows from the new_df that are different from the
        existing_df.

        :param existing_df: DataFrame of the existing values
        :param new_df: DataFrame of the next values
        :return: DataFrame with the altered/new values
        """

        # Convert both DataFrames to all string objects. Normally, the symbol_id
        #   column of the existing_df is an int64 object, messing up the merge
        if len(existing_df.index) > 0:
            existing_df = existing_df.applymap(str)
        new_df = new_df.applymap(str)

        # DataFrame with the similar values from both the existing_df and the
        #   new_df. The comparison is based on the symbol_id/sid and
        #   source_id/ticker columns.
        combined_df = pd.merge(left=existing_df, right=new_df, how='inner',
                               left_on=['tsid', 'sector', 'industry'],
                               right_on=['source_id', 'sector', 'industry'])

        # In a new DataFrame, only keep the new_df rows that did NOT have a
        #   match to the existing_df
        altered_df = new_df[~new_df['source_id'].isin(combined_df['source_id'])]

        return altered_df
