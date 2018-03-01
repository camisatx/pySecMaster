import csv
from datetime import datetime
import os
import pandas as pd
import psycopg2
import sys
import unittest

sys.path.append('..')

from utilities.user_dir import user_dir
from extractor import NASDAQSectorIndustryExtractor
from download import QuandlDownload, download_google_data, download_yahoo_data


class GoogleFinanceDownloadTests(unittest.TestCase):

    def setUp(self):
        userdir = user_dir()
        self.database = userdir['postgresql']['pysecmaster_db']
        self.user = userdir['postgresql']['pysecmaster_user']
        self.password = userdir['postgresql']['pysecmaster_password']
        self.host = userdir['postgresql']['pysecmaster_host']
        self.port = userdir['postgresql']['pysecmaster_port']

        self.google_fin_url = {
            'root': 'http://www.google.com/finance/getprices?',
            'ticker': 'q=',
            'exchange': 'x=',
            'interval': 'i=',   # 60; 60 seconds is the shortest interval
            # 'sessions': 'sessions=ext_hours',
            'period': 'p=',    # 20d; 15d is the longest period for min
            'fields': 'f=d,c,v,o,h,l',
        }    # order doesn't change anything

        self.exchanges_df = self.query_exchanges()

    def test_download_google_daily_price_data(self):
        self.google_fin_url['interval'] += str(60*60*24)
        self.google_fin_url['period'] += str(60) + 'd'
        tsid = 'AAPL.Q.0'

        csv_wo_data = 'test_goog_daily_wo_data.csv'
        with open(csv_wo_data, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['tsid', 'date_tried'])

        test_df = download_google_data(db_url=self.google_fin_url,
            tsid=tsid, exchanges_df=self.exchanges_df, csv_out=csv_wo_data)
        print(test_df)
        self.assertGreater(len(test_df.index), 1)
        os.remove(csv_wo_data)

    def test_download_google_minute_price_data(self):
        self.google_fin_url['interval'] += str(60)
        self.google_fin_url['period'] += str(20) + 'd'
        tsid = 'AAPL.Q.0'

        csv_wo_data = 'test_goog_minute_wo_data.csv'
        with open(csv_wo_data, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['tsid', 'date_tried'])

        test_df = download_google_data(db_url=self.google_fin_url,
            tsid=tsid, exchanges_df=self.exchanges_df, csv_out=csv_wo_data)
        print(test_df)
        self.assertGreater(len(test_df.index), 1)
        os.remove(csv_wo_data)

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
                              'GoogleFinanceDownloadTests')
        except conn.OperationalError:
            raise SystemError('Unable to connect to the SQL Database in '
                              'query_exchanges in GoogleFinanceDownloadTests. '
                              'Make sure the database address is correct.')
        except Exception as e:
            print(e)
            raise SystemError('Error: Unknown issue occurred in '
                              'query_exchanges in GoogleFinanceDownloadTests.')

        conn.close()
        return df


class YahooFinanceDownloadTests(unittest.TestCase):

    def setUp(self):
        userdir = user_dir()
        self.database = userdir['postgresql']['pysecmaster_db']
        self.user = userdir['postgresql']['pysecmaster_user']
        self.password = userdir['postgresql']['pysecmaster_password']
        self.host = userdir['postgresql']['pysecmaster_host']
        self.port = userdir['postgresql']['pysecmaster_port']

        cur_posix_time = str(datetime.now().timestamp())
        cur_posix_time = cur_posix_time[:cur_posix_time.find('.')]
        self.yahoo_fin_url = {
            'root': 'https://query1.finance.yahoo.com/v7/finance/download/',
            'start_date': 'period1=0',     # First POSIX time (whole hist)
            'end_date': 'period2=' + cur_posix_time,   # Cur POSIX time
            'interval': 'interval=',   # 1d, 1w, 1mo: (daily, wkly, mthly)
            'events': 'events=',       # history, div, split
            'cookie': 'crumb=',        # Cookie value
        }

        self.csv_wo_data = 'test_yahoo_daily_wo_data.csv'
        with open(self.csv_wo_data, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['tsid', 'date_tried'])

        self.exchanges_df = self.query_exchanges()

    def tearDown(self):
        os.remove(self.csv_wo_data)

    def test_download_yahoo_daily_price_data(self):
        self.yahoo_fin_url['interval'] += '1d'
        self.yahoo_fin_url['events'] += 'history'
        tsid = 'AAPL.Q.0'

        test_df = download_yahoo_data(db_url=self.yahoo_fin_url, tsid=tsid,
            exchanges_df=self.exchanges_df, csv_out=self.csv_wo_data)
        print(test_df)
        self.assertGreater(len(test_df.index), 1)

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
                              'YahooFinanceDownloadTests')
        except conn.OperationalError:
            raise SystemError('Unable to connect to the SQL Database in '
                              'query_exchanges in YahooFinanceDownloadTests. '
                              'Make sure the database address is correct.')
        except Exception as e:
            print(e)
            raise SystemError('Error: Unknown issue occurred in '
                              'query_exchanges in YahooFinanceDownloadTests.')

        conn.close()
        return df


class NASDAQSectorIndustryExtractorTests(unittest.TestCase):

    def setUp(self):
        userdir = user_dir()
        self.database = userdir['postgresql']['pysecmaster_db']
        self.user = userdir['postgresql']['pysecmaster_user']
        self.password = userdir['postgresql']['pysecmaster_password']
        self.host = userdir['postgresql']['pysecmaster_host']
        self.port = userdir['postgresql']['pysecmaster_port']

        self.nasdaq_sector_industry_url = 'http://www.nasdaq.com/screening/' \
                                          'companies-by-industry.aspx?'
        self.nasdaq_sector_industry_extractor_exchanges = ['NASDAQ', 'NYSE',
                                                           'AMEX']
        self.nasdaq_sector_industry_redownload_time = 0

    def test_nasdaq_extractor(self):
        NASDAQSectorIndustryExtractor(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            db_url=self.nasdaq_sector_industry_url,
            exchange_list=self.nasdaq_sector_industry_extractor_exchanges,
            redownload_time=self.nasdaq_sector_industry_redownload_time)


class QuandlDownloadTests(unittest.TestCase):

    def setUp(self):
        userdir = user_dir()
        quandl_token = userdir['quandl']['quandl_token']
        db_url = ['https://www.quandl.com/api/v1/datasets/', '.csv']
        self.qd = QuandlDownload(quandl_token=quandl_token, db_url=db_url)

        self.csv_wo_data = 'test_quandl_codes_wo_data.csv'
        with open(self.csv_wo_data, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['q_code', 'date_tried'])

    def tearDown(self):
        os.remove(self.csv_wo_data)

    def test_download_quandl_data(self):
        test_df = self.qd.download_quandl_data('WIKI/AAPL', self.csv_wo_data)
        print(test_df.head(5))
        self.assertGreater(len(test_df.index), 1)


if __name__ == '__main__':
    unittest.main()
