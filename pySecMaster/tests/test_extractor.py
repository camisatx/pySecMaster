import os
import pandas as pd
import unittest

from Git_Sync.pySecMaster.pySecMaster.utilities.user_dir import user_dir
from Git_Sync.pySecMaster.pySecMaster.extractor import \
    NASDAQSectorIndustryExtractor
from Git_Sync.pySecMaster.pySecMaster.download import QuandlDownload


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

        self.qd = QuandlDownload

    def test_download_quandl_data(self):

        df = self.qd.download_quandl_data(self, 'WIKI/AAPL',
                                          'test_quandl_codes_wo_data.csv')

        wo_df = pd.read_csv('test_quandl_codes_wo_data.csv', index_col=False)
        found = wo_df.loc[wo_df['q_codes'] == 'WIKI/AAPL']
        # os.remove('test_quandl_codes_wo_data.csv')
        self.assertEqual(len(found), 1)
