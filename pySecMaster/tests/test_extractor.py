import os
import pandas as pd
import unittest

from Git_Sync.pySecMaster.pySecMaster.download import QuandlDownload


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
