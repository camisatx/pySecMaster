import unittest

from create_tables import main_tables


class DatabaseCreationTests(unittest.TestCase):

    def setUp(self):

        self.db_name = 'pySecMaster'

    def test_main_table_creation(self):

        main_tables(self.db_name)
