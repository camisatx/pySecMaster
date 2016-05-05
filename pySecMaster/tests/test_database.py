import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import unittest

from create_tables import create_database, main_tables, data_tables,\
    events_tables
from utilities.user_dir import user_dir


class DatabaseCreationTests(unittest.TestCase):

    def setUp(self):

        self.userdir = user_dir()['postgresql']

        self.db_name = self.userdir['pysecmaster_test_db']
        self.user = self.userdir['pysecmaster_test_user']
        self.password = self.userdir['pysecmaster_test_password']
        self.host = self.userdir['pysecmaster_test_host']
        self.port = self.userdir['pysecmaster_test_port']

    def test_database_creation(self):

        create_database(database=self.db_name)

        conn = psycopg2.connect(database=self.userdir['main_db'],
                                user=self.userdir['main_user'],
                                password=self.userdir['main_password'],
                                host=self.userdir['main_host'],
                                port=self.userdir['main_port'])
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT datname FROM pg_catalog.pg_database
                        WHERE lower(datname)=lower('%s')""" % self.db_name)
            database_exist = cur.fetchone()

            self.assertEqual(len(database_exist), 1)

            # cur.execute("""SELECT pg_terminate_backend(pg_stat_activity.pid)
            #             FROM pg_stat_activity
            #             WHERE datname = current_database()
            #             AND pid <> pg_backend_pid()""")
            cur.execute("""DROP DATABASE IF EXISTS %s""" % self.db_name)
            cur.close()
        conn.close()

    def test_table_creation(self):

        create_database(database=self.db_name, user=self.user)

        main_tables(database=self.db_name, user=self.user,
                    password=self.password, host=self.host, port=self.port)
        data_tables(database=self.db_name, user=self.user,
                    password=self.password, host=self.host, port=self.port)
        events_tables(database=self.db_name, user=self.user,
                      password=self.password, host=self.host, port=self.port)

        tables_to_create = ['fundamental_data', 'daily_prices', 'finra_data',
                            'minute_prices', 'conference_calls', 'dividends',
                            'earnings', 'exchange', 'economic_events',
                            'ipo_pricings', 'symbology', 'splits',
                            'csidata_stock_factsheet', 'baskets',
                            'basket_values', 'indices', 'quandl_codes',
                            'data_vendor', 'option_chains', 'tick_prices',
                            'tick_prices_stream']
        tables_created = []
        extra_table = []
        missing_table = []

        conn = psycopg2.connect(database=self.db_name, user=self.user,
                                password=self.password, host=self.host,
                                port=self.port)

        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema='public'
                        AND table_type='BASE TABLE'""")
            tables_exists = cur.fetchall()

            if tables_exists:

                for table in tables_exists:
                    tables_created.append(table[0])
                    if table[0] not in tables_to_create:
                        extra_table.append(table[0])

                for table in tables_to_create:
                    if table not in tables_created:
                        missing_table.append(table)

            cur.close()
        conn.close()

        if missing_table:
            print('Missing tables: %s' % missing_table)
        if extra_table:
            print('Extra tables: %s' % extra_table)

        self.assertEqual(len(tables_to_create), len(tables_created))
        self.assertEqual(len(missing_table), 0)
        self.assertEqual(len(extra_table), 0)

        # Connect as the server super user to drop the test database
        conn = psycopg2.connect(database=self.userdir['main_db'],
                                user=self.userdir['main_user'],
                                password=self.userdir['main_password'],
                                host=self.userdir['main_host'],
                                port=self.userdir['main_port'])
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn:
            cur = conn.cursor()
            cur.execute("""DROP DATABASE IF EXISTS %s""" % self.db_name)
            cur.close()
        conn.close()
