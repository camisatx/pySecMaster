import pandas as pd
import psycopg2
import time

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


def query_entire_table(database, user, password, host, port, table):
    """ Query all of the active tsid values from the specified database.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param table: String of the table whose values should be returned
    :return: DataFrame of the returned values
    """

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    try:
        with conn:
            cur = conn.cursor()
            query = ("""SELECT sym.source_id AS tsid
                     FROM symbology AS sym,
                     LATERAL (
                         SELECT source_id
                         FROM %s
                         WHERE source_id = sym.source_id
                         ORDER BY source_id ASC NULLS LAST
                         LIMIT 1) AS prices""" %
                     (table,))
            cur.execute(query)
            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows)
            else:
                raise SystemExit('No data returned from query_entire_table')

            return df
    except psycopg2.Error as e:
        print(
            'Error when trying to retrieve data from the %s database in '
            'query_entire_table' % database)
        print(e)
    except conn.OperationalError:
        raise SystemError('Unable to connect to the %s database in '
                          'query_entire_table. Make sure the database '
                          'address/name are correct.' % database)
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_entire_table')

if __name__ == '__main__':

    from utilities.user_dir import user_dir

    userdir = user_dir()

    test_database = userdir['postgresql']['pysecmaster_db']
    test_user = userdir['postgresql']['pysecmaster_user']
    test_password = userdir['postgresql']['pysecmaster_password']
    test_host = userdir['postgresql']['pysecmaster_host']
    test_port = userdir['postgresql']['pysecmaster_port']

    test_table = 'daily_prices'      # daily_prices, minute_prices, quandl_codes

    start_time = time.time()

    table_df = query_entire_table(test_database, test_user, test_password,
                                  test_host, test_port, test_table)

    print('Query took %0.2f seconds' % (time.time() - start_time))

    # table_df.to_csv('%s.csv' % test_table)
    print(table_df)
