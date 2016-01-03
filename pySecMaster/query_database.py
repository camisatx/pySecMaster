import sqlite3
import pandas as pd
import time

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2015 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.2'

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


def query_entire_table(db_local, csv_dir, table):

    try:
        conn = sqlite3.connect(db_local)
        with conn:
            cur = conn.cursor()
            # query = ("""SELECT * FROM %s""" % (table,))
            query = ("""SELECT tsid FROM %s GROUP BY(tsid)""" % (table,))
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
    except sqlite3.Error as e:
        print(e)
        raise SystemError('Error: Not able to query the SQL DB')

    try:
        df.to_csv(csv_dir + table + '.csv')
    except FileNotFoundError:
        raise SystemError('Error: No directory found for the CSV dir provided')

    print('The CSV for the %s table was successfully saved in %s' %
          (table, csv_dir))

if __name__ == '__main__':

    start_time = time.time()

    db_local = 'C:/Users/Josh/Desktop/pySecMaster_d.db'
    csv_dir = 'C:/Users/Josh/Desktop/'
    table = 'daily_prices'      # daily_prices, minute_prices, quandl_codes

    query_entire_table(db_local, csv_dir, table)

    print('Query took %0.2f seconds' % (time.time() - start_time))
