from build_symbology import create_symbology
from extractor import CSIDataExtractor
from load_aux_tables import LoadTables
from utilities.user_dir import user_dir


__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2018 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.5.0'

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


def build_symbology(database_options):

    tables_to_load = ['data_vendor', 'exchanges']

    csidata_type = 'stock'  # stock, commodity
    csidata_update_range = 7

    # Don't change these unless you know what you are doing
    # http://www.csidata.com/factsheets.php?type=stock&format=html
    csidata_url = 'http://www.csidata.com/factsheets.php?'
    symbology_sources = ['csi_data', 'tsid', 'quandl_wiki', 'quandl_eod',
                         'quandl_goog', 'seeking_alpha', 'yahoo']

    LoadTables(database=database_options['database'],
               user=database_options['user'],
               password=database_options['password'],
               host=database_options['host'],
               port=database_options['port'],
               tables_to_load=tables_to_load)

    CSIDataExtractor(database=database_options['database'],
                     user=database_options['user'],
                     password=database_options['password'],
                     host=database_options['host'],
                     port=database_options['port'],
                     db_url=csidata_url,
                     data_type=csidata_type,
                     redownload_time=csidata_update_range)

    create_symbology(database=database_options['database'],
                     user=database_options['user'],
                     password=database_options['password'],
                     host=database_options['host'],
                     port=database_options['port'],
                     source_list=symbology_sources)

if __name__ == '__main__':

    userdir = user_dir()

    sayvmaster_database_options = {
        'admin_user': userdir['postgresql']['main_user'],
        'admin_password': userdir['postgresql']['main_password'],
        'database': userdir['postgresql']['sayvmaster_db'],
        'user': userdir['postgresql']['sayvmaster_user'],
        'password': userdir['postgresql']['sayvmaster_password'],
        'host': userdir['postgresql']['sayvmaster_host'],
        'port': userdir['postgresql']['sayvmaster_port'],
    }

    newsmaster_database_options = {
        'admin_user': userdir['postgresql']['main_user'],
        'admin_password': userdir['postgresql']['main_password'],
        'database': userdir['postgresql']['newsmaster_db'],
        'user': userdir['postgresql']['newsmaster_user'],
        'password': userdir['postgresql']['newsmaster_password'],
        'host': userdir['postgresql']['newsmaster_host'],
        'port': userdir['postgresql']['newsmaster_port'],
    }

    # build_symbology(database_options=sayvmaster_database_options)
    build_symbology(database_options=newsmaster_database_options)
