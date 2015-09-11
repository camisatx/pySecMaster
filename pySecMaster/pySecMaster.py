from create_tables import main_tables, stock_tables
from extractor import QuandlCodeExtract
from load_aux_tables import LoadTables
from extractor import QuandlDataExtraction, GoogleFinanceDataExtraction

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2015 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.1'

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

''' pySecMaster.py

This manages the securities master database. It can be run daily.

Database maintenance tasks:
    Creates the tables in the database.
    Downloads all available Quandl Codes for the Quandl Databases selected.
    Loads auxiliary tables from included CSV files.

Database data download tasks:
    Downloads Quandl data based on the download selection criteria.
    Downloads Google Finance minute stock data.
    Can either append only the new data, or replace part of the existing data.

Future expansions:
    Implement daily option chain data (from Google or Yahoo)
    Further link the same data via unique code IDs
'''

###############################################################################
# General options:

# Go to quandl.com to signup for a free account to get a Quandl API Token
# NOTE: DELETE THIS TOKEN BEFORE CONTRIBUTING CODE; keep it confidential!
quandl_token = 'XXXXXXXXXXX'

# Specify the name of the Security Master database
# The name must have underscores instead of spaces and must have '.db' on end
database_name = 'pySecMaster.db'

# Change the location for where the database will be created
# Example: 'C:/Users/XXXXXX/Desktop/'; change '\' to '/'
database_location = 'C:/Users/XXXX/Desktop/'

database_link = database_location + database_name

###############################################################################
# Database maintenance options:

# These are the Quandl Databases that will have all their codes downloaded
# Examples: 'GOOG', 'WIKI', 'YAHOO', 'SEC', 'EIA', 'JODI', 'CURRFX', 'FINRA'
# ToDo: Determine how to handle Futures; codes are a single item w/o a '_'
# ToDo: Determine how to handle USDAFAS; codes have 3 item formats
database_list = ['WIKI']

# Integer that represents the number of days before the Quandl Codes will be
# refreshed. In addition, if a database wasn't completely downloaded within
# this data range, the remainder of the codes will be attempted to download.
update_range = 30

# Don't change these unless you know what you are doing
database_url = ['https://www.quandl.com/api/v2/datasets.csv?query=*&'
                'source_code=', '&per_page=300&page=']
tables_to_load = ['data_vendor', 'exchanges']

###############################################################################
# Database data download options:

# What source should the data be downloaded from? Quandl and/or Google Fin?
# Examples: 'all', 'quandl', 'google_fin'
download_source = 'quandl'

# Specify the items that will have their data downloaded.
# Examples: 'all', 'us_only', 'us_main'
# To add a field or understand what is actually being downloaded, go to
# 	query_q_codes in extractor.py, and look at the SQLite queries.
quandl_selection = 'wiki'
google_fin_selection = 'us_main_goog'

# Specify the time in seconds before the data is allowed to be re-downloaded.
redownload_time = 60 * 60 * 72      # 72 hours
google_fin_redwnld_time = 60 * 60 * 48      # 48 hours

# Should the latest data point be appended to the table, or should the new
#   data replace the prior x days of data. If data_process is set to append,
#   then the days_back variable will be ignored.
# NOTE: Due to weekends, the days replaced may differ depending on what day
#   this function is run (I don't think the extra code is worth the space).
# Examples: 'append', 'replace'
data_process = 'replace'
quandl_days_back = 30
google_fin_days_back = 5

# Don't change these unless you know what you are doing
quandl_data_url = ['https://www.quandl.com/api/v1/datasets/', '.csv']
google_fin_url = {'root': 'http://www.google.com/finance/getprices?',
                  'ticker': 'q=',
                  'exchange': 'x=',
                  'interval': 'i=60',   # 60 seconds; the shortest interval
                  # 'sessions': 'sessions=ext_hours',
                  'period': 'p=20d',    # 15 days; longest period for 1m data
                  'fields': 'f=d,c,v,o,h,l'}    # order doesn't change anything
###############################################################################


def maintenance():

    print('Starting Security Master table maintenance function. This can take '
          'some time to finish if large databases are used. If this fails, '
          'rerun it after a few minutes.')

    # Create the SQL tables, if they don't already exist
    main_tables(database_link)
    stock_tables(database_link)

    QuandlCodeExtract(database_link, quandl_token, database_list, database_url,
                      update_range)

    LoadTables(database_link, tables_to_load)


def data_download():

    if download_source in ['all', 'quandl']:
        # Download data for selected Quandl Codes
        print('\nDownloading all Quandl fields for: %s \nNew data will %s the '
              'prior %s days data'
              % (quandl_selection, data_process, quandl_days_back))
        QuandlDataExtraction(database_link, quandl_token, quandl_data_url,
                             quandl_selection, redownload_time, data_process,
                             quandl_days_back)

    if download_source in ['all', 'google_fin']:
        # Download minute data for selected Google Finance codes
        print('\nDownloading all Google Finance fields for: %s \nNew data will '
              '%s the prior %s day''s data'
              % (google_fin_selection, data_process, google_fin_days_back))
        GoogleFinanceDataExtraction(database_link, google_fin_url,
                                    google_fin_selection,
                                    google_fin_redwnld_time, data_process,
                                    google_fin_days_back)

if __name__ == '__main__':
    maintenance()
    data_download()
