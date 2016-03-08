from create_tables import main_tables, data_tables, events_tables
from extractor import QuandlCodeExtract, QuandlDataExtraction,\
    GoogleFinanceDataExtraction, CSIDataExtractor
from load_aux_tables import LoadTables
from build_symbology import create_symbology

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.3.1'

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
    Loads auxiliary tables from included CSV files.
    Downloads all available Quandl Codes for the Quandl Databases selected.
    Downloads the specified CSI Data factsheet (stocks, commodities).
    Creates the symbology table which establishes a unique code for every
        item in the database, along with translating different source's codes

Database data download tasks:
    Downloads Quandl data based on the download selection criteria using either
        the official Quandl Codes or implied codes from CSI Data.
    Downloads Google Finance minute stock data.
    Can either append only the new data, or replace part of the existing data.

Future expansions:
    Implement daily option chain data (from Google or Yahoo).
'''

###############################################################################
# Database maintenance options:

csidata_type = 'stock'      # stock, commodity

# Don't change these unless you know what you are doing
database_url = ['https://www.quandl.com/api/v2/datasets.csv?query=*&'
                'source_code=', '&per_page=300&page=']
# http://www.csidata.com/factsheets.php?type=stock&format=html
csidata_url = 'http://www.csidata.com/factsheets.php?'
tables_to_load = ['data_vendor', 'exchanges']
symbology_sources = ['csi_data', 'tsid', 'quandl_wiki', 'quandl_goog',
                     'seeking_alpha', 'yahoo']

###############################################################################
# Database data download options:

# Specify the time in seconds before the data is allowed to be re-downloaded.
redownload_time = 60 * 60 * 12      # 12 hours
google_fin_redwnld_time = 60 * 60 * 12      # 12 hours

# Should the latest data point be appended to the table, or should the new
#   data replace the prior x days of data. If data_process is set to append,
#   then the days_back variable will be ignored.
# NOTE: Due to weekends, the days replaced may differ depending on what day
#   this function is run (I don't think the extra code is worth the space).
# Examples: 'append', 'replace'
data_process = 'replace'
# ToDo: Make an efficient way for all historical adj_<price> values to update
quandl_days_back = 30000    # Forces all quandl values to be replaced
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


def maintenance(database_link, quandl_ticker_source, database_list, threads,
                quandl_key, quandl_update_range, csidata_update_range,
                symbology_sources):

    print('Starting Security Master table maintenance function. This can take '
          'some time to finish if large databases are used. If this fails, '
          'rerun it after a few minutes.')

    # Create the SQL tables, if they don't already exist
    main_tables(database_link)
    data_tables(database_link)
    events_tables(database_link)

    LoadTables(database_location=database_link, tables_to_load=tables_to_load)

    # Always extract CSI values, as they are used for the symbology table
    CSIDataExtractor(database_link, csidata_url, csidata_type,
                     csidata_update_range)

    if quandl_ticker_source == 'quandl':
        QuandlCodeExtract(database_link, quandl_key, database_list,
                          database_url, quandl_update_range, threads)

    create_symbology(db_location=database_link, source_list=symbology_sources)


def data_download(database_link, download_source, quandl_selection,
                  google_fin_selection, threads, quandl_key):

    if download_source in ['all', 'quandl']:
        # Download data for selected Quandl Codes
        print('\nDownloading all Quandl fields for: %s \nNew data will %s the '
              'prior %s days data'
              % (quandl_selection, data_process, quandl_days_back))
        QuandlDataExtraction(database_link, quandl_key, quandl_data_url,
                             quandl_selection, redownload_time, data_process,
                             quandl_days_back, threads)

    if download_source in ['all', 'google_fin']:
        # Download minute data for selected Google Finance codes
        print('\nDownloading all Google Finance fields for: %s \nNew data will '
              '%s the prior %s day''s data'
              % (google_fin_selection, data_process, google_fin_days_back))
        GoogleFinanceDataExtraction(database_link, google_fin_url,
                                    google_fin_selection,
                                    google_fin_redwnld_time, data_process,
                                    google_fin_days_back, threads)

if __name__ == '__main__':

    ############################################################################
    # General options:

    # Go to quandl.com to signup for a free account to get a Quandl API Token
    # NOTE: DELETE THIS TOKEN BEFORE CONTRIBUTING CODE; keep it confidential!
    quandl_token = 'XXXXXXXXXXX'

    # Specify the name of the Security Master database
    # Name must have underscores instead of spaces and must have '.db' on end
    database_name = 'pySecMaster_m.db'

    # Change the location for where the database will be created
    # Example: 'C:/Users/XXXXXX/Desktop/'; change '\' to '/' for Windows
    database_location = 'C:/Users/###/Programming/Databases/pySecMaster/'

    database_link = database_location + database_name

    ############################################################################
    # Database maintenance options:

    # These are the Quandl Databases that will have all their codes downloaded
    # Examples: 'GOOG', 'WIKI', 'YAHOO', 'SEC', 'EIA', 'JODI', 'CURRFX', 'FINRA'
    # ToDo: Determine how to handle Futures; codes are a single item w/o a '_'
    # ToDo: Determine how to handle USDAFAS; codes have 3 item formats
    database_list = ['WIKI']

    # Integer that represents the number of days before the ticker tables will
    #   be refreshed. In addition, if a database wasn't completely downloaded
    #   within this data range, the remainder of the codes will attempt to
    #   download.
    update_range = 30
    csidata_update_range = 5

    ############################################################################
    # Database data download options:

    # What source should the data be downloaded from? Quandl and/or Google Fin?
    # Examples: 'all', 'quandl', 'google_fin'
    download_source = 'google_fin'

    # When the Quandl data is downloaded, where should the extractor get the
    #   ticker codes from? Either use the official list of codes from Quandl
    #   (quandl), or make implied codes from the CSI Data stock factsheet
    #   (csidata), which is more accurate but tries more non-existent companies.
    quandl_ticker_source = 'csidata'        # quandl, csidata

    # Specify the items that will have their data downloaded. To add a field or
    #   to understand what is actually being downloaded, go to the query_q_codes
    #   method in either the QuandlDataExtraction class or the
    #   GoogleFinanceDataExtraction class in extractor.py, and look at the
    #   SQLite queries.
    # Options: 'quandl_wiki', 'quandl_goog', 'quandl_goog_etf'
    quandl_selection = 'quandl_wiki'
    # Google Fin options: 'all', 'us_main', 'us_canada_london'
    google_fin_selection = 'us_main'
    ############################################################################

    maintenance(database_link, quandl_ticker_source, database_list, 8,
                quandl_token, update_range, csidata_update_range,
                symbology_sources)

    data_download(database_link, download_source, quandl_selection,
                  google_fin_selection, 8, quandl_token)
