from datetime import datetime
import pandas as pd
import time

from create_tables import create_database, main_tables, data_tables, \
    events_tables
from load_aux_tables import LoadTables
from extractor import CSIDataExtractor, QuandlCodeExtract
from utilities.database_queries import df_to_sql, query_csi_stocks, \
    query_existing_sid, query_exchanges, update_symbology_values

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.4.2'

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


def altered_values(existing_df, new_df):
    """ Compare the two provided DataFrames, returning a new DataFrame that only
    includes rows from the new_df that are different from the existing_df.

    :param existing_df: DataFrame of the existing values
    :param new_df: DataFrame of the next values
    :return: DataFrame with the altered/new values
    """

    # Convert both DataFrames to all string objects. Normally, the symbol_id
    #   column of the existing_df is an int64 object, messing up the merge
    if len(existing_df.index) > 0:
        existing_df = existing_df.applymap(str)
    new_df = new_df.applymap(str)

    # DataFrame with the similar values from both the existing_df and the
    #   new_df. The comparison is based on the symbol_id/sid and
    #   source_id/ticker columns.
    combined_df = pd.merge(left=existing_df, right=new_df, how='inner',
                           left_on=['symbol_id', 'source_id'],
                           right_on=['sid', 'ticker'])

    # In a new DataFrame, only keep the new_df rows that did NOT have a match
    #   to the existing_df
    altered_df = new_df[~new_df['sid'].isin(combined_df['sid'])]

    return altered_df


def create_symbology(database, user, password, host, port, source_list):
    """
    Create the symbology table. Use the CSI numbers as the unique symbol
    identifiers. See if they already exist within the symbology table, and if
    not, add them. For the source, use either 'csi_data' or the data vendor ID.
    For the source_id, use the actual CSI number.

    After the initial unique symbols are created, map the Quandl codes to their
    respective symbol ID. This can be done using the ticker and exchange
    combination, as it will always be unique (for active tickers...). For
    Quandl codes that don't have a match, create a unique symbol ID for them
    in a seperate number range (1M - 2M; B#). Use this same matching structure
    for mapping basket items.

    Steps:
    1. Download the CSI stockfacts data and store in database
    2. Add the CSI data's number ID to the symbology table if it hasn't been
        done yet
    3. Map Quandl codes to a unique ID

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param source_list: List of strings with the symbology sources to use
    """

    exch_df = query_exchanges(database=database, user=user, password=password,
                              host=host, port=port)

    # ToDo: Add economic_events codes

    for source in source_list:
        source_start = time.time()

        # Retrieve any existing ID values from the symbology table
        existing_symbology_df = query_existing_sid(database=database, user=user,
                                                   password=password, host=host,
                                                   port=port, source=source)

        if source == 'csi_data':
            csi_stock_df = query_csi_stocks(database=database, user=user,
                                            password=password, host=host,
                                            port=port, query='all')

            # csi_data is unique where the sid (csi_num) is the source_id
            csi_stock_df['ticker'] = csi_stock_df['sid']

            # Find the values that are different between the two DataFrames
            altered_values_df = altered_values(
                existing_df=existing_symbology_df, new_df=csi_stock_df)

            # Prepare a new DataFrame with all relevant data for these values
            altered_df = pd.DataFrame()
            altered_df.insert(0, 'symbol_id', altered_values_df['sid'])
            altered_df.insert(1, 'source', source)
            altered_df.insert(2, 'source_id', altered_values_df['sid'])
            altered_df.insert(3, 'type', 'stock')
            altered_df.insert(len(altered_df.columns), 'created_date',
                              datetime.now().isoformat())
            altered_df.insert(len(altered_df.columns), 'updated_date',
                              datetime.now().isoformat())

        elif source in ['tsid', 'quandl_wiki', 'quandl_goog', 'seeking_alpha',
                        'yahoo']:
            # These sources have a similar symbology creation process

            if source == 'quandl_wiki':
                # I don't trust that Quandl provides all available WIKI codes
                #   in the downloadable tables, thus I imply plausible WIKI
                #   codes: NYSE and NASDAQ that are active or recently delisted.

                # DataFrame of main US active tickers, their exchanges and
                #   sub exchanges
                csi_stock_df = query_csi_stocks(database=database, user=user,
                                                password=password, host=host,
                                                port=port, query='main_us')

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

                # Need to add 'WIKI/' before every ticker to make it
                #   compatible with the Quandl WIKI code structure
                csi_stock_df['ticker'] = csi_stock_df['ticker'].\
                    apply(lambda x: 'WIKI/' + x)

            elif source == 'quandl_goog':
                # Imply plausible Quandl codes for their GOOG database. Only
                #   codes for American, Canadian and London exchanges
                csi_stock_df = query_csi_stocks(database=database, user=user,
                                                password=password, host=host,
                                                port=port,
                                                query='exchanges_only')

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

                def csi_to_quandl_goog(row):
                    # Create the Quandl GOOG symbol combination of
                    #   GOOG/<goog exchange symbol>_<ticker>
                    ticker = row['ticker']
                    exchange = row['exchange']
                    sub_exchange = row['sub_exchange']

                    if sub_exchange == 'NYSE ARCA':
                        # NYSE ARCA is a special situation where the sub
                        #   exchange matches the csi_symbol
                        goog_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                     sub_exchange, 'goog_symbol'].values)
                        if goog_exch:
                            return 'GOOG/' + goog_exch[0] + '_' + ticker
                        else:
                            print('Unable to find the goog exchange symbol for '
                                  'the sub exchange %s in csi_to_quandl_goog'
                                  % sub_exchange)
                    elif sub_exchange == 'NYSE Mkt':
                        # AMEX changed to NYSE Mkt, but only the sub exchange
                        #   from csi data is showing this, not the exchange.
                        #   Thus, the exchanges table will continue using AMEX.
                        goog_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                     'AMEX', 'goog_symbol'].values)
                        if goog_exch:
                            return 'GOOG/' + goog_exch[0] + '_' + ticker
                        else:
                            print('Unable to find the goog exchange symbol for '
                                  'the sub exchange NYSE Mkt in '
                                  'csi_to_quandl_goog')
                    elif sub_exchange == 'Alberta Stock Exchange':
                        # All stocks with the Alberta Stock Exchange as the
                        #   sub exchange were all delisted prior to 2004
                        pass
                    else:
                        # For all non NYSE ARCA exchanges, see if there is a
                        #   sub exchange and if so, try matching that to the
                        #   csi_symbol (first) or name (second). If no sub
                        #   exchange, try matching to the csi_symbol.

                        if sub_exchange:
                            # (exch: AMEX | chld_exch: NYSE)
                            goog_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                         sub_exchange, 'goog_symbol'].values)
                            if goog_exch:
                                return 'GOOG/' + goog_exch[0] + '_' + ticker
                            else:
                                # (exch: NYSE | chld_exch: OTC Markets QX)
                                # (exch: AMEX | chld_exch: BATS Global Markets)
                                goog_exch = (exch_df.loc[exch_df['name'] ==
                                             sub_exchange, 'goog_symbol'].
                                             values)
                                if goog_exch:
                                    return 'GOOG/' + goog_exch[0] + '_' + ticker
                                else:
                                    print('Unable to find the goog exchange '
                                          'symbol for the sub exchange %s in '
                                          'csi_to_quandl_goog. Will try to '
                                          'find a match for the exchange now.'
                                          % sub_exchange)
                                    # If there is an exchange, try to match that

                        if exchange:
                            # Either no sub exchange or the sub exchange
                            #   never found a match
                            goog_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                         exchange, 'goog_symbol'].values)
                            if goog_exch:
                                return 'GOOG/' + goog_exch[0] + '_' + ticker
                            else:
                                print('Unable to find the goog exchange symbol '
                                      'for the exchange %s in '
                                      'csi_to_quandl_goog' % exchange)
                        else:
                            print('Unable to find the goog exchange symbol for '
                                  'either the exchange or sub exchange for '
                                  '%s:%s' % (exchange, sub_exchange))

                csi_stock_df['ticker'] = csi_stock_df.apply(csi_to_quandl_goog,
                                                            axis=1)

            elif source == 'seeking_alpha':
                # Use main US tickers that should have Seeking Alpha articles
                csi_stock_df = query_csi_stocks(database=database, user=user,
                                                password=password, host=host,
                                                port=port, query='main_us')

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

            elif source == 'tsid':
                # Build tsid codes (<ticker>.<exchange>.<position>), albeit
                #   only for American, Canadian and London exchanges.
                csi_stock_df = query_csi_stocks(database=database, user=user,
                                                password=password, host=host,
                                                port=port,
                                                query='exchanges_only')

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

                def csi_to_tsid(row):
                    # Create the tsid symbol combination of:
                    #   <ticker>.<tsid exchange symbol>.<count>
                    ticker = row['ticker']
                    exchange = row['exchange']
                    sub_exchange = row['sub_exchange']

                    if sub_exchange == 'NYSE ARCA':
                        # NYSE ARCA is a special situation where the sub
                        #   exchange matches the csi_symbol
                        tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                     sub_exchange, 'tsid_symbol'].values)
                        if tsid_exch:
                            return ticker + '.' + tsid_exch[0] + '.0'
                        else:
                            print('Unable to find the tsid exchange symbol for '
                                  'the sub exchange %s in csi_to_tsid' %
                                  sub_exchange)
                    elif sub_exchange == 'NYSE Mkt':
                        # AMEX changed to NYSE Mkt, but only the sub exchange
                        #   from csi data is showing this, not the exchange.
                        #   Thus, the exchanges table will continue using AMEX.
                        tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                                 'AMEX', 'tsid_symbol'].values)
                        if tsid_exch:
                            return ticker + '.' + tsid_exch[0] + '.0'
                        else:
                            print('Unable to find the tsid exchange symbol for '
                                  'the sub exchange NYSE Mkt in csi_to_tsid')
                    else:
                        # For all non NYSE ARCA exchanges, see if there is a
                        #   sub exchange and if so, try matching that to the
                        #   csi_symbol (first) or name (second). If no sub
                        #   exchange, try matching to the csi_symbol.
                        if sub_exchange:
                            # (exch: AMEX | chld_exch: NYSE)
                            tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                         sub_exchange, 'tsid_symbol'].values)
                            if tsid_exch:
                                return ticker + '.' + tsid_exch[0] + '.0'
                            else:
                                # (exch: NYSE | chld_exch: OTC Markets QX)
                                # (exch: AMEX | chld_exch: BATS Global Markets)
                                tsid_exch = (exch_df.loc[exch_df['name'] ==
                                             sub_exchange, 'tsid_symbol'].
                                             values)
                                if tsid_exch:
                                    return ticker + '.' + tsid_exch[0] + '.0'
                                else:
                                    print('Unable to find the tsid exchange '
                                          'symbol for the sub exchange %s in '
                                          'csi_to_tsid. Will try to '
                                          'find a match for the exchange now.'
                                          % sub_exchange)
                                    # If there is an exchange, try to match that
                        if exchange:
                            # Either no sub exchange or the sub exchange
                            #   never found a match
                            tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                         exchange, 'tsid_symbol'].values)
                            if tsid_exch:
                                return ticker + '.' + tsid_exch[0] + '.0'
                            else:
                                print('Unable to find the tsid exchange symbol '
                                      'for the exchange %s in csi_to_tsid' %
                                      exchange)
                        else:
                            print('Unable to find the tsid exchange symbol for '
                                  'either the exchange or sub exchange for '
                                  '%s:%s' % (exchange, sub_exchange))

                csi_stock_df['ticker'] = csi_stock_df.apply(csi_to_tsid, axis=1)

            elif source == 'yahoo':
                # Imply plausible Yahoo codes, albeit only for American,
                #   Canadian and London exchanges.
                csi_stock_df = query_csi_stocks(database=database, user=user,
                                                password=password, host=host,
                                                port=port,
                                                query='exchanges_only')

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

                def csi_to_yahoo(row):
                    # Create the Yahoo symbol combination of <ticker>.<exchange>

                    ticker = row['ticker']
                    exchange = row['exchange']
                    sub_exchange = row['sub_exchange']
                    us_exchanges = ['AMEX', 'BATS Global Markets',
                                    'Nasdaq Capital Market',
                                    'Nasdaq Global Market',
                                    'Nasdaq Global Select',
                                    'NYSE', 'NYSE ARCA']

                    if exchange in ['AMEX', 'NYSE'] \
                            or sub_exchange in us_exchanges:
                        return ticker           # US ticker; no exchange needed
                    elif exchange == 'LSE':
                        return ticker + '.L'    # LSE -> L
                    elif exchange == 'TSX':
                        return ticker + '.TO'   # TSX -> TO
                    elif exchange == 'VSE':
                        return ticker + '.V'    # VSE -> V
                    elif sub_exchange == 'OTC Markets Pink Sheets':
                        return ticker + '.PK'   # OTC Pinks -> PK
                    else:
                        print('csi_to_yahoo did not find a match for %s with an'
                              'exchange of %s and a sub exchange of %s' %
                              (ticker, exchange, sub_exchange))

                csi_stock_df['ticker'] = csi_stock_df.apply(csi_to_yahoo,
                                                            axis=1)

            else:
                return NotImplementedError('%s is not implemented in the '
                                           'create_symbology function of '
                                           'build_symbology.py' % source)

            # Remove post processed duplicates to prevent database FK errors
            csi_stock_df.drop_duplicates(subset=['ticker'], inplace=True)

            # Find the values that are different between the two DataFrames
            altered_values_df = altered_values(
                existing_df=existing_symbology_df, new_df=csi_stock_df)

            # Prepare a new DataFrame with all relevant data for these values
            altered_df = pd.DataFrame()
            altered_df.insert(0, 'symbol_id', altered_values_df['sid'])
            altered_df.insert(1, 'source', source)
            altered_df.insert(2, 'source_id', altered_values_df['ticker'])
            altered_df.insert(3, 'type', 'stock')
            altered_df.insert(len(altered_df.columns), 'created_date',
                              datetime.now().isoformat())
            altered_df.insert(len(altered_df.columns), 'updated_date',
                              datetime.now().isoformat())

        else:
            return NotImplementedError('%s is not implemented in the '
                                       'create_symbology function of '
                                       'build_symbology.py' % source)

        # Separate out the updated values from the altered_df
        updated_symbols_df = (altered_df[altered_df['symbol_id'].
                              isin(existing_symbology_df['symbol_id'])])
        # Update all modified symbology values in the database
        update_symbology_values(database=database, user=user, password=password,
                                host=host, port=port,
                                values_df=updated_symbols_df)

        # Separate out the new values from the altered_df
        new_symbols_df = (altered_df[~altered_df['symbol_id'].
                          isin(existing_symbology_df['symbol_id'])])
        # Append the new symbol values to the existing database table
        df_to_sql(database=database, user=user, password=password, host=host,
                  port=port, df=new_symbols_df, sql_table='symbology',
                  exists='append', item=source)

        print('Finished processing the symbology IDs for %s taking '
              '%0.2f seconds' % (source, (time.time() - source_start)))

    print('Added all %i sources to the symbology table.' % (len(source_list),))


if __name__ == '__main__':

    from utilities.user_dir import user_dir
    userdir = user_dir()

    create_database(database=userdir['postgresql']['pysecmaster_db'],
                    user=userdir['postgresql']['pysecmaster_user'])
    main_tables(database=userdir['postgresql']['pysecmaster_db'],
                user=userdir['postgresql']['pysecmaster_user'],
                password=userdir['postgresql']['pysecmaster_password'],
                host=userdir['postgresql']['pysecmaster_host'],
                port=userdir['postgresql']['pysecmaster_port'])
    data_tables(database=userdir['postgresql']['pysecmaster_db'],
                user=userdir['postgresql']['pysecmaster_user'],
                password=userdir['postgresql']['pysecmaster_password'],
                host=userdir['postgresql']['pysecmaster_host'],
                port=userdir['postgresql']['pysecmaster_port'])
    events_tables(database=userdir['postgresql']['pysecmaster_db'],
                  user=userdir['postgresql']['pysecmaster_user'],
                  password=userdir['postgresql']['pysecmaster_password'],
                  host=userdir['postgresql']['pysecmaster_host'],
                  port=userdir['postgresql']['pysecmaster_port'])

    LoadTables(database=userdir['postgresql']['pysecmaster_db'],
               user=userdir['postgresql']['pysecmaster_user'],
               password=userdir['postgresql']['pysecmaster_password'],
               host=userdir['postgresql']['pysecmaster_host'],
               port=userdir['postgresql']['pysecmaster_port'],
               tables_to_load=['data_vendor', 'exchanges'])

    csidata_url = 'http://www.csidata.com/factsheets.php?'
    csidata_type = 'stock'
    CSIDataExtractor(database=userdir['postgresql']['pysecmaster_db'],
                     user=userdir['postgresql']['pysecmaster_user'],
                     password=userdir['postgresql']['pysecmaster_password'],
                     host=userdir['postgresql']['pysecmaster_host'],
                     port=userdir['postgresql']['pysecmaster_port'],
                     db_url=csidata_url, data_type=csidata_type,
                     redownload_time=3000)

    quandl_db_url = ['https://www.quandl.com/api/v2/datasets.csv?query=*&'
                     'source_code=', '&per_page=300&page=']
    quandl_db_list = ['WIKI']    # WIKI, GOOG, YAHOO, SEC, FINRA
    # QuandlCodeExtract(database=userdir['postgresql']['pysecmaster_db'],
    #                   user=userdir['postgresql']['pysecmaster_user'],
    #                   password=userdir['postgresql']['pysecmaster_password'],
    #                   host=userdir['postgresql']['pysecmaster_host'],
    #                   port=userdir['postgresql']['pysecmaster_port'],
    #                   quandl_token=userdir['quandl']['quandl_token'],
    #                   database_list=quandl_db_list,
    #                   database_url=quandl_db_url,
    #                   update_range=3000, threads=4)

    symbology_sources = ['csi_data', 'quandl_wiki', 'seeking_alpha', 'tsid',
                         'yahoo', 'quandl_goog']
    create_symbology(database=userdir['postgresql']['pysecmaster_db'],
                     user=userdir['postgresql']['pysecmaster_user'],
                     password=userdir['postgresql']['pysecmaster_password'],
                     host=userdir['postgresql']['pysecmaster_host'],
                     port=userdir['postgresql']['pysecmaster_port'],
                     source_list=symbology_sources)
