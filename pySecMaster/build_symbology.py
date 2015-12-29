from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import time

from create_tables import main_tables, data_tables, events_tables
from load_aux_tables import LoadTables
from extractor import CSIDataExtractor, QuandlCodeExtract


def df_to_sql(df, db_location, sql_table, exists, item):

    # print('Entering the data for %s into the SQL database.' % (item,))
    conn = sqlite3.connect(db_location)

    # Try and except block writes the new data to the SQL Database.
    try:
        # if_exists options: append new df rows, replace all table values
        df.to_sql(sql_table, conn, if_exists=exists, index=False)
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("PRAGMA busy_timeout = 60000")
        # print('Successfully entered the Quandl Codes into the SQL Database')
    except conn.Error:
        conn.rollback()
        print("Failed to insert the DataFrame into the Database for %s" %
              (item,))
    except conn.OperationalError:
        raise ValueError('Unable to connect to the SQL Database in df_to_sql. '
                         'Make sure the database address/name are correct.')
    except Exception as e:
        print('Error: Unknown issue when adding DF to SQL for %s' % (item,))
        print(e)


def query_source_id(db_location, source_name):

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT data_vendor_id
                        FROM data_vendor
                        WHERE name=?""", (source_name,))
            source_id = cur.fetchone()
            if source_id:
                return source_id[0]
            else:
                return None
    except sqlite3.Error as e:
        print(e)
        raise SystemError('Failed to query the data from the data_vendor table '
                          'within query_source_id')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the SQL Database in '
                          'query_source_id. Make sure the database '
                          'address/name are correct.')
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_source_id')


def query_existing_sid(db_location):

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""SELECT symbol_id, source, source_id, type,
                        created_date, updated_date
                        FROM symbology""")
            rows = cur.fetchall()
            sid_df = pd.DataFrame(rows, columns=['symbol_id', 'source',
                                                   'source_id', 'type',
                                                   'created_date',
                                                   'updated_date'])
            return sid_df
    except sqlite3.Error as e:
        print(e)
        raise SystemError('Failed to query the data from the symbology table '
                          'within query_existing_sid')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the SQL Database in '
                          'query_existing_sid. Make sure the database '
                          'address/name are correct.')
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in query_existing_sid')


def query_csi_stock_factsheet(db_location, query='all'):

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            cur = conn.cursor()

            if query == 'all':
                cur.execute("""SELECT CsiNumber, Symbol, Exchange, ChildExchange
                               FROM csidata_stock_factsheet""")
                rows = cur.fetchall()
                csi_df = pd.DataFrame(rows, columns=['sid', 'ticker',
                                                     'exchange',
                                                     'childexchange'])
                csi_df.sort_values('sid', axis=0, inplace=True)
                csi_df.reset_index(drop=True, inplace=True)

            elif query == 'main_us':
                # Restricts tickers to those that have been active within the
                #   prior two years. For the few duplicate tickers, choose the
                #   active one over the non-active one (same company but
                #   different start and end dates, with one being active).
                beg_date = (datetime.utcnow() - timedelta(days=730))
                cur.execute("""SELECT CsiNumber, Symbol, Exchange, ChildExchange
                               FROM (SELECT CsiNumber, Symbol, Exchange,
                                   ChildExchange, IsActive
                                   FROM csidata_stock_factsheet
                                   WHERE EndDate > ?
                                   AND (Exchange IN ('AMEX', 'NYSE')
                                   OR ChildExchange IN ('AMEX',
                                       'BATS Global Markets',
                                       'Nasdaq Capital Market',
                                       'Nasdaq Global Market',
                                       'Nasdaq Global Select',
                                       'NYSE', 'NYSE ARCA'))
                                   ORDER BY IsActive ASC)
                               GROUP BY Symbol""", (beg_date.isoformat(),))
                rows = cur.fetchall()
                if rows:
                    csi_df = pd.DataFrame(rows, columns=['sid', 'ticker',
                                                         'exchange',
                                                         'childexchange'])
                else:
                    raise SystemExit('Not able to retrieve any tickers after '
                                     'querying %s in query_csi_stock_factsheet'
                                     % (query,))
            else:
                raise SystemExit('%s query does not exist within '
                                 'query_csi_stock_factsheet. Valid queries '
                                 'include all and main_us.' % (query,))
            return csi_df
    except sqlite3.Error as e:
        print(e)
        raise SystemError('Failed to query the data into the symbology table '
                          'within create_symbology')
    except conn.OperationalError:
        raise SystemError('Unable to connect to the SQL Database in '
                          'create_symbology. Make sure the database '
                          'address/name are correct.')
    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in create_symgology')


def insert_csi_data(db_location, df, source):

    conn = sqlite3.connect(db_location)
    try:
        with conn:
            for index, row in df.iterrows():
                symbol_id = int(row['symbol_id'])

                cur_time = datetime.utcnow().isoformat()
                cur = conn.cursor()
                cur.execute("""INSERT INTO symbology (symbol_id, source,
                            source_id, type, created_date, updated_date)
                            VALUES (?,?,?,?,?,?)""",
                            (symbol_id, source, symbol_id, 'stock',
                             cur_time, cur_time))
                conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        print('Failed to insert the data into the symbology table within '
              'insert_csi_data.')
        print(e)
    except conn.OperationalError:
        print('Unable to connect to the SQL Database in insert_csi_data. Make '
              'sure the database address/name are correct.')
    except Exception as e:
        print(e)
        print('Error: Unknown issue occurred in insert_csi_data')


def create_symbology(db_location, source_list):
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

    :param db_location: String of the database directory location
    :param source_list: List of strings with the symbology sources to use
    """

    # Retrieve any existing ID values from the symbology table
    symbology_df = query_existing_sid(db_location=db_location)

    for source in source_list:
        source_start = time.time()

        # # Get the ID number for the source
        # source_id = query_source_id(db_location=db_location,
        #                             source_name=source)
        # if not source_id:
        #     raise SystemError('There is not a match for the source ID. Add '
        #                       'the %s to the data_vendor table before trying '
        #                       'to build the symbology table.' % source)

        # A DF of all the values except the current source, which will be
        #   rebuilt and appended to this DF before being added to the database
        other_symbology_df = symbology_df[symbology_df['source'] != source]

        cur_time = datetime.utcnow().isoformat()
        if source == 'csi_data':
            csi_stock_df = query_csi_stock_factsheet(db_location=db_location,
                                                     query='all')

            source_sym_df = pd.DataFrame()
            source_sym_df.insert(0, 'symbol_id', csi_stock_df['sid'])
            source_sym_df.insert(1, 'source', source)
            source_sym_df.insert(2, 'source_id', csi_stock_df['sid'])
            source_sym_df.insert(3, 'type', 'stock')
            source_sym_df.insert(len(source_sym_df.columns), 'created_date',
                                 cur_time)
            source_sym_df.insert(len(source_sym_df.columns), 'updated_date',
                                 cur_time)

            # Append this new DF to the non-changing sources DF
            new_symbology_df = other_symbology_df.append(source_sym_df,
                                                         ignore_index=True)
            df_to_sql(df=new_symbology_df, db_location=db_location,
                      sql_table='symbology', exists='replace', item=source)

        elif source in ['quandl_wiki']:
            # DataFrame of main US active tickers and their exchange/child exch
            csi_stock_df = query_csi_stock_factsheet(db_location=db_location,
                                                     query='main_us')

            if source == 'quandl_wiki':
                # I don't trust that Quandl provides all available WIKI codes
                #   in the downloadable tables, thus I imply plausible WIKI
                #   codes: NYSE and NASDAQ that are active or recently delisted.

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

                # Need to add 'WIKI/' before every ticker to make it
                #   compatible with the Quandl WIKI code structure
                csi_stock_df['ticker'] = csi_stock_df['ticker'].\
                    apply(lambda x: 'WIKI/' + x)

                source_sym_df = pd.DataFrame()
                source_sym_df.insert(0, 'symbol_id', csi_stock_df['sid'])
                source_sym_df.insert(1, 'source', source)
                source_sym_df.insert(2, 'source_id', csi_stock_df['ticker'])
                source_sym_df.insert(3, 'type', 'stock')
                source_sym_df.insert(len(source_sym_df.columns), 'created_date',
                                     cur_time)
                source_sym_df.insert(len(source_sym_df.columns), 'updated_date',
                                     cur_time)

                # Append this new DF to the non-changing sources DF
                new_symbology_df = other_symbology_df.append(source_sym_df,
                                                             ignore_index=True)
                df_to_sql(df=new_symbology_df, db_location=db_location,
                          sql_table='symbology', exists='replace', item=source)

        print('Finished processing the symbology IDs for %s taking '
              '%0.2f seconds' % (source, (time.time() - source_start)))

    print('Added all %i sources to the symbology table.' % (len(source_list),))


if __name__ == '__main__':

    database_location = 'C:/Users/Josh/Desktop/pySecMaster_test.db'

    main_tables(database_location)
    data_tables(database_location)
    events_tables(database_location)

    LoadTables(database_location, ['data_vendor', 'exchanges'])

    csidata_url = 'http://www.csidata.com/factsheets.php?'
    csidata_type = 'stock'
    CSIDataExtractor(db_location=database_location, db_url=csidata_url,
                     data_type=csidata_type, redownload_time=3000)

    quandl_code = 'PXoR3E7FucJmxd8RFDkZ'
    quandl_db_url = ['https://www.quandl.com/api/v2/datasets.csv?query=*&'
                     'source_code=', '&per_page=300&page=']
    quandl_db_list = ['WIKI']    # WIKI, GOOG, YAHOO, SEC, FINRA
    QuandlCodeExtract(db_location=database_location, quandl_token=quandl_code,
                      database_list=quandl_db_list, database_url=quandl_db_url,
                      update_range=3000, threads=4)

    symbology_sources = ['csi_data', 'quandl_wiki']    # csi_data, quandl_wiki
    create_symbology(database_location, symbology_sources)
