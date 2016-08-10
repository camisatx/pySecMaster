import time
from datetime import datetime
import os
import pandas as pd
import psycopg2

from utilities.database_queries import df_to_sql, query_load_table,\
    update_load_table

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


class LoadTables(object):

    def __init__(self, database, user, password, host, port, tables_to_load,
                 load_tables='load_tables'):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.load_to_sql(tables_to_load, load_tables)

    @staticmethod
    def altered_values(existing_df, new_df):
        """ Compare the two provided DataFrames, returning a new DataFrame that
        only includes rows from the new_df that are different from the
        existing_df.

        :param existing_df: DataFrame of the existing values
        :param new_df: DataFrame of the next values
        :return: DataFrame with the altered/new values
        """

        # DataFrame with the similar values from both the existing_df and the
        #   new_df.
        combined_df = pd.merge(left=existing_df, right=new_df, how='inner',
                               on=list(new_df.columns.values))

        # In a new DataFrame, only keep the new_df rows that did NOT have a
        #   match to the existing_df
        id_col_name = list(new_df.columns.values)[0]
        altered_df = new_df[~new_df[id_col_name].isin(combined_df[id_col_name])]

        return altered_df

    def find_tsid(self, table_df):
        """ This only converts the stock's ticker to it's respective symbol_id.
        This requires knowing the ticker, the exchange and data vendor.

        :param table_df: DataFrame with the ticker and index
        :return: DataFrame with symbol_id's instead of tickers
        """
        
        try:
            conn = psycopg2.connect(database=self.database, user=self.user,
                                    password=self.password, host=self.host,
                                    port=self.port)
            with conn:
                cur = conn.cursor()
                # Determines if the quandl_codes table is empty? Stop if it is.
                cur.execute('SELECT q_code FROM quandl_codes LIMIT 1')
                if not cur.fetchall():
                    print('The quandl_codes table is empty. Run the code to '
                          'download the Quandl Codes and then run this again.')
                else:
                    table_df = self.find_symbol_id_process(table_df, cur)
                    return table_df
        except psycopg2.Error as e:
            print('Error when trying to retrieve data from the %s database '
                  'in LoadTables.find_q_code')
            print(e)

    @staticmethod
    def find_symbol_id_process(table_df, cur):
        """ Finds the ticker's symbol_id. If the table provided has an exchange
        column, then the ticker and exchange will be used to find the
        symbol_id. The result should be a perfect match to the quandl_codes 
        table. If an exchange column doesn't exist, then only the ticker will
        be used, along with an implied US exchange. Thus, only tickers traded
        on US exchanges will have their symbol_id's found. A way around this is
        to provide the exchange in the load file.

        :param table_df: A DataFrame with each row a ticker plus extra items
        :param cur: A cursor for navigating the SQL database.
        :return: A DataFrame with the original ticker replaced with a symbol_id
        """

        if 'exchange' in table_df.columns:
            # ToDo: Find a new source for the tickers table

            cur.execute("""SELECT symbol_id, component, data
                           FROM quandl_codes""")
            data = cur.fetchall()
            q_codes_df = pd.DataFrame(data, columns=['symbol_id', 'ticker',
                                                     'exchange'])
            q_codes_df.drop_duplicates('symbol_id', inplace=True)

            # Match the rows that have the same ticker and exchange
            df = pd.merge(table_df, q_codes_df, how='inner',
                          on=['ticker', 'exchange'])

            df = df[['symbol_id', 'ticker', 'exchange', 'sector', 'industry',
                     'sub_industry', 'currency', 'hq_country', 'created_date',
                     'updated_date']]

        else:
            exchanges = ['NYSE', 'NYSEMKT', 'NYSEARCA', 'NASDAQ']

            cur.execute("""SELECT symbol_id, component, data
                           FROM quandl_codes""")
            data = cur.fetchall()
            q_codes_df = pd.DataFrame(data, columns=['symbol_id', 'ticker',
                                                     'exchange'])
            q_codes_df.drop_duplicates('symbol_id', inplace=True)

            # Match the rows that have the same ticker and exchange
            # Broke the merge into two steps, involving an intermediary table
            df = pd.merge(table_df, q_codes_df, how='left', on='ticker')
            df = df[df['exchange'].isin(exchanges)]

            df = df.drop(['ticker', 'exchange'], axis=1)
            df.rename(columns={'index': 'stock_index'}, inplace=True)
            df = df[['stock_index', 'symbol_id', 'as_of', 'created_date',
                     'updated_date']]

        # ToDo: Implement a way to show the tickers that are not included
        return df

    def load_to_sql(self, tables_to_load, table_location):
        """ The main function that processes and loads the auxiliary data into
        the database. For each table listed in the tables_to_load list, their
        CSV file is loaded and the data moved into the SQL database. If the
        table is for indices, the CSV data is passed to the find_symbol_id
        function, where the ticker is replaced with it's respective symbol_id.

        :param tables_to_load: List of strings
        :param table_location: String of the directory for the load tables
        :return: Nothing. Data is just loaded into the SQL database.
        """

        start_time = time.time()
        for table, query in tables.items():
            if table in tables_to_load:
                try:
                    file = os.path.abspath(os.path.join(table_location,
                                                        table + '.csv'))
                    table_df = pd.read_csv(file, encoding='ISO-8859-1')
                except Exception as e:
                    print('Unable to load the %s csv load file. Skipping it' %
                          table)
                    print(e)
                    continue

                if table == 'indices' or table == 'tickers':
                    # ToDo: Re-implement these tables; need symbol_id
                    print('Unable to process indices and tickers table '
                          'since there is no system to create a unique '
                          'symbol_id for each item.')
                    continue
                    # # Removes the column that has the company's name
                    # table_df.drop('ticker_name', 1, inplace=True)
                    # # Finds the tsid for each ticker
                    # table_df = self.find_tsid(table_df)

                    # if table == 'tickers':
                    #     table_df.to_csv('load_tables/tickers_df.csv',
                    #                     index=False)

                # Retrieve any existing values for this table
                existing_df = query_load_table(
                    database=self.database, user=self.user,
                    password=self.password, host=self.host, port=self.port,
                    table=table)

                # Find the values that are different between the two DataFrames
                altered_df = self.altered_values(
                    existing_df=existing_df, new_df=table_df)

                altered_df.insert(len(altered_df.columns), 'created_date',
                                  datetime.now().isoformat())
                altered_df.insert(len(altered_df.columns), 'updated_date',
                                  datetime.now().isoformat())

                # Get the id column for the current table (first column)
                id_col_name = list(altered_df.columns.values)[0]

                # Separate out the new and updated values from the altered_df
                new_df = (altered_df[~altered_df[id_col_name].
                          isin(existing_df[id_col_name])])
                updated_df = (altered_df[altered_df[id_col_name].
                              isin(existing_df[id_col_name])])

                # Update all modified values within the database
                update_load_table(database=self.database, user=self.user,
                                  password=self.password, host=self.host,
                                  port=self.port, values_df=updated_df,
                                  table=table)

                # Append all new values to the database
                df_to_sql(database=self.database, user=self.user,
                          password=self.password, host=self.host,
                          port=self.port, df=new_df, sql_table=table,
                          exists='append', item=table)

                print('Loaded %s into the %s database' %
                      (table, self.database))

        load_tables_excluded = [table for table in tables_to_load
                                if table not in tables.keys()]
        if load_tables_excluded:
            print('Unable to load the following tables: %s' %
                  (", ".join(load_tables_excluded)))
            print("If the CSV file exists, make sure it's name matches the "
                  "name in the tables dictionary.")

        print('Finished loading all selected tables taking %0.1f seconds'
              % (time.time() - start_time))


# NOTE: make sure the table name (dict key) matches the csv load file name
tables = {
    'data_vendor': '(%s,%s,%s,%s,%s,%s,%s,%s)',
    'exchanges': '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
    'tickers': '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
    'indices': '(NULL,%s,%s,%s,%s,%s,%s)',
}
