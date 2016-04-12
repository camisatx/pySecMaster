from datetime import datetime, timedelta
import operator
import pandas as pd
import time

from utilities.database_queries import delete_sql_table_rows, df_to_sql,\
    query_all_active_tsids, query_all_tsid_prices, query_source_weights,\
    retrieve_data_vendor_id
from utilities.multithread import multithread

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.3.2'

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


class CrossValidate(object):
    """ Compares the prices from multiple sources, storing the price with the
    highest consensus weight.
    """

    def __init__(self, db_location, table, tsid_list, period=None,
                 verbose=False):
        """
        :param db_location: String of the database file directory
        :param table: String of the database table that should be worked on
        :param tsid_list: List of strings, with each string being a tsid
        :param period: Optional integer indicating the number of days whose
            values should be cross validated. If None is provided, then the
            entire set of values will be validated.
        :param verbose: Boolean of whether to print debugging statements or not
        """

        self.db_location = db_location
        self.table = table
        self.tsid_list = tsid_list
        self.period = period
        self.verbose = verbose

        # Build a DataFrame with the source id and weight
        self.source_weights_df = query_source_weights(db_location=
                                                      self.db_location)

        # List of data vendor names to ignore when cross validating the data.
        #   Relevant when the data source has data that would be considered.
        self.source_exclude_list = ['pySecMaster_Consensus']

        self.source_id_exclude_list = []
        for source in self.source_exclude_list:
            source_id = retrieve_data_vendor_id(db_location=self.db_location,
                                                name=source)
            self.source_id_exclude_list.append(source_id)

        if self.verbose:
            if self.period:
                print('Running cross validator for %s tsids only for the prior '
                      '%i day\'s history.' % (len(tsid_list), self.period))
            else:
                print('Running cross validator for %s tsids for the entire '
                      'data history.' % (len(tsid_list),))

        self.main()

    def main(self):
        """ Start the tsid cross validator process using either single or
        multiprocessing. """

        validator_start = time.time()

        # Cycle through each tsid, running the data cross validator on all
        #   sources and fields available.
        """No multiprocessing"""
        # [self.validator(tsid=tsid) for tsid in self.tsid_list]
        """Multiprocessing using 4 threads"""
        multithread(self.validator, self.tsid_list, threads=4)

        if self.verbose:
            print('%i tsids have had their sources cross validated taking '
                  '%0.2f seconds.' %
                  (len(self.tsid_list), time.time() - validator_start))

    def validator(self, tsid):

        tsid_start = time.time()

        # DataFrame of all stored prices for this ticker and interval. This is
        #   a multi-index DataFrame, with date and data_vendor_id in the index.
        tsid_prices_df = query_all_tsid_prices(db_location=self.db_location,
                                               table=self.table, tsid=tsid)

        unique_sources = tsid_prices_df.index.\
            get_level_values('data_vendor_id').unique()
        unique_dates = tsid_prices_df.index.get_level_values('date').unique()

        # If a period is provided, limit the unique_dates list to only those
        #   within the past n period days.
        if self.period:
            beg_date = datetime.today() - timedelta(days=self.period)
            unique_dates = unique_dates[unique_dates > beg_date]

        # The consensus_price_df contains the prices from weighted consensus
        consensus_price_df = pd.DataFrame(columns=['date', 'open', 'high',
                                                   'low', 'close', 'volume'])
        # Set the date as the index
        consensus_price_df.set_index(['date'], inplace=True)

        # Cycle through each period, comparing each data source's prices
        for date in unique_dates:

            # Either add each field's consensus price to a dictionary,
            #   which is entered into the consensus_price_df upon all fields
            #   being processed, or enter each field's consensus price directly
            #   into the consensus_price_df. Right now, this is doing the later.
            # consensus_prices = {}

            try:
                # Create a DataFrame for the current period, with the source_ids
                #   as the index and the data_columns as the column headers
                period_df = tsid_prices_df.xs(date, level='date')
            except KeyError:
                # Should never happen
                print('Unable to extract the %s period\'s prices from '
                      'the tsid_prices_df for %s' % (date, tsid))
            finally:
                # Transpose the period_df DataFrame so the source_ids are
                #   columns and the price fields are the rows
                period_df = period_df.transpose()

                # Cycle through each price field for this period's values
                for field_index, field_data in period_df.iterrows():
                    # field_index: string of the index name
                    # field_data: Pandas Series (always??) of the field data

                    # Reset the field consensus for every field processed
                    field_consensus = {}

                    # Cycle through each source's values that are in the
                    #   field_data Series.
                    for source_data in field_data.iteritems():
                        # source_data is a tuple, with the first item is being
                        #   the data_vendor_id and the second being the value.

                        # If the source_data's id is in the exclude list, don't
                        #   use its price when calculating the field consensus.
                        if source_data[0] not in self.source_id_exclude_list:

                            # Retrieve the weighted consensus for this source
                            source_weight = self.source_weights_df.loc[
                                self.source_weights_df['data_vendor_id'] ==
                                source_data[0], 'consensus_weight']

                            try:
                                if field_consensus:
                                    # There is already a value for this field
                                    if source_data[1] in field_consensus:
                                        # This source's value has a match in the
                                        #   current consensus. Increase weight
                                        #   for this price.
                                        field_consensus[source_data[1]] += \
                                            source_weight.iloc[0]
                                    else:
                                        # The data value from the source does
                                        #   not match this field's consensus
                                        field_consensus[source_data[1]] = \
                                            source_weight.iloc[0]

                                else:
                                    # Add the first price to the field_consensus
                                    #   dictionary, using the price as the key
                                    #   and the source's weight as the item.
                                    field_consensus[source_data[1]] = \
                                        source_weight.iloc[0]
                            except IndexError:
                                # No source_weight was found, probably because
                                #   there was no data_vendor_id for this value
                                pass

                    # Insert the highest consensus value for this period into
                    #   the consensus_price_df (the dictionary key (price) with
                    #   the largest value (consensus sum).
                    consensus_value = max(field_consensus.items(),
                                          key=operator.itemgetter(1))[0]
                    consensus_price_df.ix[date, field_index] = consensus_value

        def datetime_to_iso(row, column):
            return row[column].isoformat()

        # Make the date index into a normal column
        consensus_price_df.reset_index(inplace=True)
        # Convert the datetime object to an ISO date
        consensus_price_df['date'] = consensus_price_df.apply(datetime_to_iso,
                                                              axis=1,
                                                              args=('date',))

        # Add the tsid as a normal column
        consensus_price_df.insert(0, 'tsid', tsid)

        # Add the vendor id of the pySecMaster_Consensus as a normal column
        validator_id = retrieve_data_vendor_id(db_location=self.db_location,
                                               name='pySecMaster_Consensus')
        consensus_price_df.insert(0, 'data_vendor_id', validator_id)

        # Add the current date to the last column
        consensus_price_df.insert(len(consensus_price_df.columns),
                                  'updated_date', datetime.utcnow().isoformat())

        if validator_id in unique_sources:
            delete_start = time.time()

            # Data from the cross validation process has already been saved
            #   to the database before, thus it must be removed before adding
            #   the new calculated values.

            if self.period:
                # Only delete prior consensus values for this tsid that are
                #   newer than the beg_date (current date - replace period).
                delete_query = ("""DELETE FROM %s
                                   WHERE tsid='%s'
                                   AND data_vendor_id='%s'
                                   AND date>'%s'""" %
                                (self.table, tsid, validator_id,
                                 beg_date.isoformat()))
            else:
                # Delete all existing consensus values for this tsid.
                delete_query = ("""DELETE FROM %s
                                   WHERE tsid='%s'
                                   AND data_vendor_id='%s'""" %
                                (self.table, tsid, validator_id))

            retry_count = 5
            while retry_count > 0:
                retry_count -= 1

                delete_status = delete_sql_table_rows(
                    db_location=self.db_location, query=delete_query,
                    table=self.table, tsid=tsid)
                if delete_status == 'success':
                    # Add the validated values to the relevant price table AFTER
                    #   ensuring that the duplicates were deleted successfully
                    df_to_sql(df=consensus_price_df,
                              db_location=self.db_location,
                              sql_table=self.table, exists='append', item=tsid)
                    break

            # print('Data table replacement took %0.2f' %
            #       (time.time() - delete_start))

        else:
            # Add the validated values to the relevant price table
            df_to_sql(df=consensus_price_df, db_location=self.db_location,
                      sql_table=self.table, exists='append', item=tsid)

        if self.verbose:
            print('%s data cross-validation took %0.2f seconds to complete.' %
                  (tsid, time.time() - tsid_start))


if __name__ == '__main__':

    test_database_location = '/home/josh/Programming/Databases/pySecMaster/' \
                             'pySecMaster.db'
    test_table = 'daily_prices'

    test_tsids_df = query_all_active_tsids(db_location=test_database_location,
                                           table=test_table)
    test_tsid_list = test_tsids_df['tsid'].values

    CrossValidate(db_location=test_database_location, table=test_table,
                  tsid_list=test_tsid_list, verbose=True)
