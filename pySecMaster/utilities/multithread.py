from multiprocessing import Pool

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


def multithread(function, items, threads=4):
    """ Takes the main function to run in parallel, inputs the variable(s)
    and returns the results.

    :param function: The main function to process in parallel.
    :param items: A list of strings that are passed into the function for
    each thread.
    :param threads: The number of threads to use. The default is 4, but
    the threads are not CPU core bound.
    :return: The results of the function passed into this function.
    """

    """The async variant, which submits all processes at once and
    retrieve the results as soon as they are done."""
    pool = Pool(threads)
    output = [pool.apply_async(function, args=(item,)) for item in items]
    results = [p.get() for p in output]
    pool.close()
    pool.join()

    return results
