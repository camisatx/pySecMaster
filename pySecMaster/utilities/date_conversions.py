from datetime import datetime

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2016 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.4.0'

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


def dt_from_iso(row, column):
    """
    Changes the ISO 8601 date string to a datetime object.
    """

    iso = row[column]
    try:
        return datetime.strptime(iso, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(iso, '%Y-%m-%dT%H:%M:%S')
    except TypeError:
        return 'NaN'


def date_to_iso(row, column):
    """
    Change the default date format of "YYYY-MM-DD" to an ISO 8601 format.
    """

    raw_date = row[column]
    try:
        raw_date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
    except TypeError:   # Occurs if there is no date provided ("nan")
        raw_date_obj = datetime.today()
    return raw_date_obj.isoformat()
