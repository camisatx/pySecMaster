# pySecMaster
An automated system to store and maintain financial data.

The system downloads specified data from Quandl to a SQLite3 database, which can then be quickly queried. Uses of the data include trading backtests and graph visualizations. Additionally, it is possible to use the Quandl GOOG code structure to download minute stock data from Google Finance.

# System Requirements
  - Python 3.x
  - Pandas 0.16.x
  - More than 10GB of storage space

# User Requirements
  - Quandl API Token (free at quandl.com)

# Disclaimer
Before using this software, be sure to understand and follow the terms of all data providers (Quandl and Google). I am not responsible for how you use this software, so please be responsible in your use of it! Please see the following links for some information:
  - http://help.quandl.com/category/133-terms-and-conditions
  - https://www.google.com/intl/en/googlefinance/disclaimer

For further information, please seek legal counsel.

# License (GNU AGPLv3)
pySecMaster - An automated system to store and maintain financial data.

Copyright (C) 2015 Josh Schertz

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
