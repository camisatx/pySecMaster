# pySecMaster
An automated framework to store and maintain financial data.

The system downloads specified data from Quandl to a SQLite3 database, which can then be quickly queried. Uses of the data include trading backtests and graph visualizations. Additionally, it is possible to use the Quandl GOOG code structure to download minute stock data from Google Finance.

# Quick Start (Quandl daily data)
  1. Clone the pySecMaster to your computer

  2. Open the folder called pySecMaster, and run main_gui.py

  3. Within the GUI, provide a file directory in 'Database Directory' where you want the database to be built

  4. Enter a Quandl API Key (free at <https://www.quandl.com>)

  5. Click on the 'Ok' button, and the database will start building itself

# Quick Start (Google Finance minute data)
  1. Clone the pySecMaster to your computer

  2. Open the folder called pySecMaster, and run main_gui.py

  3. Within the GUI, provide a file directory in 'Database Directory' where you want the database to be built

  4. In the 'Data' tab, change 'Download Source' combo-box from 'quandl' to 'google_fin'

  5. Click on the 'Ok' button, and the database will start building itself with minute data from Google Finance

# Quick Start (Retrieve SQLite Data)
  1. To retrieve the data in the SQLite database, open query_data.py in a code editor (IDE, PyCharm, Sublime, etc.)

  2. Navigate to the query options (lines 173 - 185): change any of the options within this section to alter the query. Be aware that certain variables may be ignored depending on what type of query is run (i.e. minute data only comes from Google Finance). It is possible to retrieve very specific data by writing a custom SQLite query. By default, the data is returned as a pandas DataFrame, which can be manipulated to any format (visual, CSV, JSON, chart, etc.), or even sent to another file for further processing.

  3. You can now save and run query_data.py

# System Requirements
  - Python 3.4+
  - Pandas 0.16.2+
  - More than 10GB of storage space (depends on the data downloaded)

# User Requirements
  - Quandl API Token (free at <https://www.quandl.com>)

# Additional Info
To view the SQLite3 database, you can download SQLite Database Browser for free (<http://sqlitebrowser.org>). This allows you to view and edit all characteristics of the database.

# Disclaimer
Before using this software, be sure to understand and follow the terms of all data providers (Quandl and Google). I am not responsible for how you use this software, so please be responsible in your use of it! Please see the following links for some information:
  - <http://help.quandl.com/category/133-terms-and-conditions>
  - <https://www.google.com/intl/en/googlefinance/disclaimer>

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
