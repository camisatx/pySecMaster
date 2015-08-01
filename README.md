# pySecMaster
An automated system to store and maintain financial data.

The system downloads specified data from Quandl to a SQLite3 database, which can then be quickly queried. Uses of the data include trading backtests and graph visualizations. Additionally, it is possible to use the Quandl GOOG code structure to download minute stock data from Google Finance.

# Quick Start
  1. Copy the pySecMaster folder to your computer.

  2. Open pySecMaster.py in a code editor (IDLE, PyCharm, Sublime, etc.).

  3. Navigate to the General Options area (lines 51 - 68): enter your Quandl API Token after 'quandl_token = ', and change the folder location after 'database_location = ' to match your computer (you can change the database name at this time, too).

  4. If you want to download only minute stock data from Google Finance, you can now save and run pySecMaster.py. Otherwise, continue the steps 5 and 6.

  5. Navigate to the Database Maintenance Options area (lines 69 - 86): change the Quandl databases to be downloaded. Do this by changing the database abbreviations after 'database_list = '. You can browse through Quandl's website to determine what databases are appropriate for you. NOTE: if you want to download minute stock data from Google Finance, you must have 'GOOG' included in the list.

  6. Navigate to the Database Data Download Options area (lines 88 - 123): change the 'download_source = ' variable to match the data you want downloaded (Quandl and/or Google Finance (for minute data)). By default, only Google provided stock data traded on NYSE and NASDAQ will be downloaded. If you want to change this, change the 'quandl_selection' variable to match one of the predefined SQLite queries. It is possible to download very specific items by writing a custom SQLite query in the query_q_codes function in the extractor.py file.

  7. You can now save and run pySecMaster.py.

  8. To retrieve the data downloaded, open query_data.py in a code editor.

  9. Navigate to the query options (lines 172 - 181): change any of the options within this section to alter the query. Be aware that certain variables may be ignored depending on what type of query is run (i.e. minute data only comes from GOOG). It is possible to retrieve very specific data by writing a custom SQLite query. By default, the data is returned as a pandas DataFrame, which can be manipulated to any format (visual, CSV, JSON, chart, etc.), or even sent to another file for further processing.

  10. You can now save and run query_data.py.

# System Requirements
  - Python 3.x (sorry, no 2.7 support)
  - Pandas 0.16.x
  - More than 10GB of storage space (depends on the data downloaded)

# User Requirements
  - Quandl API Token (free at quandl.com)

# Additional Info
To view the SQLite3 database, you can download SQLite Database Browser for free (http://sqlitebrowser.org). This allows you to view and edit all characteristics of the database.

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
