# pySecMaster
An automated framework to store and maintain financial data.

[![AGPLv3](https://img.shields.io/badge/License-AGPLv3-blue.svg)](http://opensource.org/licenses/AGPL-3.0)

The goal of the system is to have a central repository of interrelated finance data that can be used for strategy backtests.

### TSID
All of the data tables utilize a custom symbol ID (called a 'tsid'). This allows for consistent data nomenclature across the system. The symbology table is used as a translator between the tsid and other symbol structures (Quandl codes, Yahoo Finance codes, etc.). This structure enables future symbol structures to be seamlessly added to the table to allow for external database communication (RIC, Bloomberg, etc.).

### Data Types
This system is built around the idea of having extractor modules 'plug-in' to the database. Therefore, it is designed for you to build your own data extractors for any type of data you want to store in line with the tsid structure. The default extractors handle daily and minute price data, along with basic exchange information. I have built extra tables that can have extractors built to fill in data. If you have ideas on additional tables to include, please create an issue with your idea.

#### Default Extractors
- Daily Historical Stock Prices (Quandl; complete history)
- Minute Historical Stock Prices (Google Finance; prior 15 days)
- Exchange Information
- Symbology generator (symbol translator)

#### Custom Extractors (aka, build your own)
- Corporate Activities (conference calls, earnings data, splits)
- Financial Statement Data
- IPO Pricings Data
- Economic Events Data
- Historical Indices Membership


# Quick Start (Quandl daily data)
  1. Clone the pySecMaster to your computer

  2. Open the folder called pySecMaster, and run <b>main_gui.py</b>

  3. Within the GUI, provide a file directory in 'Database Directory' where you want the database to be built

  4. Enter a Quandl API Key (free at <https://www.quandl.com>)

  5. If you have a HDD, I'd recommend changing the 'Threads' count in 'System Settings" tab to <b>'2'</b> (SSD's can handle 8 threads)

  6. Click on the 'Ok' button, and the database will start building itself
  
  7. You can save your settings either when you exit the GUI or by going to 'File' -> 'Save Settings' [ctrl + s]

# Quick Start (Google Finance minute data)
  1. Clone the pySecMaster to your computer

  2. Open the folder called pySecMaster, and run <b>main_gui.py</b>

  3. Within the GUI, provide a file directory in 'Database Directory' where you want the database to be built

  4. In the 'Data' tab, change 'Download Source' combo-box to <b>'us_main'</b>
  
  5. If you have a HDD, I'd recommend changing the 'Threads' count in 'System Settings" tab to <b>'2'</b> (SSD's can handle 8 threads)

  6. Click on the 'Ok' button, and the database will start building itself with minute data from Google Finance
  
  7. You can save your settings either when you exit the GUI or by going to 'File' -> 'Save Settings' [ctrl + s]

# Quick Start (Retrieve SQLite Data)
  1. To retrieve the data in the SQLite database, open <b>query_data.py</b> in a code editor (IDE, PyCharm, Sublime, etc.)

  2. Navigate to the query options (lines 128 - 134): change any of the options within this section to alter the query. Be aware that certain variables may be ignored depending on what type of query is run (i.e. minute data only comes from Google Finance). It is possible to retrieve very specific data by writing a custom SQLite query. By default, the data is returned as a pandas DataFrame, which can be manipulated to any format (visual, CSV, JSON, chart, etc.), or even sent to another file for further processing.

  3. You can now save and run query_data.py

# System Requirements
  - Python 3.4+
  - Pandas 0.16.2+
  - More than 10GB of storage space (daily Quandl WIKI data is about 3.4 GB, while Google Finance minute data can become 30+ GB)

# User Requirements
  - Quandl API Token (free at <https://www.quandl.com>)

# Future Goals
  - Change the database from Sqlite3 to PostgreSQL
  - Build a table tsid re-index function (if a tsid changes, all tables that have data of that tsid should be updated)

# Additional Info
To view the SQLite3 database, you can download SQLite Database Browser for free (<http://sqlitebrowser.org>). This allows you to view and edit all characteristics of the database.

# Disclaimer
Before using this software, be sure to understand and follow the terms of all data providers (Quandl and Google). I am not responsible for how you use this software, so please be responsible in your use of it! Please see the following links for some information:
  - <http://help.quandl.com/category/133-terms-and-conditions>
  - <https://www.google.com/intl/en/googlefinance/disclaimer>

For further information, please seek legal counsel.

# License (GNU AGPLv3)
pySecMaster - An automated system to store and maintain financial data.

Copyright (C) 2016 Josh Schertz

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
