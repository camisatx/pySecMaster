# pySecMaster
An automated framework to store and maintain financial data.

[![AGPLv3](https://img.shields.io/badge/License-AGPLv3-blue.svg)](http://opensource.org/licenses/AGPL-3.0)

The goal of the system is to have a central repository of interrelated finance data that can be used for strategy backtests.

## TSID
All of the data tables utilize a custom symbol ID (called a **'tsid'**; 'trading system ID'). This allows for consistent data nomenclature across the system.

#### TSID Structure
The tsid structure is composed of the following (Note 1):
```
<ticker>.<tsid exchange abbreviation>.<integer of duplicate>
```
Since Apple (AAPL) is traded on NASDAQ (tsid exchange abbreviation is 'Q'), it's tsid symbol is:
```
AAPL.Q.0
```
Walmart (WMT) is traded on NYSE (tsid exchange abbreviation is 'N'), thus it's tsid symbol is:
```
WMT.N.0
```

#### TSID Creation
The tsid creation process requires a unique ID as the backbone. At the moment, the CSI Data's *CSI Number* system is used as the backbone for ensuring that there are no tsid duplicates. It is possible to use another vendor's ID structure as the backbone (Bloomberg, RIC, etc.), or create a custom one (using a predefined base).

The biggest hindrance to using CSI Data's CSI Number system is that it restricts tsid codes to only the US, Toronto and London based exchanges (as those are the only exchanges they list). I've considering using Quandl's GOOG database to enable the tsid structure to expand to all other global exchanges, but haven't implemented this yet.

You can view (or download) the CSI Data stock factsheet [here](http://www.csidata.com/factsheets.php?type=stock&format=html).

#### TSID Exchange Abbreviations
Custom exchange abbreviations are used in the tsid structure to allow for naming flexibility and prevent duplicate abbreviations.

All abbreviations can be found by looking at the **tsid_symbol** column within [exchanges.csv](../blob/master/pySecMaster/load_tables/exchanges.csv) in pySecMaster/load_tables (or from the **exchange** table of the database).

Some common exchange abbreviation include:
|          Exchange Name         | TSID Exchange Abbreviation |
|:------------------------------:|:--------------------------:|
|     American Stock Exchange    |            AMEX            |
| New York Stock Exchange (NYSE) |              N             |
|  New York Stock Exchange ARCA  |            NARCA           |
|             NASDAQ             |              Q             |
|     OTC Markets Pink Sheets    |            PINK            |
|      London Stock Exchange     |             LON            |
|     Toronto Stock Exchange     |             TSX            |

## Symbology
The symbology table is used as a translator between the tsid symbol and other symbol structures (Quandl codes, Yahoo Finance codes, etc.) (Note 2). This structure enables future symbol structures to be seamlessly added to the table to allow for external database communication (RIC, Bloomberg, etc.).

Not only does this translation ability allows you convert one source's symbol to another, but it allows you to query any source's symbols based on characteristics stored in other tables (exchange, sector, industry, etc.).

By default, the symbology table links the tsid symbol to these data sources (Note 3):
|        Source Name       |  Source Code  |
|:------------------------:|:-------------:|
|   CSI Data's CSI Number  |    csi_data   |
| Quandl's Google database |  quandl_goog  |
|  Quandl's WIKI database  |  quandl_wiki  |
|       Seeking Alpha      | seeking_alpha |
|       Yahoo Finance      |     yahoo     |

## Data Types
This system is built around the idea of having extractor modules 'plug-in' to the database. Therefore, it is designed for you to build your own data extractors for any type of data you want to store in line with the tsid structure.

The default extractors handle daily and minute price data, along with basic exchange information. I have built extra tables that can have extractors built to fill in data. If you have ideas on additional tables to include, please create an issue with your idea.

#### Default Extractors
- Daily Historical Stock Prices (Quandl; complete history)
- Minute Historical Stock Prices (Google Finance; restricted to prior 15 days)
- Exchange Information
- Symbology generator (symbol translator)

#### Custom Extractors (aka, build your own)
- Corporate Activities (conference calls, earnings data, splits) (Yahoo)
- Financial Statement Data (may require table modification as I haven't tried this yet) (SEC Edgar)
- IPO Pricings Data (Yahoo)
- Economic Events Data (Yahoo)
- Historical Indices Membership (?)


# Quick Start Guides

### Quandl daily data
  1. Clone the pySecMaster to your computer

  2. Open the folder called pySecMaster, and run **main_gui.py**

  3. Within the GUI, provide a file directory in *Database Directory* where you want the database to be built

  4. Enter a Quandl API Key (free at <https://www.quandl.com>)
  
  5. In the *Data* tab, change *Download Source* combo-box to **quandl**
  
  6. In the *Data* tab, change *Quandl Data* combo-box to:
    - **quandl_wiki** if you want all Quandl WIKI daily data (Note 4) (~3,000 symbols)
    - **quandl_goog** if you want all *US, Toronto and London* Quandl Google Finance daily data (~38,000 symbols)
    - **quandl_goog_etf** if you want all Quandl Google Finance ETF daily data (Note 5) (~3,700 symbols)

  7. If you have a HDD, I'd recommend changing the *Threads* count in *System Settings* tab to **2** (SSD's can handle 8 threads). If you see the database constantly being locked, lower this number.

  8. Click on the *Ok* button, and the database will start building itself
  
  9. You can save your settings either when you exit the GUI or by going to *File* -> *Save Settings* [ctrl + s]

### Google Finance minute data
  1. Clone the pySecMaster to your computer

  2. Open the folder called pySecMaster, and run **main_gui.py**

  3. Within the GUI, provide a file directory in *Database Directory* where you want the database to be built

  4. In the *Data* tab, change *Download Source* combo-box to **google_fin**
  
  5. In the *Data* tab, change *Google Finance Data* combo-box to:
    - **all** if you want all *US, Toronto and London* Google Finance minute data (~38,000 symbols)
    - **us_main** if you want main US exchange Google Finance minute data (Note 6) (~9,000 symbols)
    - **us_canada_london** if you want all *US, Toronto and London* Google Finance minute data that's been active within the prior two years (~25,000 symbols)
  
  6. If you have a HDD, I'd recommend changing the *Threads* count in *System Settings* tab to **2** (SSD's can handle 8 threads). If you see the database constantly being locked, lower this number.

  7. Click on the *Ok* button, and the database will start building itself with minute data from Google Finance
  
  8. You can save your settings either when you exit the GUI or by going to *File* -> *Save Settings* [ctrl + s]

### Retrieve SQLite Data
  1. To retrieve the data in the SQLite database, open **query_data.py** in a code editor (IDE, PyCharm, Sublime, etc.)

  2. Navigate to the query options (lines 128 - 134): change any of the options within this section to alter the query. Be aware that certain variables may be ignored depending on what type of query is run (i.e. minute data only comes from Google Finance). It is possible to retrieve very specific data by writing a custom SQLite query. By default, the data is returned as a pandas DataFrame, which can be manipulated to any format (visual, CSV, JSON, chart, etc.), or even sent to another file for further processing.

  3. You can now save and run query_data.py

# System Requirements
  - Python 3.4+
  - Pandas 0.16.2+
  - More than 10GB of storage space (daily Quandl WIKI data is about 3.4 GB, while Google Finance minute data can become 30+ GB)
  - Windows (I'm sorry, I haven't tested this on 'Nix yet. The only possible issue would involve file links ('/' vs '\\'))

# User Requirements
  - Quandl API Token (free at <https://www.quandl.com>)

# Future Goals
  - Change the database from Sqlite3 to PostgreSQL
  - Build a table tsid re-index function (if a tsid changes, all tables that have data of that tsid should be updated)
  - Add a cross-source data validator (check data validity between two or more sources; preferably three or more to get a consensus)
  - Add a direct Yahoo Finance data extractor (instead of relying completely on Quandl for Yahoo data)
  - Perform cross system checks (especially Linux)

# Additional Info
To view the SQLite3 database, you can download SQLite Database Browser for free (<http://sqlitebrowser.org>). This allows you to view and edit all characteristics of the database.

# Notes
  - Note 1: I have not implemented the integer of duplicates yet, so all tsid symbols have a 0 (zero) value for that. This is only relevant when you have access to delisted stock data, and for tickers that overlap active tickers (I.E. ABG on NYSE).
  - Note 2: All source codes created for the symbology table are built from scratch using a methodology that closely follows the true symbol structure. This means that there will be occurrences where to symbology built symbol does not match the true symbol. Create an issue if you see this happening.
  - Note 3: Google Finance does not have a symbol structure; they only require a ticker and unique exchange abbreviation as separate fields. Thus the Google Finance extractor uses the tsid structure as a symbol source.
  - Note 4: The symbology table actually includes about 9,000 symbols classified as quandl_wiki, but only about 3,000 of those actually have data from Quandl. I did this because I do not have a high quality list of all the WIKI codes (I don't trust Quandl's list), thus to ensures that a good percent of WIKI codes are downloaded.
  - Note 5: All ETF symbols are derived from the CSI Data Stock Factsheet, which includes listed and delisted ETFs
  - Note 6: US main exchanges include AMEX, NYSE, BATS, NASDAQ (CM, GM, GS) and NYSE ARCA; includes stocks and ETFs

# Disclaimer
Before using this software, be sure to understand and follow the terms of all data providers (Quandl and Google). I am not responsible for how you use this software, so please be responsible in your use of it! Please see the following links for some information:
  - [Quandl TOS](http://help.quandl.com/category/133-terms-and-conditions)
  - [Google Finance TOS](https://www.google.com/intl/en/googlefinance/disclaimer)

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
