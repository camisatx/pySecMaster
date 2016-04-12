# pySecMaster Table Structure

## Main Tables

#### baskets

| Column Name  | Type                | Foreign Key          | Index |
|--------------|---------------------|----------------------|-------|
| basket_id    | INTEGER PRIMARY KEY |                      |       |
| basket_name  | TEXT                |                      |       |
| tsid         | TEXT                | symbology(source_id) |       |
| date         | FLOAT               |                      |       |
| created_date | FLOAT               |                      |       |
| updated_date | FLOAT               |                      |       |

#### csidata_stock_factsheet

| Column Name      | Type                | Foreign Key          | Index              |
|------------------|---------------------|----------------------|--------------------|
| CsiNumber        | INTEGER PRIMARY KEY | symbology(source_id) |                    |
| Symbol           | TEXT                |                      | idx_csidata_symbol |
| Name             | TEXT                |                      |                    |
| Exchange         | TEXT                |                      |                    |
| IsActive         | INTEGER             |                      |                    |
| StartDate        | TEXT                |                      |                    |
| EndDate          | TEXT                |                      |                    |
| Sector           | TEXT                |                      |                    |
| Industry         | TEXT                |                      |                    |
| ConversionFactor | INTEGER             |                      |                    |
| SwitchCfDate     | TEXT                |                      |                    |
| PreSwitchCf      | INTEGER             |                      |                    |
| LastVolume       | INTEGER             |                      |                    |
| Type             | TEXT                |                      |                    |
| ChildExchange    | TEXT                |                      |                    |
| Currency         | TEXT                |                      |                    |
| created_date     | FLOAT               |                      |                    |
| updated_date     | FLOAT               |                      |                    |

#### data_vendor

| Column Name      | Type                              | Foreign Key | Index |
|------------------|-----------------------------------|-------------|-------|
| data_vendor_id   | INTEGER PRIMARY KEY AUTOINCREMENT |             |       |
| name             | TEXT UNIQUE                       |             |       |
| url              | TEXT                              |             |       |
| support_email    | TEXT                              |             |       |
| api              | TEXT                              |             |       |
| consensus_weight | FLOAT                             |             |       |
| created_date     | FLOAT                             |             |       |
| updated_date     | FLOAT                             |             |       |

#### exchange

| Column Name  | Type                              | Foreign Key | Index |
|--------------|-----------------------------------|-------------|-------|
| exchange_id  | INTEGER PRIMARY KEY AUTOINCREMENT |             |       |
| symbol       | TEXT UNIQUE                       |             |       |
| goog_symbol  | TEXT                              |             |       |
| yahoo_symbol | TEXT                              |             |       |
| csi_symbol   | TEXT                              |             |       |
| tsid_symbol  | TEXT                              |             |       |
| name         | TEXT                              |             |       |
| country      | TEXT                              |             |       |
| city         | TEXT                              |             |       |
| currency     | TEXT                              |             |       |
| time_zone    | TEXT                              |             |       |
| utc_offset   | INT                               |             |       |
| open         | TEXT                              |             |       |
| close        | TEXT                              |             |       |
| lunch        | TEXT                              |             |       |
| created_date | FLOAT                             |             |       |
| updated_date | FLOAT                             |             |       |

#### indices

| Column Name  | Type                | Foreign Key          | Index |
|--------------|---------------------|----------------------|-------|
| index_id     | INTEGER PRIMARY KEY |                      |       |
| stock_index  | TEXT                |                      |       |
| tsid         | TEXT                | symbology(source_id) |       |
| as_of_date   | FLOAT               |                      |       |
| created_date | FLOAT               |                      |       |
| updated_date | FLOAT               |                      |       |

#### quandl_codes

| Column Name  | Type                              | Foreign Key          | Index       |
|--------------|-----------------------------------|----------------------|-------------|
| q_code_id    | INTEGER PRIMARY KEY AUTOINCREMENT | symbology(source_id) |             |
| data_vendor  | TEXT                              | data_vendor(name)    |             |
| data         | TEXT                              |                      | idx_qc_data |
| component    | TEXT                              |                      |             |
| period       | TEXT                              |                      |             |
| q_code       | TEXT                              |                      |             |
| name         | TEXT                              |                      |             |
| start_date   | FLOAT                             |                      |             |
| end_date     | FLOAT                             |                      |             |
| frequency    | TEXT                              |                      |             |
| last_updated | FLOAT                             |                      |             |
| page_num     | INT                               |                      |             |
| created_date | FLOAT                             |                      |             |
| updated_date | FLOAT                             |                      |             |

#### symbology

| Column Name  | Type  | Foreign Key | Index                   |
|--------------|-------|-------------|-------------------------|
| symbol_id    | INT   |             | idx_symbology_symbol_id |
| source       | TEXT  |             |                         |
| source_id    | TEXT  |             | idx_symbology_source_id |
| type         | TEXT  |             |                         |
| created_date | FLOAT |             |                         |
| updated_date | FLOAT |             |                         |


## Data Tables

#### daily_prices

| Column Name    | Type                              | Foreign Key                 | Index                 |
|----------------|-----------------------------------|-----------------------------|-----------------------|
| daily_price_id | INTEGER PRIMARY KEY AUTOINCREMENT |                             |                       |
| data_vendor_id | INT                               | data_vendor(data_vendor_id) | idx_dp_data_vendor_id |
| tsid           | TEXT                              | symbology(source_id)        | idx_dp_tsid           |
| date           | FLOAT                             |                             | idx_dp_date           |
| open           | REAL                              |                             |                       |
| high           | REAL                              |                             |                       |
| low            | REAL                              |                             |                       |
| close          | REAL                              |                             |                       |
| volume         | REAL                              |                             |                       |
| ex_dividend    | REAL                              |                             |                       |
| split_ratio    | REAL                              |                             |                       |
| adj_open       | REAL                              |                             |                       |
| adj_high       | REAL                              |                             |                       |
| adj_low        | REAL                              |                             |                       |
| adj_close      | REAL                              |                             |                       |
| adj_volume     | REAL                              |                             |                       |
| updated_date   | FLOAT                             |                             | idx_dp_updated_date   |

#### finra_data

| Column Name         | Type                              | Foreign Key          | Index          |
|---------------------|-----------------------------------|----------------------|----------------|
| finra_id            | INTEGER PRIMARY KEY AUTOINCREMENT |                      |                |
| tsid                | TEXT                              | symbology(source_id) | idx_finra_tsid |
| date                | TEXT                              |                      |                |
| short_volume        | REAL                              |                      |                |
| short_exempt_volume | REAL                              |                      |                |
| total_volume        | REAL                              |                      |                |
| updated_date        | FLOAT                             |                      |                |

#### fundamental_data

| Column Name    | Type                              | Foreign Key          | Index         |
|----------------|-----------------------------------|----------------------|---------------|
| fundamental_id | INTEGER PRIMARY KEY AUTOINCREMENT |                      |               |
| tsid           | TEXT                              | symbology(source_id) | idx_fund_tsid |
| date           | TEXT                              |                      |               |
| value          | FLOAT                             |                      |               |
| note           | TEXT                              |                      |               |
| created_date   | FLOAT                             |                      |               |
| updated_date   | FLOAT                             |                      |               |

#### minute_prices

| Column Name     | Type                              | Foreign Key                 | Index                 |
|-----------------|-----------------------------------|-----------------------------|-----------------------|
| minute_price_id | INTEGER PRIMARY KEY AUTOINCREMENT |                             |                       |
| data_vendor_id  | INT                               | data_vendor(data_vendor_id) | idx_mp_data_vendor_id |
| tsid            | TEXT                              | symbology(source_id)        | idx_mp_tsid           |
| date            | TEXT                              |                             | idx_mp_date           |
| close           | REAL                              |                             |                       |
| high            | REAL                              |                             |                       |
| low             | REAL                              |                             |                       |
| open            | REAL                              |                             |                       |
| volume          | REAL                              |                             |                       |
| update_date     | FLOAT                             |                             | idx_mp_updated_date   |


## Events Tables

#### conference_calls

| Column Name  | Type                              | Foreign Key          | Index         |
|--------------|-----------------------------------|----------------------|---------------|
| conf_call_id | INTEGER PRIMARY KEY AUTOINCREMENT |                      |               |
| tsid         | TEXT                              | symbology(source_id) | idx_conf_tsid |
| symbol       | TEXT                              |                      |               |
| date         | FLOAT                             |                      |               |
| event_title  | TEXT                              |                      |               |
| created_date | FLOAT                             |                      |               |
| updated_date | FLOAT                             |                      |               |

#### dividends

| Column Name       | Type                              | Foreign Key          | Index        |
|-------------------|-----------------------------------|----------------------|--------------|
| dividend_id       | INTEGER PRIMARY KEY AUTOINCREMENT |                      |              |
| tsid              | TEXT                              | symbology(source_id) | idx_div_tsid |
| symbol            | TEXT                              |                      |              |
| company           | TEXT                              |                      |              |
| dividend          | FLOAT                             |                      |              |
| ex_dividend_date  | FLOAT                             |                      |              |
| record_date       | FLOAT                             |                      |              |
| announcement_date | FLOAT                             |                      |              |
| payment_date      | FLOAT                             |                      |              |
| created_date      | FLOAT                             |                      |              |
| updated_date      | FLOAT                             |                      |              |

#### earnings

| Column Name   | Type                              | Foreign Key          | Index         |
|---------------|-----------------------------------|----------------------|---------------|
| earnings_id   | INTEGER PRIMARY KEY AUTOINCREMENT |                      |               |
| tsid          | TEXT                              | symbology(source_id) | idx_earn_tsid |
| symbol        | TEXT                              |                      |               |
| company_name  | TEXT                              |                      |               |
| date          | FLOAT                             |                      |               |
| reported_eps  | FLOAT                             |                      |               |
| consensus_eps | FLOAT                             |                      |               |
| created_date  | FLOAT                             |                      |               |
| updated_date  | FLOAT                             |                      |               |

#### economic_events

| Column Name       | Type                              | Foreign Key          | Index               |
|-------------------|-----------------------------------|----------------------|---------------------|
| event_id          | INTEGER PRIMARY KEY AUTOINCREMENT |                      |                     |
| tsid              | TEXT                              | symbology(source_id) | idx_econ_event_tsid |
| event_name        | TEXT                              |                      |                     |
| date              | FLOAT                             |                      |                     |
| date_for          | FLOAT                             |                      |                     |
| actual            | TEXT                              |                      |                     |
| briefing_forecast | TEXT                              |                      |                     |
| market_expects    | TEXT                              |                      |                     |
| prior             | TEXT                              |                      |                     |
| revised_from      | TEXT                              |                      |                     |
| created_date      | FLOAT                             |                      |                     |
| updated_date      | FLOAT                             |                      |                     |

#### ipo_pricings

| ipo_pricings   |                                   |                      |               |
|----------------|-----------------------------------|----------------------|---------------|
| Column Name    | Type                              | Foreign Key          | Index         |
| ipo_id         | INTEGER PRIMARY KEY AUTOINCREMENT |                      |               |
| tsid           | TEXT                              | symbology(source_id) | idx_ipop_tsid |
| symbol         | TEXT                              |                      |               |
| company_name   | TEXT                              |                      |               |
| offer_date     | FLOAT                             |                      |               |
| shares_offered | TEXT                              |                      |               |
| proposed_price | TEXT                              |                      |               |
| initial_price  | TEXT                              |                      |               |
| created_date   | FLOAT                             |                      |               |
| updated_date   | FLOAT                             |                      |               |

#### splits

| Column Name    | Type                              | Foreign Key          | Index           |
|----------------|-----------------------------------|----------------------|-----------------|
| split_id       | INTEGER PRIMARY KEY AUTOINCREMENT |                      |                 |
| tsid           | TEXT                              | symbology(source_id) | idx_splits_tsid |
| symbol         | TEXT                              |                      |                 |
| company_name   | TEXT                              |                      |                 |
| payable_date   | FLOAT                             |                      |                 |
| ex_date        | FLOAT                             |                      |                 |
| announced_date | FLOAT                             |                      |                 |
| optionable     | INT                               |                      |                 |
| ratio          | FLOAT                             |                      |                 |
| created_date   | FLOAT                             |                      |                 |
| updated_date   | FLOAT                             |                      |                 |
