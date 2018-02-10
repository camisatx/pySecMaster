# pySecMaster Table Structure

These are structures for all of the tables built by pySecMaster. The three types of tables include [Main Tables](#main-tables), [Data Tables](#data-tables) and [Events Tables](#events-tables).
 
 24 tables are created within the specified PostgreSQL database when pySecMaster is run.

## Main Tables

#### baskets

| Column Name  | Type                     | Foreign Key | Index |
|--------------|--------------------------|-------------|-------|
| basket_id    | SERIAL PRIMARY KEY       |             |       |
| name         | TEXT NOT NULL            |             |       |
| description  | TEXT                     |             |       |
| start_date   | TIMESTAMP WITH TIME ZONE |             |       |
| end_date     | TIMESTAMP WITH TIME ZONE |             |       |
| created_by   | TEXT                     |             |       |
| created_date | TIMESTAMP WITH TIME ZONE |             |       |
| updated_date | TIMESTAMP WITH TIME ZONE |             |       |

#### basket_values

| Column Name   | Type                     | Foreign Key                  | Index |
|---------------|--------------------------|------------------------------|-------|
| basket_val_id | BIGSERIAL PRIMARY KEY    |                              |       |
| basket_id     | INTEGER NOT NULL         | baskets(basket_id)           |       |
| source        | TEXT NOT NULL            | symbology(source, source_id) |       |
| source_id     | TEXT NOT NULL            | symbology(source, source_id) |       |
| updated_date  | TIMESTAMP WITH TIME ZONE |                              |       |

#### classification

| Column Name       | Type                     | Foreign Key                  | Index                     |
|-------------------|--------------------------|------------------------------|---------------------------|
| classificatino_id | BIGSERIAL PRIMARY KEY    |                              |                           |
| source            | TEXT NOT NULL            | symbology(source, source_id) | idx_classification_values |
| source_id         | TEXT NOT NULL            | symbology(source, source_id) | idx_classification_values |
| standard          | TEXT                     |                              | idx_classification_values |
| code              | INTEGER                  |                              |                           |
| level_1           | TEXT                     |                              | idx_classification_values |
| level_2           | TEXT                     |                              | idx_classification_values |
| level_3           | TEXT                     |                              | idx_classification_values |
| level_4           | TEXT                     |                              | idx_classification_values |
| created_date      | TIMESTAMP WITH TIME ZONE |                              |                           |
| updated_date      | TIMESTAMP WITH TIME ZONE |                              |                           |

#### csidata_stock_factsheet

| Column Name       | Type                     | Foreign Key | Index              |
|-------------------|--------------------------|-------------|--------------------|
| csi_number        | TEXT PRIMARY KEY         |             |                    |
| symbol            | TEXT                     |             | idx_csidata_symbol |
| name              | TEXT                     |             |                    |
| exchange          | TEXT                     |             |                    |
| sub_exchange      | TEXT                     |             |                    |
| is_active         | SMALLINT                 |             |                    |
| start_date        | DATE                     |             |                    |
| end_date          | DATE                     |             |                    |
| conversion_factor | SMALLINT                 |             |                    |
| switch_cf_date    | DATE                     |             |                    |
| pre_switch_cf     | SMALLINT                 |             |                    |
| created_date      | TIMESTAMP WITH TIME ZONE |             |                    |
| updated_date      | TIMESTAMP WITH TIME ZONE |             |                    |

#### data_vendor

| Column Name      | Type                     | Foreign Key | Index |
|------------------|--------------------------|-------------|-------|
| data_vendor_id   | INTEGER PRIMARY KEY      |             |       |
| name             | TEXT UNIQUE              |             |       |
| url              | TEXT                     |             |       |
| support_email    | TEXT                     |             |       |
| api              | TEXT                     |             |       |
| consensus_weight | SMALLINT                 |             |       |
| created_date     | TIMESTAMP WITH TIME ZONE |             |       |
| updated_date     | TIMESTAMP WITH TIME ZONE |             |       |

#### exchange

| Column Name  | Type                     | Foreign Key | Index |
|--------------|--------------------------|-------------|-------|
| exchange_id  | SMALLINT PRIMARY KEY     |             |       |
| symbol       | TEXT UNIQUE NOT NULL     |             |       |
| goog_symbol  | TEXT                     |             |       |
| yahoo_symbol | TEXT                     |             |       |
| csi_symbol   | TEXT                     |             |       |
| tsid_symbol  | TEXT NOT NULL            |             |       |
| name         | TEXT                     |             |       |
| country      | TEXT                     |             |       |
| city         | TEXT                     |             |       |
| currency     | TEXT                     |             |       |
| time_zone    | TEXT                     |             |       |
| utc_offset   | REAL                     |             |       |
| open         | TEXT                     |             |       |
| close        | TEXT                     |             |       |
| lunch        | TEXT                     |             |       |
| created_date | TIMESTAMP WITH TIME ZONE |             |       |
| updated_date | TIMESTAMP WITH TIME ZONE |             |       |

#### indices

| Column Name  | Type                     | Foreign Key                  | Index |
|--------------|--------------------------|------------------------------|-------|
| index_id     | SERIAL PRIMARY KEY       |                              |       |
| stock_index  | TEXT NOT NULL            |                              |       |
| source       | TEXT NOT NULL            | symbology(source, source_id) |       |
| source_id    | TEXT NOT NULL            | symbology(source, source_id) |       |
| as_of_date   | TIMESTAMP WITH TIME ZONE |                              |       |
| created_date | TIMESTAMP WITH TIME ZONE |                              |       |
| updated_date | TIMESTAMP WITH TIME ZONE |                              |       |

#### quandl_codes

| Column Name      | Type                     | Foreign Key       | Index       |
|------------------|--------------------------|-------------------|-------------|
| q_code_id        | BIGSERIAL PRIMARY KEY    |                   |             |
| data_vendor      | TEXT NOT NULL            | data_vendor(name) |             |
| data             | TEXT NOT NULL            |                   | idx_qc_data |
| component        | TEXT NOT NULL            |                   |             |
| period           | TEXT                     |                   |             |
| symbology_source | TEXT NOT NULL            |                   |             |
| q_code           | TEXT NOT NULL            |                   |             |
| name             | TEXT                     |                   |             |
| start_date       | TIMESTAMP WITH TIME ZONE |                   |             |
| end_date         | TIMESTAMP WITH TIME ZONE |                   |             |
| frequency        | TEXT                     |                   |             |
| last_updated     | TIMESTAMP WITH TIME ZONE |                   |             |
| page_num         | INTEGER                  |                   |             |
| created_date     | TIMESTAMP WITH TIME ZONE |                   |             |
| updated_date     | TIMESTAMP WITH TIME ZONE |                   |             |

#### symbology

| Column Name  | Type                     | Foreign Key | Index                 |
|--------------|--------------------------|-------------|-----------------------|
| symbol_id    | BIGINT NOT NULL          |             |                       |
| source       | TEXT NOT NULL            |             | idx_symbology_sources |
| source_id    | TEXT NOT NULL            |             | idx_symbology_sources |
| type         | TEXT                     |             |                       |
| created_date | TIMESTAMP WITH TIME ZONE |             |                       |
| updated_date | TIMESTAMP WITH TIME ZONE |             |                       |

#### tickers

| Column Name      | Type                     | Foreign Key                  | Index              |
|------------------|--------------------------|------------------------------|--------------------|
| tsid             | TEXT PRIMARY KEY         | symbology(source, source_id) |                    |
| ticker           | TEXT NOT NULL            |                              |                    |
| name             | TEXT                     |                              |                    |
| exchange         | TEXT NOT NULL            | exchanges(abbrev)            |                    |
| child_exchange   | TEXT                     |                              |                    |
| is_active        | SMALLINT                 |                              |                    |
| start_date       | TIMESTAMP WITH TIME ZONE |                              |                    |
| end_date         | TIMESTAMP WITH TIME ZONE |                              |                    |
| type             | TEXT                     |                              |                    |
| sector           | TEXT                     |                              | idx_tickers_sector |
| industry         | TEXT                     |                              |                    |
| sub_industry     | TEXT                     |                              |                    |
| currency         | TEXT                     |                              |                    |
| hq_country       | TEXT                     |                              |                    |
| symbology_source | TEXT NOT NULL            | symbology(source, source_id) |                    |
| created_date     | TIMESTAMP WITH TIME ZONE |                              |                    |
| updated_date     | TIMESTAMP WITH TIME ZONE |                              |                    |


## Data Tables

#### daily_prices

| Column Name    | Type                              | Foreign Key                  | Index              |
|----------------|-----------------------------------|------------------------------|--------------------|
| daily_price_id | BIGSERIAL PRIMARY KEY             |                              |                    |
| data_vendor_id | SMALLINT                          | data_vendor(data_vendor_id)  | idx_dp_identifiers |
| source         | TEXT NOT NULL                     | symbology(source, source_id) |                    |
| source_id      | TEXT NOT NULL                     | symbology(source, source_id) | idx_dp_identifiers |
| date           | TIMESTAMP WITH TIME ZONE NOT NULL |                              | idx_dp_identifiers |
| open           | DECIMAL(11,4)                     |                              |                    |
| high           | DECIMAL(11,4)                     |                              |                    |
| low            | DECIMAL(11,4)                     |                              |                    |
| close          | DECIMAL(11,4)                     |                              |                    |
| volume         | BIGINT                            |                              |                    |
| ex_dividend    | DECIMAL(6,3)                      |                              |                    |
| split_ratio    | DECIMAL(11,4)                     |                              |                    |
| updated_date   | TIMESTAMP WITH TIME ZONE          |                              | idx_dp_identifiers |

#### finra_data

| Column Name         | Type                              | Foreign Key                  | Index               |
|---------------------|-----------------------------------|------------------------------|---------------------|
| finra_id            | SERIAL PRIMARY KEY                |                              |                     |
| source              | TEXT NOT NULL                     | symbology(source, source_id) |                     |
| source_id           | TEXT NOT NULL                     | symbology(source, source_id) | idx_finra_source_id |
| date                | TIMESTAMP WITH TIME ZONE NOT NULL |                              |                     |
| short_volume        | INTEGER                           |                              |                     |
| short_exempt_volume | INTEGER                           |                              |                     |
| total_volume        | INTEGER                           |                              |                     |
| updated_date        | TIMESTAMP WITH TIME ZONE          |                              |                     |

#### fundamental_data

| Column Name    | Type                              | Foreign Key                  | Index              |
|----------------|-----------------------------------|------------------------------|--------------------|
| fundamental_id | BIGSERIAL PRIMARY KEY             |                              |                    |
| data_vendor_id | SMALLINT                          | data_vendor(data_vendor_id)  | idx_fund_source_id |
| source         | TEXT NOT NULL                     | symbology(source, source_id) | idx_fund_source_id |
| source_id      | TEXT NOT NULL                     | symbology(source, source_id) | idx_fund_source_id |
| date           | TIMESTAMP WITH TIME ZONE NOT NULL |                              | idx_fund_source_id |
| field          | TEXT                              |                              |                    |
| value          | DECIMAL(14,2)                     |                              |                    |
| note           | TEXT                              |                              |                    |
| created_date   | TIMESTAMP WITH TIME ZONE          |                              |                    |
| updated_date   | TIMESTAMP WITH TIME ZONE          |                              |                    |

#### minute_prices

| Column Name     | Type                     | Foreign Key                  | Index              |
|-----------------|--------------------------|------------------------------|--------------------|
| minute_price_id | BIGSERIAL PRIMARY KEY    |                              |                    |
| data_vendor_id  | SMALLINT                 | data_vendor(data_vendor_id)  | idx_mp_identifiers |
| source          | TEXT NOT NULL            | symbology(source, source_id) |                    |
| source_id       | TEXT NOT NULL            | symbology(source, source_id) | idx_mp_identifiers |
| date            | TIMESTAMP WITH TIME ZONE |                              | idx_mp_identifiers |
| close           | DECIMAL(11,4)            |                              |                    |
| high            | DECIMAL(11,4)            |                              |                    |
| low             | DECIMAL(11,4)            |                              |                    |
| open            | DECIMAL(11,4)            |                              |                    |
| volume          | BITINT                   |                              |                    |
| update_date     | TIMESTAMP WITH TIME ZONE |                              | idx_mp_identifiers |

#### option_chains

| Column Name    | Type                     | Foreign Key                  | Index                    |
|----------------|--------------------------|------------------------------|--------------------------|
| option_id      | BIGSERIAL PRIMARY KEY    |                              |                          |
| data_vendor_id | SMALLINT                 | data_vendor(data_vendor_id)  | idx_option_chains_values |
| source         | TEXT NOT NULL            | symbology(source, source_id) | idx_option_chains_values |
| source_id      | TEXT NOT NULL            | symbology(source, source_id) | idx_option_chains_values |
| symbol         | TEXT                     |                              |                          |
| exchange       | TEXT                     |                              |                          |
| currency       | TEXT                     |                              |                          |
| multiplier     | SMALLINT                 |                              |                          |
| contract_id    | BIGINT NOT NULL          |                              | idx_option_chains_values |
| expiry         | DATE                     |                              | idx_option_chains_values |
| type           | TEXT                     |                              |                          |
| strike         | DECIMAL(8,2)             |                              | idx_option_chains_values |
| pre_split      | BOOLEAN                  |                              | idx_option_chains_values |
| created_date   | TIMESTAMP WITH TIME ZONE |                              |                          |
| updated_date   | TIMESTAMP WITH TIME ZONE |                              |                          |

#### option_prices

| Column Name      | Type                     | Foreign Key                 | Index             |
|------------------|--------------------------|-----------------------------|-------------------|
| option_prices_id | BIGSERIAL PRIMARY KEY    |                             |                   |
| data_vendor_id   | SMALLINT                 | data_vendor(data_vendor_id) | idx_option_prices |
| option_id        | BIGINT                   | option_chains(option_id)    | idx_option_prices |
| date             | TIMESTAMP WITH TIME ZONE |                             | idx_option_prices |
| bid              | DECIMAL(10,4)            |                             |                   |
| bid_size         | INTEGER                  |                             |                   |
| ask              | DECIMAL(10,4)            |                             |                   |
| ask_size         | INTEGER                  |                             |                   |
| close            | DECIMAL(10,4)            |                             |                   |
| open_interest    | INTEGER                  |                             |                   |
| volume           | INTEGER                  |                             |                   |
| imp_volatility   | DECIMAL(6,4)             |                             |                   |
| delta            | DECIMAL(6,5)             |                             |                   |
| gamma            | DECIMAL(6,5)             |                             |                   |
| rho              | DECIMAL(6,5)             |                             |                   |
| theta            | DECIMAL(6,5)             |                             |                   |
| vega             | DECIMAL(6,5)             |                             |                   |
| updated_date     | TIMESTAMP WITH TIME ZONE |                             |                   |

#### tick_prices

olumn Name    | Type                     | Foreign Key                  | Index           |
|----------------|--------------------------|------------------------------|-----------------|
| tick_id        | BIGSERIAL PRIMARY KEY    |                              |                 |
| data_vendor_id | SMALLINT                 | data_vendor(data_vendor_id)  |                 |
| source         | TEXT NOT NULL            | symbology(source, source_id) | idx_tick_values |
| source_id      | TEXT NOT NULL            | symbology(source, source_id) | idx_tick_values |
| date           | TIMESTAMP WITH TIME ZONE |                              | idx_tick_values |
| bid            | DECIMAL(11,4)            |                              |                 |
| ask            | DECIMAL(11,4)            |                              |                 |
| last           | DECIMAL(11,4)            |                              |                 |
| high           | DECIMAL(11,4)            |                              |                 |
| low            | DECIMAL(11,4)            |                              |                 |
| close          | DECIMAL(11,4)            |                              |                 |
| bid_size       | INTEGER                  |                              |                 |
| ask_size       | INTEGER                  |                              |                 |
| last_size      | INTEGER                  |                              |                 |
| volume         | INTEGER                  |                              |                 |

#### tick_prices_stream

| Column Name    | Type                     | Foreign Key                  | Index                  |
|----------------|--------------------------|------------------------------|------------------------|
| tick_id        | BIGSERIAL PRIMARY KEY    |                              |                        |
| data_vendor_id | SMALLINT                 | data_vendor(data_vendor_id)  |                        |
| source         | TEXT NOT NULL            | symbology(source, source_id) |                        |
| source_id      | TEXT NOT NULL            | symbology(source, source_id) | idx_tick_stream_values |
| date           | TIMESTAMP WITH TIME ZONE |                              | idx_tick_stream_values |
| field          | TEXT                     |                              | idx_tick_stream_values |
| value          | DECIMAL(11,4)            |                              |                        |


## Events Tables

#### conference_calls

| Column Name  | Type                              | Foreign Key                  | Index              |
|--------------|-----------------------------------|------------------------------|--------------------|
| conf_call_id | SERIAL PRIMARY KEY                |                              |                    |
| source       | TEXT NOT NULL                     | symbology(source, source_id) | idx_conf_source_id |
| source_id    | TEXT NOT NULL                     | symbology(source, source_id) | idx_conf_source_id |
| symbol       | TEXT                              |                              |                    |
| date         | TIMESTAMP WITH TIME ZONE NOT NULL |                              | idx_conf_source_id |
| event_title  | TEXT                              |                              |                    |
| created_date | TIMESTAMP WITH TIME ZONE          |                              |                    |
| updated_date | TIMESTAMP WITH TIME ZONE          |                              |                    |

#### dividends

| Column Name       | Type                              | Foreign Key                  | Index             |
|-------------------|-----------------------------------|------------------------------|-------------------|
| dividend_id       | SERIAL PRIMARY KEY                |                              |                   |
| source            | TEXT NOT NULL                     | symbology(source, source_id) | idx_div_source_id |
| source_id         | TEXT NOT NULL                     | symbology(source, source_id) | idx_div_source_id |
| symbol            | TEXT                              |                              |                   |
| company           | TEXT                              |                              |                   |
| dividend          | DECIMAL(6,3)                      |                              |                   |
| ex_dividend_date  | TIMESTAMP WITH TIME ZONE NOT NULL |                              | idx_div_source_id |
| record_date       | TIMESTAMP WITH TIME ZONE          |                              |                   |
| announcement_date | TIMESTAMP WITH TIME ZONE          |                              |                   |
| payment_date      | TIMESTAMP WITH TIME ZONE          |                              |                   |
| created_date      | TIMESTAMP WITH TIME ZONE          |                              |                   |
| updated_date      | TIMESTAMP WITH TIME ZONE          |                              |                   |

#### earnings

| Column Name   | Type                              | Foreign Key                  | Index              |
|---------------|-----------------------------------|------------------------------|--------------------|
| earnings_id   | SERIAL PRIMARY KEY                |                              |                    |
| source        | TEXT NOT NULL                     | symbology(source, source_id) | idx_earn_source_id |
| source_id     | TEXT NOT NULL                     | symbology(source, source_id) | idx_earn_source_id |
| symbol        | TEXT                              |                              |                    |
| company_name  | TEXT                              |                              |                    |
| date          | TIMESTAMP WITH TIME ZONE NOT NULL |                              | idx_earn_source_id |
| reported_eps  | DECIMAL(6,3)                      |                              |                    |
| consensus_eps | DECIMAL(6,3)                      |                              |                    |
| created_date  | TIMESTAMP WITH TIME ZONE          |                              |                    |
| updated_date  | TIMESTAMP WITH TIME ZONE          |                              |                    |

#### economic_events

| Column Name       | Type                     | Foreign Key                  | Index                    |
|-------------------|--------------------------|------------------------------|--------------------------|
| event_id          | SERIAL PRIMARY KEY       |                              |                          |
| source            | TEXT NOT NULL            | symbology(source, source_id) | idx_econ_event_source_id |
| source_id         | TEXT NOT NULL            | symbology(source, source_id) | idx_econ_event_source_id |
| event_name        | TEXT                     |                              | idx_econ_event_source_id |
| date              | TIMESTAMP WITH TIME ZONE |                              | idx_econ_event_source_id |
| date_for          | TIMESTAMP WITH TIME ZONE |                              |                          |
| actual            | TEXT                     |                              |                          |
| briefing_forecast | TEXT                     |                              |                          |
| market_expects    | TEXT                     |                              |                          |
| prior             | TEXT                     |                              |                          |
| revised_from      | TEXT                     |                              |                          |
| created_date      | TIMESTAMP WITH TIME ZONE |                              |                          |
| updated_date      | TIMESTAMP WITH TIME ZONE |                              |                          |

#### ipo_pricings

| Column Name    | Type                     | Foreign Key                  | Index              |
|----------------|--------------------------|------------------------------|--------------------|
| ipo_id         | SERIAL PRIMARY KEY       |                              |                    |
| source         | TEXT NOT NULL            | symbology(source, source_id) | idx_ipop_source_id |
| source_id      | TEXT NOT NULL            | symbology(source, source_id) | idx_ipop_source_id |
| symbol         | TEXT                     |                              |                    |
| company_name   | TEXT                     |                              |                    |
| offer_date     | TIMESTAMP WITH TIME ZONE |                              | idx_ipop_source_id |
| shares_offered | TEXT                     |                              |                    |
| proposed_price | TEXT                     |                              |                    |
| initial_price  | TEXT                     |                              |                    |
| created_date   | TIMESTAMP WITH TIME ZONE |                              |                    |
| updated_date   | TIMESTAMP WITH TIME ZONE |                              |                    |

#### splits

| Column Name    | Type                     | Foreign Key                  | Index                |
|----------------|--------------------------|------------------------------|----------------------|
| split_id       | SERIAL PRIMARY KEY       |                              |                      |
| source         | TEXT NOT NULL            | symbology(source, source_id) | idx_splits_source_id |
| source_id      | TEXT NOT NULL            | symbology(source, source_id) | idx_splits_source_id |
| symbol         | TEXT                     |                              |                      |
| company_name   | TEXT                     |                              |                      |
| payable_date   | TIMESTAMP WITH TIME ZONE |                              |                      |
| ex_date        | TIMESTAMP WITH TIME ZONE |                              | idx_splits_source_id |
| announced_date | TIMESTAMP WITH TIME ZONE |                              |                      |
| optionable     | BOOLEAN                  |                              |                      |
| ratio          | DECIMAL(11,4)            |                              | idx_splits_source_id |
| created_date   | TIMESTAMP WITH TIME ZONE |                              |                      |
| updated_date   | TIMESTAMP WITH TIME ZONE |                              |                      |
