# Description
#### 1. Loads NSE bhavcopy file downloaded using Hemen Kapadia's getbhavcopy tool into nsedata project database
#### 2. Performs data cleanup, split/bonus adjustments
#### 3. Provides history api to read historical stock data into pandas dataframes
#### 4. Sample input data files in data folder as of 2020-08-21

# Use updatedb.ipynb notebook to run


```python
NSEDATAROOTDIR = 'D:/Studies'        # Directory path of nsedata project folder
PATH = 'D:/Trading/Historical Data/' # Root directory path for following directories
CSVPATH = 'NSE EOD/'                 # Relative directory path for NSE bhavcopy historical files
DBPATH = 'db/nsedb.db'               # Relative directory path for sqlite database to store data
MOD_PATH = 'nse_eod_modifiers/'      # Relative path to keep split and other data downloaded from NSE website
AMI_PATH = 'amibroker_files/'        # Relative path to create Amibroker files - Not tested
```


```python
import os
os.chdir(NSEDATAROOTDIR)
```


```python
import nsedata.datadbhandler as dbhandler
db = dbhandler.DataDB(path=PATH, db=DBPATH)
```

### Run following cell for delta run


```python
# For delta run:
delta_date = '20200821'
db.delta_date_check(delta_date)
db.append_tbldumps_from_csv(CSVPATH, start_date=delta_date)  # 1 Usual run
db.calculate_skipped_record_errors(start_date=delta_date)  # 2 Usual run
db.load_dump_replace_records()  # 3 Usual run
db.append_modified_tbldumps(delta_date)  # 4 Usual run
db.save_symbols_date_range_delta(delta_date)  # 5 Usual run
db.update_adjusted_price(delta_date)  # 7 # Usual run
```

### Run following cell for delta run in case of split/bonus update


```python
# For delta run:
delta_date = '20200815'
db.delta_date_check(delta_date)
db.append_tbldumps_from_csv(CSVPATH, start_date=delta_date)  # 1 Usual run
db.calculate_skipped_record_errors(start_date=delta_date)  # 2 Usual run
db.load_dump_replace_records()  # 3 Usual run
db.append_modified_tbldumps(delta_date)  # 4 Usual run
db.save_symbols_date_range_delta(delta_date)  # 5 Usual run
db.load_multipliers(type='refresh')  # 6 May not be needed always, only on corporate actions
db.update_adjusted_price(delta_date)  # 7 # Usual run
```

### Run following cell in case of index components change after month end
1. Set flag to True before run


```python
monthly_update = False
if monthly_update:
    db.load_historical_index_components()  # 8 May not be needed always, after month-end
```

### Use below sample codes to read historical stock data


```python
os.chdir(NSEDATAROOTDIR)
import nsedata.history as history

hist =  history.History(path=PATH, db=DBPATH)
```

#### Fetch individual symbol stock price history


```python
TCS = hist.symbol_history(symbol='TCS',
                          start_date='20040801',
                          end_date='20200820',
                          buffer_start='20040301', # optional
                          index='Nifty 50'         # optional
                         )
```


```python
TCS.head()
```

### Fetch historical stock price data for index components


```python
NIFTY50 = hist.index_components_history(index='Nifty 50',
                          start_date='20190801',
                          end_date='20190820',
                          buffer_start='20190301', # optional
                         )
```


```python
NIFTY50['TCS']
```

### Slice fetched data based on symbol and dates


```python
h2 = hist.copy()
h2.slice(symbols=['TCS', 'INFY'], start_date='2019-08-06', end_date='2019-08-09')
```
