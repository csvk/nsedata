"""
Created on Jun 10, 2018
@author: Souvik
@Program Function: NSE Data Handler
"""

import os
import datadbhandler as dbhandler

PATH = 'D:/Trading/Historical Data/'
CSVPATH = 'NSE EOD/'
DBPATH = 'db/nsedb.db'
MOD_PATH = 'nse_eod_modifiers/'
AMI_PATH = 'amibroker_files/'

os.chdir(PATH)

db = dbhandler.DataDB(DBPATH)

# For full run
#db.load_tbldumps_from_csv(CSVPATH, start_year='2018')  # 1
#db.calculate_skipped_record_errors()  # 2
#db.load_dump_replace_records()  # 3
#db.load_modified_tbldumps()  # 4
#db.save_symbols_date_range(type='refresh')  # 5
#db.load_multipliers(type='refresh')  # 6
#db.update_adjusted_price()  # 7
#db.table_report()
#db.index_change_xl_to_csv()
#db.check_symbol_dates()
#db.load_historical_index_components()  # 8
#db.test_inc_exc_mod_list() # 
#db.symbols_index_hist_files(type='full')  # 9 for amibroker
#db.symbols_index_hist_files(start_year='2007', end_year='2018', type='full') #  for amibroker
#db.symbols_index_hist_files(indices=('Nifty 50', 'Nifty 100'), type='full') #  for amibroker
#db.test_inc_exc_index_data()  # 10
#db.update_symbol_change() # Use if symbol changed, and db update is delayed
#db.create_amibroker_import_files(path=AMI_PATH, type='full')  # 11
#db.create_amibroker_import_files_index_incexc(index='Nifty 50', path=AMI_PATH, type='full')  # 12

# For delta run:
delta_date = '20200808'
db.delta_date_check(delta_date)
db.append_tbldumps_from_csv(CSVPATH, start_date=delta_date)  # 1 Usual run
db.calculate_skipped_record_errors(start_date=delta_date)  # 2 Usual run
db.load_dump_replace_records()  # 3 Usual run
db.append_modified_tbldumps(delta_date)  # 4 Usual run
db.save_symbols_date_range_delta(delta_date)  # 5 Usual run
#db.load_multipliers(type='refresh')  # 6 May not be needed always, only on corporate actions
db.update_adjusted_price(delta_date)  # 7 # Usual run
#db.load_historical_index_components()  # 8 May not be needed always, after mont-end
# db.symbols_index_hist_files_delta(delta_date)  # 9 for amibroker
#db.create_amibroker_import_files(path=AMI_PATH, start_date=delta_date)  # 10 for amibroker
#db.create_amibroker_import_files_index_incexc(index='Nifty 50', path=AMI_PATH, start_date=delta_date)  # 11 for amibroker
