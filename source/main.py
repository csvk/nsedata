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
#db.load_tbldumps_from_csv(CSVPATH, start_year='1995')  # 1
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
#db.test_inc_exc_mod_list()
db.symbols_index_hist_files()  # 9
#db.symbols_index_hist_files(start_year='2007', end_year='2018')
#db.symbols_index_hist_files(indices=('Nifty 50', 'Nifty 100'))
#db.test_inc_exc_index_data()  # 10
#db.update_symbol_change() # Use if symbol changed, and db update is delayed
#db.create_amibroker_import_files(path=AMI_PATH) # 11
#db.create_amibroker_import_files_index_incexc(index='Nifty 50', path=AMI_PATH, type='full')

# For delta run:
#db.append_tbldumps_from_csv(CSVPATH, start_date='2018-04-11')
#db.calculate_skipped_record_errors(start_date='20180411')
#db.load_dump_replace_records()  # May not be needed always
#db.save_symbols_date_range()  # May not be needed always
#db.load_multipliers(type='refresh')  # May not be needed always
#db.append_modified_tbldumps('20180411')
#db.update_adjusted_price('20180411')
#db.create_amibroker_import_files(path=AMI_PATH) # 11
#db.create_amibroker_import_files_index_incexc(index='Nifty 50', path=AMI_PATH, start_date='20180720')
