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

os.chdir(PATH)

db = dbhandler.DataDB(DBPATH)


#db.load_tbldumps_from_csv(CSVPATH, start_year='1995')
#db.calculate_skipped_record_errors()
#db.load_dump_replace_records()
#db.load_modified_tbldumps()
#db.fetch_records('tblModDump', ['TCS', 'RELIANCE'], '19950531', '20171011')
#db.fetch_records('tblModDump', ['PDUMJEIND', 'PITTILAM'], '19950101', '20181231')
db.save_symbols_date_range()
#db.load_multipliers(type='refresh')
#db.update_adjusted_price()
#db.index_change_xl_to_csv()
#db.check_symbol_dates()