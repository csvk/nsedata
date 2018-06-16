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

#db.load_multipliers(CSVPATH)
#db.load_tbldumps_from_csv(CSVPATH, start_year='1995')
db.calculate_skipped_record_errors()