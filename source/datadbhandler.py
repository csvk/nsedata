"""
Created on Jun 10, 2018
@author: Souvik
@Program Function: Adjust NSE EOD Data
"""

import os
import dates, utils
import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine
from collections import OrderedDict


class DataDB:
    """ Historical Bhavcopy data"""

    # constants

    BONUS_SPLITS_FILE = 'modifiers/bonus_splits.csv'
    UNVERIFIED_SKIPPED_RECORDS_FILE = 'unverified_skipped_records.csv'
    UNVERIFIED_SELECTED_RECORDS_FILE = 'unverified_selected_records.csv'
    REPLACEBLE_SELECTED_RECORDS = 'replaceble_selected_records.csv'
    REPLACEBLE_SKIPPED_RECORDS = 'replaceble_skipped_records.csv'

    def __init__(self, db, type='EQ'):

        # variables

        self.TYPE = type

        print('Opening Bhavcopy database {}...'.format(db))
        self.conn = sqlite3.connect(db)
        self.engine = create_engine('sqlite:///{}'.format(db))

    def __del__(self):

        print('Closing DB connection..')
        self.conn.close()

    def load_multipliers(self, csvpath):


        #c = self.conn.cursor()
        #c.execute(truncate_query)
        #self.conn.commit()

        multipliers = pd.read_csv(csvpath + self.BONUS_SPLITS_FILE)
        read_count = len(multipliers.index)

        print('Initiating loading of {} records into tblMultipliers'.format(read_count))

        multipliers['ResultantMultiplier'] = multipliers['Multiplier']

        for symbol in multipliers['Symbol'].unique():
            symbol_multipliers = multipliers[multipliers.Symbol == symbol].copy()
            symbol_multipliers['ResultantMultiplier'] = np.cumprod(symbol_multipliers['Multiplier'])
            print(symbol_multipliers)

    def insert_records(self, df, table_name):
        """
        Insert passed records into table_name
        """

        df.to_sql(table_name, self.engine, index=False, if_exists='append')

    def load_table_from_csv(self, csv_path, start_date='1900-01-01', table_name='tblDump', type='append'):
        """
        Load table with historical data : Deprecated
        :param start_date: start date from which data files need to be considered
        :param csv_path: path of historical data in csv format
        :param table_name: table to be loaded
        :param type: 'append' if only new dates are to be loaded, 'refresh' if table needs to be refreshed
        :return: None
        """

        c = self.conn.cursor()

        if type == 'refresh':
            print('Truncating table')
            truncate_query = '''DELETE FROM {}'''.format(table_name)
            c.execute(truncate_query)
            self.conn.commit()

        csv_files = [f for f in os.listdir(csv_path) if f.endswith('.txt') and f[0:10] >= start_date]
        csv_files.sort()

        print('Initiating loading of {} files'.format(len(csv_files)))

        col_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        year = '1900'
        skipped_records = pd.DataFrame(col_names)
        read_count, write_count, skipped_count = 0, 0, 0
        for file in csv_files:
            if year != file[0:4]:
                year = file[0:4]
                print('reading records for year {}'.format(year))
            df = pd.read_csv(csv_path + file, names=col_names, header=None)
            read_count = read_count + len(df.index)

            for idx, row in df.iterrows():
                insert_row = (row['Symbol'], row['Date'], row['Open'], row['High'], row['Low'], row['Close'],
                              row['Volume'])
                try:
                    c.execute('''INSERT INTO {} VALUES (?,?,?,?,?,?,?)'''.format(table_name), insert_row)

                except sqlite3.IntegrityError:
                    skipped_count = skipped_count + 1
                    skipped_record = [('Symbol', row['Symbol']), ('Date', row['Date']), ('Open', row['Open']),
                                      ('High', row['High']), ('Low', row['Low']), ('Close', row['Close']),
                                      ('Volume', row['Volume'])]
                    skipped_records = pd.concat([skipped_records, pd.DataFrame([OrderedDict(skipped_record)])], axis=0)

            if read_count - write_count > 20000:
                self.conn.commit()
                write_count = read_count - skipped_count
                print('inserted {} records, skipped {} records till now'.format(write_count, skipped_count))

        self.conn.commit()
        write_count = read_count - skipped_count
        c.close()
        skipped_records.to_csv(self.SKIPPED_RECORDS_FILE, sep=',', index=False)

        print('{} files processed, {} records inserted'.format(len(csv_files), write_count))

    def load_tbldumps_from_csv(self, csv_path, start_year='1995'):
        """
        Load table with historical data
        :param start_year: start year from which data files need to be considered
        :param csv_path: path of historical data in csv format
        :return: None
        """

        years = ['1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007',
                 '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']

        years_to_load = [year for year in years if year >= start_year]

        c = self.conn.cursor()

        print('Truncating tables')
        for year in years_to_load:
            truncate_query = '''DELETE FROM tblDump{}'''.format(year)
            c.execute(truncate_query)
            self.conn.commit()

        csv_files = [f for f in os.listdir(csv_path) if f.endswith('.txt') and f[0:4] >= start_year]
        csv_files.sort()

        print('Initiating loading of {} files'.format(len(csv_files)))

        col_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        year = '1900'
        skipped_records = pd.DataFrame()
        duplicate_records = pd.DataFrame()
        read_count, write_count, skipped_count = 0, 0, 0
        for file in csv_files:
            if year != file[0:4]:
                if year != '1900': # Write records to yearly tblDumps
                    year_unique = year_records.drop_duplicates(['Symbol', 'Date'], keep='first')
                    year_duplicate = year_records[year_records.duplicated(['Symbol', 'Date'], keep=False)]
                    year_skipped = year_records[year_records.duplicated(['Symbol', 'Date'], keep='first')]
                    self.insert_records(year_unique, table_name='tblDump{}'.format(year))
                    write_count = write_count + len(year_unique.index)
                    skipped_records = pd.concat([skipped_records, year_skipped], axis=0)
                    duplicate_records = pd.concat([duplicate_records, year_duplicate], axis=0)
                    print('{}: inserted {} records, duplicate {} records, skipped {} records'.format(
                        year, len(year_unique.index), len(year_duplicate.index), len(year_skipped.index)))

                year = file[0:4]
                print('reading records for year {}'.format(year))
                year_records = pd.DataFrame()

            df = pd.read_csv(csv_path + file, names=col_names, header=None)
            year_records = pd.concat([year_records, df], axis=0)
            read_count = read_count + len(df.index)

        # Write records for final year
        year_unique = year_records.drop_duplicates(['Symbol', 'Date'], keep='first')
        year_duplicate = year_records[year_records.duplicated(['Symbol', 'Date'], keep=False)]
        year_skipped = year_records[year_records.duplicated(['Symbol', 'Date'], keep='first')]
        self.insert_records(year_unique, table_name='tblDump{}'.format(year))
        write_count = write_count + len(year_unique.index)
        skipped_records = pd.concat([skipped_records, year_skipped], axis=0)
        duplicate_records = pd.concat([duplicate_records, year_duplicate], axis=0)
        print('{}: inserted {} records, duplicate {} records, skipped {} records'.format(year, len(year_unique.index),
                                                                                         len(year_skipped.index),
                                                                                         len(year_duplicate.index)))

        c.close()
        self.insert_records(skipped_records, table_name='tblSkipped')
        self.insert_records(duplicate_records, table_name='tblDuplicates')

        print('{} files processed, {} records read, {} records inserted, {} records duplicate, {} records skipped'.format(
            len(csv_files), read_count, write_count, len(duplicate_records.index), len(skipped_records.index)))

    def int(self, val):
        try:
            return int(val)
        except ValueError:
            return 0

    def error(self, record, prev_record, next_record):

        p_open, n_open, r_open = self.int(prev_record['Open'].iloc[0]), self.int(next_record['Open'].iloc[0]), \
                                 self.int(record['Open'].iloc[0])
        p_high, n_high, r_high = self.int(prev_record['High'].iloc[0]), self.int(next_record['High'].iloc[0]), \
                                 self.int(record['High'].iloc[0])
        p_low, n_low, r_low = self.int(prev_record['Low'].iloc[0]), self.int(next_record['Low'].iloc[0]), \
                                 self.int(record['Low'].iloc[0])
        p_close, n_close, r_close = self.int(prev_record['Close'].iloc[0]), self.int(next_record['Close'].iloc[0]), \
                              self.int(record['Close'].iloc[0])
        p_volume, n_volume, r_volume = self.int(prev_record['Volume'].iloc[0]), self.int(next_record['Volume'].iloc[0]), \
                                    self.int(record['Volume'].iloc[0])

        open_e = (p_open + n_open) / 2 - r_open
        high_e = (p_high + n_high) / 2 - r_high
        low_e = (p_low + n_low) / 2 - r_low
        close_e = (p_close + n_close) / 2 - r_close
        volume_e = (p_volume + n_volume) / 2 - r_volume

        return {'price_error': open_e ** 2 + high_e ** 2 + low_e ** 2 + close_e ** 2,
                'volume_error': volume_e ** 2}
        
    def calculate_skipped_record_errors(self):

        skipped_qry = 'SELECT DISTINCT * from tblSkipped'
        skipped_records = pd.read_sql_query(skipped_qry, self.conn)

        unverified_skipped_records = pd.DataFrame()
        unverified_selected_records = pd.DataFrame()
        replaceable_selected_records = pd.DataFrame()
        replaceable_skipped_records = pd.DataFrame()

        for symbol in skipped_records['Symbol'].unique():
            print('checking for {}'.format(symbol))
            symbol_records = skipped_records[skipped_records.Symbol == symbol]
            for date in symbol_records['Date'].unique():
                date_records = symbol_records[symbol_records.Date == date]
                prev_sel_record_qry = '''SELECT * FROM tblDump{} WHERE Symbol = "{}" AND Date < "{}"
                                          ORDER BY Date DESC LIMIT 1'''.format(date[0:4], symbol, date)
                sel_record_qry = '''SELECT * FROM tblDump{} 
                                     WHERE Symbol = "{}" AND Date = {}'''.format(date[0:4], symbol, date)
                next_sel_record_qry = '''SELECT * FROM tblDump{} WHERE Symbol = "{}" AND Date > {}
                                          ORDER BY Date ASC LIMIT 1'''.format(date[0:4], symbol, date)

                prev_sel_record = pd.read_sql_query(prev_sel_record_qry, self.conn)
                sel_record = pd.read_sql_query(sel_record_qry, self.conn)
                next_sel_record = pd.read_sql_query(next_sel_record_qry, self.conn)

                if len(prev_sel_record.index) == 0 and len(next_sel_record.index) == 0:
                    for i in range(0, len(date_records.index)):
                        unverified_skipped_records = pd.concat([unverified_skipped_records,
                                                                date_records.iloc[i:i + 1, :]], axis=0)
                        unverified_selected_records = pd.concat([unverified_selected_records, sel_record], axis=0)
                else:
                    if len(prev_sel_record.index) == 0 and len(next_sel_record.index) > 0:
                        prev_sel_record = next_sel_record
                    elif len(prev_sel_record.index) > 0 and len(next_sel_record.index) == 0:
                        next_sel_record = prev_sel_record

                    sel_error = self.error(sel_record, prev_sel_record, next_sel_record)
                    skipped_error = []
                    for i in range(0, len(date_records.index)):
                        skipped_error.append(self.error(date_records.iloc[i:i+1, :], prev_sel_record, next_sel_record))
                        if skipped_error[i]['price_error'] < sel_error['price_error'] or \
                                skipped_error[i]['volume_error'] < sel_error['volume_error']:
                            replaceable_skipped_records = pd.concat([replaceable_skipped_records,
                                                                    date_records.iloc[i:i + 1, :]], axis=0)
                            replaceable_selected_records = pd.concat([replaceable_selected_records, sel_record], axis=0)

        unverified_skipped_records.to_csv(self.UNVERIFIED_SKIPPED_RECORDS_FILE, sep=',', index=False)
        unverified_selected_records.to_csv(self.UNVERIFIED_SELECTED_RECORDS_FILE, sep=',', index=False)
        replaceable_selected_records.to_csv(self.REPLACEBLE_SELECTED_RECORDS, sep=',', index=False)
        replaceable_skipped_records.to_csv(self.REPLACEBLE_SKIPPED_RECORDS, sep=',', index=False)

