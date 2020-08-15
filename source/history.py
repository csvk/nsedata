"""
Created on Aug 12, 2020
@author: Souvik
@Program Function: Read price history
"""

import datadbhandler as dbhandler
import numpy as np
import pandas as pd

class History:
    """Read historical price data from database"""

    def __init__(self, dbpath):
        self.db = dbhandler.DataDB(dbpath)
        
    def __del__(self):
        pass

    def date_check(self, start_date, end_date, buffer_start=0):
        """Perform parameter date checks"""
        assert int(start_date) <= int(end_date), 'End date {} cannot be less than start date {}'.format(
                end_date, start_date)

        if buffer_start == 0:
            buffer_start = start_date
        else:
            assert int(buffer_start) <= int(start_date), 'Buffer start date {} must be less than equals start date {}'.format(
                buffer_start, start_date)

        return buffer_start
    
    def symbol_history(self, symbol, start_date, end_date, buffer_start=0):
        """
        Return historical price data for symbol
        :param symbol: symbol name
        :param start_date: start date from which trading is allowed
        :param end_date: end date till when trading is allowed
        :param buffer_start: date before start date fetched to aid calculations
        :return: ohlcv + split/bonus adjusted ohlc for symbol + buffer indicator for buffer days before start date
        """

        print('fetching data for ', symbol)

        buffer_start = self.date_check(start_date, end_date, buffer_start)

        df = self.db.fetch_records('tblModDump', [symbol], buffer_start, end_date)

        df['Buffer'] = np.where(df.Date.astype(int) < int(start_date), True, None)

        if not df.empty and df.AdjustedOpen.iloc[0] is None:
            df.AdjustedOpen = df.Open
            df.AdjustedHigh = df.High
            df.AdjustedLow = df.Low
            df.AdjustedClose = df.Close

        return df

    def index_change_history(self, index):
        """
        Return historical component data for index
        :param index: index name
        :return: histIndex: tblHistIndex data for given index
                 histIndexDates: distinct dates signifying index change dates for given index
        """

        histIndex = pd.read_sql("SELECT * FROM tblHistIndex WHERE IndexName = '{}' ORDER BY Date".format(index), \
            self.db.conn)
        histIndexDates = pd.read_sql("SELECT DISTINCT Date FROM tblHistIndex WHERE IndexName = '{}' ORDER BY Date".format(
            index), self.db.conn)

        return histIndex, histIndexDates

    def symbol_history_indexflagged(self, symbol, index, start_date, end_date, buffer_start=0):
        """
        Return historical price data for symbol with additional flag to indicate if it was part of an index
        :param symbol: symbol name
        :param index: index to mark index flag to indicate if symbol was part of index on a given date
        :param start_date: start date from which trading is allowed
        :param end_date: end date till when trading is allowed
        :param buffer_start: date before start date fetched to aid calculations
        :return: ohlcv + split/bonus adjusted ohlc for symbol + buffer indicator for buffer days before start date
                 + index flag
        """

        buffer_start = self.date_check(start_date, end_date, buffer_start)

        symbol_hist = self.symbol_history(symbol, start_date, end_date, buffer_start)

        histIndex, histIndexDates = self.index_change_history(index)

        # Index change dates when symbol was part of index
        df_symbol = histIndex[histIndex.Symbol == symbol]

        # All index change dates with info on symbol present or absent
        df_index_symbol = pd.merge(histIndexDates, df_symbol, how='left', on=['Date']).sort_values(by='Date')
        df_index_symbol.Symbol = np.where(df_index_symbol.IndexName.notnull(), True, np.nan)

        # Marking dates when symbol was removed from index - 1.0 = Present, 0.0 = Removal, NaN = Absent
        df_index_symbol.Symbol.fillna(method='ffill', inplace=True, limit=1) 
        df_index_symbol.Symbol = np.where((df_index_symbol.IndexName.isnull()) & \
            (df_index_symbol.Symbol.notnull()), False, df_index_symbol.Symbol)
        # Removing NaN records, renaming columns
        df_index_symbol = df_index_symbol[df_index_symbol.Symbol.notnull()][['Date', 'Symbol']]
        df_index_symbol.columns = ['Date', 'IndexFlag']

        # Symbol in index on buffer_start?
        try:
            index_flag_buffer_start = df_index_symbol[df_index_symbol.Date.astype(int) < \
                int('20050801')].sort_values(by='Date', ascending=False).iloc[0].IndexFlag
        except IndexError:
            index_flag_buffer_start = False

        df = pd.merge(symbol_hist, df_index_symbol, how='left', on=['Date']).sort_values(by='Date')

        # Fill up for all dates
        df.IndexFlag.fillna(method='ffill', inplace=True) 
        if index_flag_buffer_start == 1.0:
            df.IndexFlag.fillna(method='bfill', inplace=True) 
        # Mark dates when symbol not part of index as null
        df.IndexFlag = np.where(df.IndexFlag is False, np.nan, df.IndexFlag) 

        return df

    def index_components_history(self, index, start_date, end_date, buffer_start=0):
        """
        Return historical price data for symbols historically part of index
        :param index: index name
        :param start_date: start date from which trading is allowed
        :param end_date: end date till when trading is allowed
        :param buffer_start: date before start date fetched to aid calculations
        :return: dict of dataframes with pricing history {symbol1: symbol1_history, symbol2: symbol2_history, ....}
        """

        buffer_start = self.date_check(start_date, end_date, buffer_start)

        histIndex, _ = self.index_change_history(index)

        symbols = histIndex.Symbol.unique() # symbols historically part of index

        data = dict()

        for symbol in symbols:
            symbol_data = self.symbol_history_indexflagged(symbol, index, start_date, end_date, buffer_start)
            if symbol_data.IndexFlag.sum() > 0: # symbol part of index during requested period
                data[symbol] = symbol_data

        return data

