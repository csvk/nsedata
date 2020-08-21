-- Sample SQL queries

select symbol, count(date), min(date), max(date) from (
select symbol, date from tblDump1995
union select symbol, date from tblDump1996
union select symbol, date from tblDump1997
union select symbol, date from tblDump1998
union select symbol, date from tblDump1999
union select symbol, date from tblDump2000
union select symbol, date from tblDump2001
union select symbol, date from tblDump2002
union select symbol, date from tblDump2003
union select symbol, date from tblDump2004
union select symbol, date from tblDump2005
union select symbol, date from tblDump2006
union select symbol, date from tblDump2007
union select symbol, date from tblDump2008
union select symbol, date from tblDump2009
union select symbol, date from tblDump2010
union select symbol, date from tblDump2011
union select symbol, date from tblDump2012
union select symbol, date from tblDump2013
union select symbol, date from tblDump2014
union select symbol, date from tblDump2015
union select symbol, date from tblDump2016
union select symbol, date from tblDump2017
union select symbol, date from tblDump2018) group by symbol

select symbol, min(MinDate) MinDate, max(MaxDate) MaxDate from (
      select symbol, min(date) MinDate, max(date) MaxDate from tblDump1995 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump1996 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump1997 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump1998 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump1999 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2000 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2001 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2002 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2003 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2004 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2005 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2006 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2007 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2008 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2009 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2010 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2011 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2012 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2013 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2014 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2015 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2016 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2017 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblDump2018 group by symbol) group by symbol

select symbol, min(MinDate) MinDate, max(MaxDate) MaxDate from (
      select symbol, min(date) MinDate, max(date) MaxDate from tblModDump1995 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump1996 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump1997 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump1998 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump1999 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2000 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2001 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2002 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2003 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2004 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2005 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2006 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2007 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2008 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2009 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2010 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2011 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2012 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2013 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2014 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2015 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2016 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2017 group by symbol
union select symbol, min(date) MinDate, max(date) MaxDate from tblModDump2018 group by symbol) group by symbol

SELECT D.*
                   FROM tblDuplicates D LEFT OUTER JOIN tblSkipped S
                     ON D.Symbol = S.Symbol
                    AND D.Date = S.Date
					AND D.Open = S.Open
					AND D.High = S.High
					AND D.Low = S.Low
					AND D.Close = S.Close
					AND D.Volume = S.Volume
				WHERE S.Symbol is Null

select symbol, date, count(date) count from tblDuplicates group by symbol, date order by count desc

select '1995', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump1995
select '1996', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump1996
select '1997', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump1997
select '1998', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump1998
select '1999', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump1999
select '2000', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2000
select '2001', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2001
select '2002', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2002
select '2003', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2003
select '2004', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2004
select '2005', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2005
select '2006', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2006
select '2007', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2007
select '2008', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2008
select '2009', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2009
select '2010', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2010
select '2011', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2011
select '2012', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2012
select '2013', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2013
select '2014', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2014
select '2015', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2015
select '2016', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2016
select '2017', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2017
select '2018', count(*), sum(Open), sum(High), sum(Low), sum(Close), sum(Volume), sum(AdjustedOpen), sum(AdjustedHigh), sum(AdjustedLow), sum(AdjustedClose)  from tblModDump2018


select count(*) c from tblModDump1995
union select count(*) c from tblModDump1996
union select count(*) c from tblModDump1997
union select count(*) c from tblModDump1998
union select count(*) c from tblModDump1999
union select count(*) c from tblModDump2000
union select count(*) c from tblModDump2001
union select count(*) c from tblModDump2002
union select count(*) c from tblModDump2003
union select count(*) c from tblModDump2004
union select count(*) c from tblModDump2005
union select count(*) c from tblModDump2006
union select count(*) c from tblModDump2007
union select count(*) c from tblModDump2008
union select count(*) c from tblModDump2009
union select count(*) c from tblModDump2010
union select count(*) c from tblModDump2011
union select count(*) c from tblModDump2012
union select count(*) c from tblModDump2013
union select count(*) c from tblModDump2014
union select count(*) c from tblModDump2015
union select count(*) c from tblModDump2016
union select count(*) c from tblModDump2017
union select count(*) c from tblModDump2018

SELECT distinct SYMBOL FROM (
SELECT DISTINCT SYMBOL FROM tblModDump1995 union
SELECT DISTINCT SYMBOL FROM tblModDump1996 union
SELECT DISTINCT SYMBOL FROM tblModDump1997 union
SELECT DISTINCT SYMBOL FROM tblModDump1998 union
SELECT DISTINCT SYMBOL FROM tblModDump1999 union
SELECT DISTINCT SYMBOL FROM tblModDump2000 union
SELECT DISTINCT SYMBOL FROM tblModDump2001 union
SELECT DISTINCT SYMBOL FROM tblModDump2002 union
SELECT DISTINCT SYMBOL FROM tblModDump2003 union
SELECT DISTINCT SYMBOL FROM tblModDump2004 union
SELECT DISTINCT SYMBOL FROM tblModDump2005 union
SELECT DISTINCT SYMBOL FROM tblModDump2006 union
SELECT DISTINCT SYMBOL FROM tblModDump2007 union
SELECT DISTINCT SYMBOL FROM tblModDump2008 union
SELECT DISTINCT SYMBOL FROM tblModDump2009 union
SELECT DISTINCT SYMBOL FROM tblModDump2010 union
SELECT DISTINCT SYMBOL FROM tblModDump2011 union
SELECT DISTINCT SYMBOL FROM tblModDump2012 union
SELECT DISTINCT SYMBOL FROM tblModDump2013 union
SELECT DISTINCT SYMBOL FROM tblModDump2014 union
SELECT DISTINCT SYMBOL FROM tblModDump2015 union
SELECT DISTINCT SYMBOL FROM tblModDump2016 union
SELECT DISTINCT SYMBOL FROM tblModDump2017 union
SELECT DISTINCT SYMBOL FROM tblModDump2018)


