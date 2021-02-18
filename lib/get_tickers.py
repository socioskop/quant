# ticker indexing for quant modeling setup
from dotenv import load_dotenv

load_dotenv()

import tiingo as tngo
import os
import pandas as pd
import sqlite3
import time

# connect to database (will be created on first run)
conn   = sqlite3.connect(os.environ["DB_PATH"]+'/quant.db')
cursor = conn.cursor()

# open tiingo session
config = {} # Tiingo session configuration dictionary
config['session'] = True # stay in session across api calls
config['api_key'] = os.environ['TIINGO_API_KEY']
client = tngo.TiingoClient(config)

# get list of tickers with Tiingo support
tickers = pd.DataFrame(client.list_tickers())

# country wise ticker lists
# SPDRS+ indices
indices = pd.DataFrame({'ticker': ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLB", "XLRE", "XLC", "XLU",
                                   "SPY", "GLD", "DIA", "SDY", "MDY", "JNK", "SPYG", "XBI"]})
indices["date"] = "1970-01-01"
indices["name"] = "INDX"

# us sp500 tickers + historical sp500 entries
sp500=pd.read_csv('https://raw.githubusercontent.com/leosmigel/analyzingalpha/master/sp500-historical-components-and-changes/sp500_history.csv',
                  usecols=['date', 'name', 'value'])
sp500.columns = ["date", "name", "ticker"]
sp500 = pd.concat([sp500, indices])
sp500['universe'] = "SP500"

# reading spdr sector codes, including outside of current sp500 to minimize bias in sample
spy     = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx", engine='openpyxl', skiprows=4)
onev    = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-onev.xlsx", engine='openpyxl', skiprows=4)
oneo    = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-oneo.xlsx", engine='openpyxl', skiprows=4)
oney    = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-oney.xlsx", engine='openpyxl', skiprows=4)
sctrs   = pd.concat([spy, onev, oneo, oney])

# format sector data and merge with ticker list
sctrs.columns   = sctrs.columns.str.lower()
sctrs   = sctrs[["ticker", "sector"]]
sctrs["sector"] = sctrs["sector"].replace(["Technology"], 'Information Technology')
sctrs["sector"] = sctrs["sector"].replace(["Basic Materials"], 'Materials')

# add INDEX as sector
spdrs = pd.DataFrame({'ticker': indices["ticker"]})
spdrs["sector"] = "INDX"
sctrs = pd.concat([sctrs, spdrs]).drop_duplicates()
sp500           = pd.merge(sp500[["name", "ticker", "universe"]],
                           sctrs, on="ticker").drop_duplicates()
sp500 = sp500.sort_values("ticker")

# push sp500 universe ticker list to sql db
sp500[sp500.ticker.isin(tickers.ticker)].to_sql('tickers', conn, if_exists='replace', index=False)

# chine szse500 tickers, only current, includes bias (growth/survivorship)
szse500 = pd.read_excel("http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xls&CATALOGID=1954_detail&TABKEY=tab1&ZSDM=399001&random=0.031393467125904594")
szse500 = szse500[["Code", "Name", "Industry"]]
szse500.columns = ["ticker", "name", "sector"]
szse500['universe'] = "SZSE500"     # adding universe label
szse500.ticker = szse500.ticker.astype(str).apply(lambda x: x.zfill(6)) # convert numeric ticker codes to padded strings
szse500.sector = szse500.sector.replace("P    Education", "R    Media") # moves tiny sectors to bigger, similar ones
szse500.sector = szse500.sector.replace("S    Conglomerates", "K    Real Estate")   # only one conglomerate. Could go anywhere, but reit for now
szse500.sector = szse500.sector.replace("M    Research & Development", "Q    Public Health") # same same

# write ticker list to database
szse500[szse500.ticker.isin(tickers.ticker)].to_sql('tickers', conn, if_exists='append', index=False) # append to existing ticker list

# check out that things are as expected: sql table is present
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# ...and ticker sectors are ok (and ok sized)
tickers = pd.read_sql_query("SELECT * FROM tickers", conn)
print(tickers.universe.value_counts())
print(tickers.sector.value_counts(sort=True, ascending=False))

# end
time.sleep(10)
print("done getting ticker indices")