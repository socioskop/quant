# main program for retrieving data, learning and predicting stock market returns
import tiingo as tngo
import os
import env_file
import pandas as pd
import sqlite3

# connect to database (will be created on first run)
conn   = sqlite3.connect('quant.db')
cursor = conn.cursor()

# add keys and other local environment variables manually
env_file.load('.env')

# open tiingo session
print("Tiingo key ok? :", len(os.environ['TIINGO_API_KEY'])>0)
config = {} # Tiingo session configuration dictionary
config['session'] = True # stay in session across api calls
config['api_key'] = os.environ['TIINGO_API_KEY']
client = tngo.TiingoClient(config)

# get list of tickers with Tiingo support
tickers = pd.DataFrame(client.list_tickers())

# country wise ticker lists
# us sp500 tickers + historical sp500 entries
sp500=pd.read_csv('https://raw.githubusercontent.com/leosmigel/analyzingalpha/master/sp500-historical-components-and-changes/sp500_history.csv',
                  usecols=['date', 'name', 'value'])
sp500.columns = ["date", "name", "ticker"]
sp500['universe'] = "SP500"

# reading spdr sector codes, including outside of current sp500 to minimize bias in sample
spy     = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx", skiprows=4)
onev    = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-onev.xlsx", skiprows=4)
oneo    = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-oneo.xlsx", skiprows=4)
oney    = pd.read_excel("https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-oney.xlsx", skiprows=4)
sctrs   = pd.concat([spy, onev, oneo, oney])

# format sector data and merge with ticker list
sctrs   = sctrs[["Ticker", "Sector"]]
sctrs.columns   = sctrs.columns.str.lower()
sp500           = pd.merge(sp500, sctrs, on="ticker").drop_duplicates()

# push  sp500 universe ticker list to sql db
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

szse500[szse500.ticker.isin(tickers.ticker)].to_sql('tickers', conn, if_exists='append', index=False) # append to existing ticker list

# check out that things are as expected: sql table is present
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# ...and ticker sectors are ok (and ok sized)
tickers = pd.read_sql_query("SELECT * FROM tickers", conn)
print(tickers.universe.value_counts())
print(tickers.sector.value_counts(sort=True, ascending=False))

# sweep temp files
for f in ["./Indices Constituent.xls", "./szse_stock_list.xlsx"]:
    try:
        os.remove(f)
    except:
        print("temp file", f, "not there... moving on")
print("done sweeping temp files, tickers are stored in db")