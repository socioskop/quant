# program to fetch raw pricing data for tickers through tiingo interface
# saves data in sqlite table 'raw'
import tiingo as tngo
import os
import env_file
import pandas as pd
import sqlite3
from datetime import datetime

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

# load list of tickers to retrieve data for
tickers = pd.read_sql_query("SELECT * FROM tickers", conn)
ts = pd.Series(tickers.ticker.unique())

# touch sql storage
touch = '''CREATE TABLE IF NOT EXISTS raw (
	    date      TEXT,
	    ticker    TEXT,
	    adjClose  REAL,
	    adjOpen   REAL,
	    adjHigh   REAL,
	    adjLow    REAL,
	    adjVolume REAL,
	    divCash   REAL
	    );'''

cursor.execute(touch)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# time interval
dates = {"init": "1995-01-01",
            "endd": datetime.today().strftime('%Y-%m-%d')}

# get all tickers, one at a time
for i in range(0, len(ts)):

    # clear looping parametres
    init_tmp = ""

    # get existing max date for data
    query = 'SELECT date FROM raw where ticker="' + ts[i] + '";'
    dates_covered = pd.read_sql_query(query, conn)
    dates_covered = pd.to_datetime(dates_covered['date'], format='%Y-%m-%d')

    # wipe values once in a while to avoid missing adjustments or ticker switch
    if datetime.today().weekday()!=5 and len(dates_covered)>500:
        init_tmp = max(dates_covered)
        if init_tmp.strftime('%Y-%m-%d')>=dates["endd"]:
            continue
        #d = d[-d["date"].isin(dates_covered)] # would be after data download
    else:
        init_tmp = dates["init"]

    # download data from tiingo,
    d = client.get_dataframe(str(ts[i]), startDate=init_tmp, endDate=dates["endd"])
    d["date"] = d.index.strftime('%Y-%m-%d')    # add date as column
    d["ticker"] = ts[i]                         # add ticker as column
    d = d[['date', 'ticker', 'adjClose', 'adjHigh', 'adjLow', 'adjOpen', 'adjVolume', 'divCash']]

    # write to local storage
    d.to_sql('raw', conn, if_exists='append', index=False)

    if (i/100 == round(i/100)):
        print("raw data for ticker ", ts[i], " is ok. (" + str(i), " of ", len(ts), ")", sep='')

print("done getting raw ticker data")
