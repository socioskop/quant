# program to fetch macro data and combine in one data frame for learning input
from dotenv import load_dotenv

load_dotenv()

import quandl
import os
import pandas as pd
import sqlite3
import tiingo as tngo
import yfinance as yf
import talib
import time
from datetime import datetime

# connect to database (will be created on first run)
conn   = sqlite3.connect(os.environ["DB_PATH"]+'/quant.db')
cursor = conn.cursor()

# open quandl session - keep key as export in ./.env
quandl.ApiConfig.api_key = os.environ['QUANDL_API_KEY']

# open tiingo session - keep key as export in ./.env
config = {} # Tiingo session configuration dictionary
config['api_key'] = os.environ['TIINGO_API_KEY']
client = tngo.TiingoClient(config)

# time interval
dates = {"init": "1995-01-01",
            "endd": datetime.today().strftime('%Y-%m-%d')}

cars = quandl.get("FRED/TOTALSA"); cars.columns = ["cars"]
clms = quandl.get("FRED/ICSA"); clms.columns = ["clms"]
perm = quandl.get("FRED/PERMITS"); perm.columns = ["perm"]
awem = quandl.get("FRED/AWHMAN"); awem.columns = ["awem"]
ordr = quandl.get("FRED/ACOGNO"); ordr.columns = ["ordr"]
nrdr = quandl.get("FRED/NEWORDER"); nrdr.columns = ["nrdr"]
lind = quandl.get("FRED/USSLIND"); lind.columns = ["lind"]
span = quandl.get("USTREASURY/YIELD")
span["span"] = span["10 YR"]-span["1 YR"]
span = span[["span"]] ## got it

spx  = client.get_dataframe("SPY", startDate=dates["init"], endDate=dates["endd"])
spx["spx"] = spx["adjClose"]
spx = spx[["spx"]] #.apply(np.log)

vix = yf.Ticker("^VIX").history(start=dates["init"], end=dates["endd"])
vix["vix"] = vix["Close"] #.apply(np.log)
vix = vix[["vix"]]

# merge together all macro data sources by datetime index
d =  cars.merge(clms, how="outer", left_index=True, right_index=True)
d =     d.merge(perm, how="outer", left_index=True, right_index=True)
d =     d.merge(awem, how="outer", left_index=True, right_index=True)
d =     d.merge(ordr, how="outer", left_index=True, right_index=True)
d =     d.merge(nrdr, how="outer", left_index=True, right_index=True)
d =     d.merge(lind, how="outer", left_index=True, right_index=True)
d =     d.merge(span, how="outer", left_index=True, right_index=True)
d =     d.merge(vix , how="outer", left_index=True, right_index=True)
d.index = pd.to_datetime(d.index).tz_localize('UCT') # fix time zone merge issue
d =     d.merge(spx , how="outer", left_index=True, right_index=True)

# cut & fill missings with last value carried forward
d = d['1995-05-01 00:00:00+0000':d.index.max()]
d = d.ffill()

# generate derived variables
for v in d.columns[~d.columns.isin(["spx"])]:
    for p in [50, 200]:
        d[v + "_MA" + str(p).zfill(3)] = round(talib.MA(d[v], p)/d[v], 5)

# write to sql database (overwrite)
d = d.reset_index().rename(columns={"index": "date"})
d = d.drop(columns=["cars", "clms", "perm", "awem", "ordr", "nrdr", "span", "spx"])
d.to_sql('macro', conn, if_exists='replace', index=False)

# check contents of db
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# end
time.sleep(10)
print("done getting macro data")
