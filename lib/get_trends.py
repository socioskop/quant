# getting google trend data for supplementary macro signals
from dotenv import load_dotenv

load_dotenv()

import re
import os
import sqlite3
import pandas as pd
import tiingo as tngo
import numpy as np
from datetime import datetime
from pytrends import dailydata
from pytrends.request import TrendReq

# connect to database (will be created on first run)
conn   = sqlite3.connect(os.environ["DB_PATH"]+'/quant.db', timeout=10)
cursor = conn.cursor()

# open tiingo session - keep key as export in ./.env
config = {} # Tiingo session configuration dictionary
config['api_key'] = os.environ['TIINGO_API_KEY']
client = tngo.TiingoClient(config)

# settings
dates = {"init": "2005-01-01",
            "endd": datetime.today().strftime('%Y-%m-%d')}
settings = {'wait': 5,
            'now_time': 25,
            'lookback': 25,
            'first_year': 2012}

# search terms
terms = ["debt", "color", "stocks", "money", "oil", "war", "fine", "office",
         "credit", "inflation", "portfolio", "mortgage", "hedge", "derivatives",
         "inflation", "housing", "loan", "unemployment", "payment", "celebration", "party",
         "cancer", "marriage", "sp500", "dow jones", "growth", "restaurant",
         "game", "vacation", "stress", "credit card", "job", "used car", "disneyland",
         "russell 2000"] #"etf", "xauusd"

# SPX for reference y and for date index
d = client.get_dataframe("SPY", startDate=dates["init"], endDate=dates["endd"])
d["spx"] = d["adjClose"]
d = d[["spx"]].apply(np.log)
#d = d.reset_index()
d.index = pd.to_datetime(d.index).tz_convert(None) # fix time zone merge issue

# create or update gtrends' spx and date columns
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
if 'gtrends' not in str(cursor.fetchall()):
    d.to_sql('gtrends', conn, if_exists='fail', index=True)
else:
    d.to_sql('tmp', conn, if_exists='replace', index=True) # write all fresh spx data (and their dates)

    # create a temp table with only the data not already in gtrends
    cursor.execute("DROP TABLE IF EXISTS fresh;")
    update = '''CREATE TABLE fresh AS
                SELECT tmp.date, tmp.spx
                FROM   tmp
                    LEFT JOIN gtrends
                        ON gtrends.date = tmp.date
                        WHERE gtrends.date IS NULL;'''
    cursor.execute(update)

    # append the fresh data to existing gtrends spx observations
    update = '''INSERT INTO gtrends(date, spx)
                select date, spx
                from fresh;'''
    cursor.execute(update)

# existing columns
got_words = conn.execute('select * from gtrends')
got_words = list(map(lambda x: x[0], got_words.description))

# dates used
spx_dates = pd.read_sql_query("SELECT date FROM gtrends", conn)
spx_dates = spx_dates["date"]
now_dates = spx_dates.tail(settings['now_time'])

# pytrends connection
pytrends = TrendReq(hl='en-US', tz=360)

# loop through search terms while adding or updating database
for t in terms:

    # search term as column name (trimming all whitespaces and max 10 chars)
    trim_t = re.sub(" ", "", t)[:10]

    # identify existing data
    if trim_t in str(got_words):
        get_dates = pd.read_sql_query("SELECT date, " + trim_t + " FROM gtrends", conn)
        get_dates = get_dates["date"].loc[get_dates[trim_t].isnull()]
        get_dates = get_dates.tail(settings['lookback'])

    else:
        get_dates = spx_dates.tail(settings['lookback'])

        # adding the column to table
        cursor.execute('ALTER TABLE gtrends ADD COLUMN ' + trim_t + ';')

    # make sure dates are unique
    get_dates = get_dates[~get_dates.isin(now_dates)]

    # request gtrends data in monthly intervals
    tmp_now = dailydata.get_daily_data(t,
                                       int(now_dates.str.slice(0, 4).min()), int(now_dates.str.slice(5, 7).min()),
                                       int(now_dates.str.slice(0, 4).max()), int(now_dates.str.slice(5, 7).max()),
                                       geo='', wait_time=settings['wait'])
    tmp_now = tmp_now[[t]]

    if int(get_dates.str.slice(0, 4).min())>=settings['first_year']:

        # pull old data, if its not before the first_year threshold
        tmp_get = dailydata.get_daily_data(t,
                                           int(get_dates.str.slice(0, 4).min()), 1,
                                           int(get_dates.str.slice(0, 4).max()), int(get_dates.str.slice(5, 7).max()),
                                           geo='', wait_time=settings['wait'])
        tmp = pd.concat([tmp_get[[t]], tmp_now[[t]]])   # stack with the current data

        # rename keyword column to trimmed
        tmp[[trim_t]] = tmp[[t]]

    else:
        tmp = tmp_now[[t]]
        tmp[[trim_t]] = tmp[[t]]

    # approximate recent (last 36 hours) of gtrends and append to regular daily gtrend data
    try:
        pytrends.build_payload([t], cat=0, timeframe='now 7-d', geo='', gprop='')   # pull recent hourly data
        recent = pytrends.interest_over_time()[[t]]
        recent = recent.reset_index()                                               # convert index to column for groupby
        recent.columns = ["date", t + "_daily"]                                     # rename cols
        recent = recent.groupby(pd.Grouper(freq='D', key='date')).median()          # summarizy from hourly to daily
        recent = pd.merge(tmp, recent, how="outer", left_index=True, right_index=True)  # combine with daily data to enable rescaling
        scaling_factor = (recent[t] / recent[t+"_daily"]).mean()                        # find approximate scaling factor
        recent[trim_t] = recent[t+"_daily"] * (scaling_factor)                      # rescale hourly converted to daily
        recent = recent[~recent.index.isin(tmp.index)]                              # keep only dates that are missing in regular daily data
        tmp = pd.concat([tmp, recent[[trim_t]]])                                    # append the recent approximated values
    except:
        pass

    # write to tmp sql table to enable merging on date match
    tmp = tmp.loc[~tmp[trim_t].isnull()]
    tmp.to_sql('tmp', conn, if_exists='replace', index=True)
    print(tmp.tail(3))

    # update new rows in db
    update = 'update gtrends set '+trim_t+' = (SELECT '+trim_t+' from tmp where date = gtrends.date) where EXISTS (SELECT '+trim_t+' FROM tmp where date = gtrends.date)'
    cursor.execute(update)

gtrends = pd.read_sql_query("SELECT * FROM gtrends", conn)
print(gtrends.tail(5))
words = conn.execute('select * from gtrends')
words = list(map(lambda x: x[0], words.description))
print(words)

