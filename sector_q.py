# program to develop MAs for percentile positions within sectors
from dotenv import load_dotenv

load_dotenv()

import os
import pandas as pd
import sqlite3
import numpy as np
import talib

# connect to database (will be created on first run)
conn   = sqlite3.connect(os.environ["DB_PATH"]+'/quant.db')
cursor = conn.cursor()

# load list of tickers to retrieve data for
tick = pd.read_sql_query("SELECT * FROM tickers", conn)
raw  = pd.read_sql_query("SELECT * FROM raw", conn)

sectors = pd.Series(tick["sector"].drop_duplicates())
tickers = pd.Series(tick["ticker"].drop_duplicates())
dates   = pd.Series(raw.date)
dates   = dates.drop_duplicates()

# generate sector wise deltas for position relations
d = pd.DataFrame(dates, columns=["date"])
d = d.sort_values(by="date")

z = pd.DataFrame(dates, columns=["date"])   # sector performances

for sector in set(sectors):
    print(sector+" in progress")
    s = pd.DataFrame(dates, columns=["date"])
    s = s.sort_values(by="date")

    # tickers are pulled per ticker, day-to-day deltas are carried forward
    sectics = tickers[tickers.isin(tick["ticker"][tick["sector"].isin([sector])])]
    if len(sectics)==0:
        continue
    for ticker in sectics:
        tmp = raw.loc[raw["ticker"]==ticker, ["date", "adjClose"]]
        tmp.columns = ["date", ticker]
        tmp = tmp.groupby('date').last()                    # avoid data errors to blow up rows/memory/merging

        tmp[ticker] = np.log(tmp[ticker]).diff()+1          # differentiate
        for p in [21, 50, 200]:
            tmp[ticker+"_MA"+str(p).zfill(3)] = round(talib.MA(tmp[ticker], p), 5)
        s = pd.merge(s, tmp, on="date", how="outer")
        del tmp

    # dynamic name vectors
    sectics.ok = sectics[sectics.isin(s.columns)]
    qnames = [t + "_q" for t in sectics.ok]
    mnames = [t + "_MA050" for t in sectics.ok]

    # add percentile position for each day based on MA
    tmp = s[["date"]+mnames]                                # base quantiles on MA named above
    tmp = tmp[tmp.isna().sum(axis=1)<(len(tmp.columns)-1)]  # drop rows with only NAs
    q = round(tmp[mnames].rank(axis=1, method="dense", na_option="keep", pct=True)*100, 1)
    q.columns = qnames
    q["date"] = tmp["date"]
    del tmp

    # sector performance indicator
    s[sector.replace(" ", "")] = s[sectics.ok].mean(axis=1)
    s = s[["date", sector.replace(" ", "")]]

    # generate MA(q)
    u = []
    for p in [50, 200]:
        for ticker in sectics.ok:
            q[ticker + "_q" + str(p).zfill(3)] = round(talib.MA(q[ticker+"_q"], p), 5)
        unames = [t + "_q"+str(p).zfill(3) for t in sectics.ok]
        tmp = q[["date"]+unames] #### Here: lær at bruge dictionary
        #u[str(p)] = q[["date"]+unames]
        tmp = pd.melt(tmp, id_vars=['date'], var_name='ticker', value_name="q"+str(p))
        tmp["ticker"] = tmp["ticker"].str.replace('_q'+str(p).zfill(3), '')
        u.append(tmp)
        del tmp

    u = pd.concat(u, axis=1)                # concat the list elements holding qMA
    u = u.loc[:,~u.columns.duplicated()]    # remove duped date/ticker columns
    d = pd.concat([d, u], ignore_index=True)# and again with running d holder
    d = d.loc[:,~d.columns.duplicated()]    # remove duped date/ticker columns

    # sector performance indicator
    z = pd.merge(z, s, on="date", how="left")
    z = z.drop_duplicates()                 # memory issue prevention
    z = z.sort_values(by="date")

# write to sql database (overwrite)
d.to_sql('sector_q', conn, if_exists='replace', index=False)
z.to_sql('sector_z', conn, if_exists='replace', index=False)

# check contents of db
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# end
print("done getting sector q data")