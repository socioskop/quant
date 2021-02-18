# gets MAs for sector diffs
from dotenv import load_dotenv

load_dotenv()

import os
import pandas as pd
import sqlite3
import talib
import time

# connect to database (will be created on first run)
conn   = sqlite3.connect(os.environ["DB_PATH"]+'/quant.db')
cursor = conn.cursor()

# universes
universes = {'SZSE500': ['NEnvironmentalProtection', 'KRealEstate', 'RMedia', 'BMining', 'DUtilities',
                        'EConstruction', 'FWholesale&Retail', 'QPublicHealth', 'GTransportation', 'LBusinessSupport',
                        'AAgriculture', 'IIT', 'CManufacturing', 'JFinance'],
             'SP500':   ['InformationTechnology', 'Industrials', 'Materials', 'Financials', 'Utilities',
                        'ConsumerStaples', 'HealthCare', 'Telecommunications', 'ConsumerDiscretionary', 'CommunicationServices',
                        'Energy', 'RealEstate']}

# load sector data
z = pd.read_sql_query("SELECT * FROM sector_z", conn)

for u in list(universes):
    d = z[["date"]+list(z.columns.intersection(universes[u]))]
    d = d.set_index("date")

    # add MA's
    for p in [50, 200]:
        for sector in universes[u]:
            print([sector+" at "+str(p)])
            d[sector + "_MA" + str(p).zfill(3)] = round(talib.MA(d[sector], p), 5)

    # write universe sectors to
    table = "sectors_of_"+u
    d.to_sql(table, conn, if_exists='replace', index=True)

# check out that things are as expected: sql tables are there
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# end
time.sleep(10)
print("done preparing sector_z data")
