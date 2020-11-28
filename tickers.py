# main program for retrieving data, learning and predicting stock market returns
import tiingo as tngo
import os
import env_file
import pandas as pd

# add keys and other local environment variables manually
env_file.load('.env')


print("Tiingo key ok? :", len(os.environ['TIINGO_API_KEY'])>0)

# open tiingo session
config = {}
config['session'] = True # stay in session across api calls
config['api_key'] = os.environ['TIINGO_API_KEY']
client = tngo.TiingoClient(config)

tickers = pd.DataFrame(client.list_tickers())


