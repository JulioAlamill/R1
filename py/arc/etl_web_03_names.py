
# ######################################################################################
"""Bring Names for tickers."""
#Bring Names for tickers
# ######################################################################################

import requests
import pandas as pd
from sqlalchemy import create_engine
import datetime
import ut_update_queries as q
import psql_connect 

##URL to scarpt data from
URL_S = 'https://finance.yahoo.com/quote/{}/profile'
#"http://d.yimg.com/autoc.finance.yahoo.com/autoc?query={}&region=1&lang=en"


HEADER =q.HEADER

##dataframe to created 
DATAFRAME_COLLECTION = {}
DF = 'DF_TICKER_NAME'

##engine
engine=psql_connect.engine


##engine
engine=psql_connect.engine

#query
try:
    query
except NameError:
    query = q.get_ticker_list (2) # default to all  KPI  select * from r1_update_versions 
    print('query created')
else:
    print('query already declared')
    
    
##Create Function get_symbol
def get_symbol(ticker):#pylint: disable=inconsistent-return-statements
    """Bring Names for tickers function."""
    url = URL_S.format(ticker)
    request = requests.get(url, headers=HEADER, verify=True ).json()
    results = request['ResultSet']['Result']
    for result in results:
        if result['symbol'] == ticker:
            return [result]


##Request data with function get_symbol
TICKER_NAME = []
for i in LS_TICKERS:
    try:
        Name = get_symbol(i)
        TICKER_NAME += Name
        print('DONE: ' + i)
    except:
        print('No Found: ' + i)
        continue

##some fixes
DF_TICKER_NAME = pd.DataFrame.from_dict(TICKER_NAME)
DF_TICKER_NAME.columns= DF_TICKER_NAME.columns.str.lower()
DF_TICKER_NAME = DF_TICKER_NAME.rename(columns={"symbol":"ticker"})
DF_TICKER_NAME['period'] =Period = datetime.date.today().year*100+ datetime.date.today().month
DF_TICKER_NAME['load_date'] = str(datetime.datetime.now())

DATAFRAME_COLLECTION[DF]=DF_TICKER_NAME

###Export data to data frame 
for df in  DATAFRAME_COLLECTION: 
    q.imp_df_to_db(DATAFRAME_COLLECTION, df)



