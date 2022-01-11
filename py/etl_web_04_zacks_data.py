#!/usr/bin/env Python_Home

import datetime
import requests
from bs4 import BeautifulSoup
import ut_psql_connect 
import io #StringIO module an in-memory file-like object
import pandas as pd
import numpy as np
import ut_update_queries as q



##HEADER
HEADER =q.HEADER

DATAFRAME_COLLECTION = {}
DF = 'DF_ZACKS_INDICATORS'

##engine
engine=ut_psql_connect.engine

#query
try:
    query
except NameError:
    query = q.get_ticker_list (2) # default to all  KPI  select * from r1_update_versions 
    print('query created')
else:
    print('query already declared')
    
#engine
DF_TICKERS = pd.read_sql_query(query,con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])
#LS_TICKERS= ['V']

##SET DATAFRAME TO BE FILL UP
ZACKS_COLUMN_NAMES = ['ticker', 'z_rank', 'z_sector_class', 'z_industry_rank', 'z_exp_growth']
DF_ZACKS_INDICATORS = pd.DataFrame(columns=ZACKS_COLUMN_NAMES)

HEADER = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) Gecko/20100101 Firefox/32.0',} #pylint: disable=line-too-long

##LOOP AND BRING DATA FROM ZACKS
for ticker in LS_TICKERS:
    try:
        url = 'https://www.zacks.com/stock/quote/' + ticker
        req = requests.get(url, headers=HEADER, verify=True )
        soup = BeautifulSoup(req.text, features="lxml" )
        ###Working
        #Rank
        rank_view_class = soup.find(class_="rank_view").get_text()
        rank_view = rank_view_class.split()
        rank = rank_view[0]
        #Sector:
        sector_class = soup.find(class_="sector").get_text()
        #industry_Rank
        industry_Rank = soup.find(class_="status").get_text()
        #Quote_Overview
        Quote_Overview=soup.find_all(class_="abut_bottom" )
        for i in Quote_Overview:
            try:
                Exp_growth = i.find(class_="up float_right").get_text()
                Exp_growth= float(Exp_growth.replace('%',''))/100
            except: #pylint: disable=bare-except
                pass
        indicators = [ticker, rank, sector_class, industry_Rank, Exp_growth]
        indicators_df = pd.DataFrame(np.array(indicators).reshape(1, len(indicators))
                                     , columns=ZACKS_COLUMN_NAMES)
        DF_ZACKS_INDICATORS = pd.concat([indicators_df, DF_ZACKS_INDICATORS], ignore_index=False)
        print( ticker + "'s Zacks data done")
    except: #pylint: disable=bare-except
        print( ticker + "'s Zacks data error")
        pass

#Dataframe fix 
DF_ZACKS_INDICATORS['period'] =Period = datetime.date.today().year*100+ datetime.date.today().month
DF_ZACKS_INDICATORS['load_date'] = str(datetime.datetime.now())
#DF_ZACKS_INDICATORS.to_csv(FILE_OUTPUT)


DATAFRAME_COLLECTION[DF]=DF_ZACKS_INDICATORS

###Export data to data frame 
for df in  DATAFRAME_COLLECTION: 
    q.imp_df_to_db(DATAFRAME_COLLECTION, df)
    
    
##Connect to the db
con= ut_psql_connect.con
#cursor
cur=con.cursor()
cur.execute("call etl07_Recomendations_h_update();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

print('Zacks H updated')

     