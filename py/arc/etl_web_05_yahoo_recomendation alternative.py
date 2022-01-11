import pandas as pd
import yfinance as yf 
import psql_connect 
import r1_update_queries as q
from dateutil.relativedelta import relativedelta
import datetime
import time
import numpy as np

##engine
engine=psql_connect.engine
query_local = q.get_ticker_list (4) # default to all  KPI for consistants  select * from r1_update_versions 
#DF_TICKERS
DF_TICKERS = pd.read_sql_query(query_local, con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])
##LS_TICKERS= ('V', 'LOGI')


######################################################
###Storage 
######################################################

DATAFRAME_COLLECTION = {}
Y_DICT_COLLECTION = {}
DF = 'DF_YAHOO_RECOMMENDATIONS_IMP'
DF_info= pd.DataFrame()
DF_YAHOO_RECOMMENDATIONS_IMP = pd.DataFrame()

    
wanted_keys = [
'shortName'
,'marketCap'
,'country'
,'averageVolume'
,'sector'
,'industry'
,'market'
,'targetMeanPrice'
,'recommendationKey'
,'recommendationMean'
,'numberOfAnalystOpinions'
,'profitMargins'] # The keys you want

columns_rename={ "index" : "ticker"
                ,"shortname" : "name"
                , "marketcap" :"market_cap" 
                ,"averagevolume" : "volume" 
                 ,"targetmeanprice" : "y_price_1y_est" 
                ,"recommendationmean" : "y_rank" 
                ,"numberofanalystopinions" : "y_rank_Analyst_num" 
                ,"profitmargins" : "profit_Margins"
                 }

Counting= 0

print(str(len(LS_TICKERS)) + ' tickets to Download')
print( 'INFO Dowload started at ' + str(datetime.datetime.now())+'\n' )


for ticker in LS_TICKERS :
    try:
        t_info = yf.Ticker(ticker)
        bigdict= t_info.info
        smalldict={}
        for k in wanted_keys:
            value=bigdict[k]
            smalldict[k] = value
        Y_DICT_COLLECTION[ticker]= smalldict
        time.sleep(3)
    except:
        print(ticker + ' ticker error processing INFO, Proc has  pass')
        pass
    Counting+=1    
    
print( 'INFO Dowload finished at ' + str(datetime.datetime.now())+'\n' )


    ##Data frame handleling 
DF_YAHOO_RECOMMENDATIONS_IMP= pd.DataFrame(Y_DICT_COLLECTION).T.reset_index()
DF_YAHOO_RECOMMENDATIONS_IMP.columns = DF_YAHOO_RECOMMENDATIONS_IMP.columns.str.lower()
DF_YAHOO_RECOMMENDATIONS_IMP=DF_YAHOO_RECOMMENDATIONS_IMP.rename(columns=columns_rename)
DF_YAHOO_RECOMMENDATIONS_IMP['load_date'] = str(datetime.datetime.now())
DATAFRAME_COLLECTION[DF]=DF_YAHOO_RECOMMENDATIONS_IMP

print( 'INFO Imp to DB Start at ' + str(datetime.datetime.now())+'\n' )

###Export data to DB
for df in  DATAFRAME_COLLECTION: 
    q.imp_df_to_db(DATAFRAME_COLLECTION, df)



#Connect to the db
con= psql_connect.con
#cursor
cur=con.cursor()
cur.execute("call etl04_Recomendations_h_update();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

print( 'INFO Imp to DB End at ' + str(datetime.datetime.now())+'\n' )