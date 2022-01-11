import pandas as pd
import yfinance as yf 
import ut_psql_connect 
import ut_update_queries as q
from dateutil.relativedelta import relativedelta
import datetime
import time
import numpy as np

import yahoo_fin.stock_info as si

##engine
engine=ut_psql_connect.engine
query_local = q.get_ticker_list (4) # default to all  KPI for consistants  select * from r1_update_versions 
#DF_TICKERS
DF_TICKERS = pd.read_sql_query(query_local, con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])
#list_of_chunks = list( q.chunks(LS_TICKERS, 10))


######################################################
###Storage 
######################################################

##diccionaries dowload
Info_dict= {}
Price_dict= {}

##data  manipulations
DATAFRAME_COLLECTION = {}
Y_DICT_COLLECTION = {}   
DF = 'DF_YAHOO_RECOMMENDATIONS_IMP'
DF2 = 'DF_BUY_SELL_FLAG_IMP_TEST'
DF_info= pd.DataFrame()
DF_close = pd.DataFrame()
DF_YAHOO_RECOMMENDATIONS_IMP = pd.DataFrame()
DF_BUY_SELL_FLAG_IMP_TEST= pd.DataFrame()

# The keys you want and name to convert to 
columns_rename={ 'index' : 'ticker'
                ,'shortName' : 'name'
                , 'marketCap' :'market_cap' 
   
                 ,'targetMeanPrice' : 'y_price_1y_est' 
                ,'recommendationMean' : 'y_rank' 
                ,'numberOfAnalystOpinions' : 'y_rank_Analyst_num' 
                ,'profitMargins' : 'profit_Margins'
              
                ,'trailingEps': 'EPS'
                ,'trailingPE': 'PE_RATIO'
                ,'averageVolume': 'AVG_VOLUME'
                ,'sharesOutstanding': 'SS_SHARES_OUTSTANDING'
                ,'trailingAnnualDividendRate': 'DIVIDEND_RATE_PER_SHARE'
                ,'fiveYearAvgDividendYield': 'DIVIDEND_YIELD'
                ,'revenueGrowth': 'Y_REVENUE_GROWTH_EST'
                ,'earningsGrowth': 'Y_EARNINGS_GROWTH_EST'
                ,'country' : 'country'
               ,'sector' :  'sector' 
                ,'industry' : 'industry'
                ,'market' :  'market' 
                ,'recommendationKey':  'recommendationKey'
                ,'growth_est_next_year' : 'growth_est_next_year'
                ,'growth_est_next_5_years' : 'growth_est_next_5_years'
                
               }

Counting= 0


print(str(len(LS_TICKERS)) + ' tickets to Download')
print( 'INFO Dowload started at ' + str(datetime.datetime.now())+'\n' )



#for ticker in LS_TICKERS :
    #list_of_chunk_wk =  (' '.join(str(x) for x in list_ticker )) #Converts list to expected format
    
for ticker in LS_TICKERS:
    try:
         ##info from  yfinance
        tickers = yf.Tickers(ticker)
        Y_DICT_COLLECTION[ticker]= tickers.tickers[ticker].info
        
        ##growth from yahoo_fin
        dc_s= si.get_analysts_info(ticker)
        df_w = dc_s['Growth Estimates']
        df_w=df_w[3:5][['Growth Estimates',ticker]]
        Y_DICT_COLLECTION[ticker]['growth_est_next_year']=df_w[0:1][ticker].values[0]
        Y_DICT_COLLECTION[ticker]['growth_est_next_5_years']=df_w[1:2][ticker].values[0]
        
        print( ticker + "'s yahoo recomendation data done")
    except:
        print(ticker + ' ticker error processing INFO, Proc has  pass')
        pass
Counting+=1
print( 'List No ' + str( Counting) +  ' Download  finished')
        
print( 'INFO Dowload finished at ' + str(datetime.datetime.now())+'\n' )

DF_YAHOO_RECOMMENDATIONS_IMP= pd.DataFrame(Y_DICT_COLLECTION).T.reset_index()
DF_YAHOO_RECOMMENDATIONS_IMP=DF_YAHOO_RECOMMENDATIONS_IMP[list(columns_rename.keys())]
DF_YAHOO_RECOMMENDATIONS_IMP=DF_YAHOO_RECOMMENDATIONS_IMP.rename(columns=columns_rename)
DF_YAHOO_RECOMMENDATIONS_IMP.columns = DF_YAHOO_RECOMMENDATIONS_IMP.columns.str.lower()
DF_YAHOO_RECOMMENDATIONS_IMP['load_date'] = str(datetime.datetime.now())
DATAFRAME_COLLECTION[DF]=DF_YAHOO_RECOMMENDATIONS_IMP


        
        


########################
## DATA Imp to DB Start
########################
print( 'INFO Imp to DB Start at ' + str(datetime.datetime.now())+'\n' )

###Export data to DB
for df in  DATAFRAME_COLLECTION: 
    q.imp_df_to_db(DATAFRAME_COLLECTION, df)



#Connect to the db
con= ut_psql_connect.con
#cursor
cur=con.cursor()
cur.execute("call etl07_Recomendations_h_update();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

print( 'INFO Imp to DB End at ' + str(datetime.datetime.now())+'\n' )
