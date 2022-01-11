import pandas as pd
import yahoo_fin.stock_info as si #BEFORE SUIBG OTHER  import yfinance as yf https://towardsdatascience.com/historical-stock-price-data-in-python-a0b6dc826836
from dateutil.relativedelta import relativedelta
import datetime
import numpy as np
import r1_update_queries as q
import psql_connect 
import time

##########################################################################
###+++Bring Tickets to be updated
##########################################################################

##engine
engine=psql_connect.engine
#query
query_local = q.get_ticker_list (2) # default to all  KPI for consistants 
#DF_TICKERS
DF_TICKERS = pd.read_sql_query(query_local, con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])

DATAFRAME_COLLECTION = {}
DF = 'DF_BUY_SELL_FLAGS'
######################################################
##time
######################################################
Today = datetime.datetime.now() 
Y2_ago = Today- relativedelta(months=12)

Counting= 0 
######################################################
##Functions to be used
######################################################


DF_BUY_SELL_FLAGS = pd.DataFrame()

for ticker in LS_TICKERS:
    try:
        DF_Price_load=si. get_data(ticker , start_date = Y2_ago , end_date = Today )
        
        DF_Price_load['ticker']=ticker
        #MA_10
        DF_Price_load['MA_10']=DF_Price_load['close'].rolling(10,).mean()
        DF_Price_load['MA_10_Trend']=np.where( DF_Price_load['MA_10'].diff()>0 ,"Trend UP","Trend Down")
        DF_Price_load['MA_10_Trend_Change']= np.where( DF_Price_load['MA_10_Trend']==DF_Price_load['MA_10_Trend'].shift(periods=1), "N","Y")
        DF_Price_load['MA_10_BS_Flag']= np.where(((DF_Price_load['MA_10_Trend_Change'] =="Y" )
                                                 & (DF_Price_load['MA_10'] > DF_Price_load['close']))
                                                 ,-1 
                                                 ,( np.where( ( (DF_Price_load['MA_10_Trend_Change'] =="Y" )
                                                  & (DF_Price_load['MA_10'] < DF_Price_load['close'] ))
                                                 ,1 
                                                 , None)))  
        DF_Price_load['MA_10_BS_Flag'] = DF_Price_load['MA_10_BS_Flag'].fillna(method='ffill')
        #some fixes in between cases like LOGI  21/05/2021
        DF_Price_load['MA_10_BS_Flag'] = np.where(((DF_Price_load['MA_10_Trend'] == "Trend Down") 
                                                 & (DF_Price_load['MA_10_BS_Flag']== 1))
                                                 ,0
                                                 , np.where(((DF_Price_load['MA_10_Trend'] == "Trend UP") 
                                                 & (DF_Price_load['MA_10_BS_Flag']== -1))
                                                 ,0
                                                 ,DF_Price_load['MA_10_BS_Flag']))
                                                 
        
        #Moving Average Convergence Divergence (MACD)
        #https://www.learnpythonwithrune.org/pandas-calculate-the-moving-average-convergence-divergence-macd-for-a-stock/
        DF_Price_load['EMA8'] = DF_Price_load['close'].ewm(span=8, adjust=False).mean()
        DF_Price_load['EMA18'] = DF_Price_load['close'].ewm(span=18, adjust=False).mean()
        DF_Price_load['MACD'] = DF_Price_load['EMA8']  - DF_Price_load['EMA18']
        DF_Price_load['Signal9'] = DF_Price_load['MACD'].rolling(9,).mean()
        DF_Price_load['MACD_Signal9_diff'] = DF_Price_load['MACD']  - DF_Price_load['Signal9']
        DF_Price_load['MACD_Trend']= np.where((DF_Price_load['MACD_Signal9_diff'] >0) 
                                              & (DF_Price_load['MACD_Signal9_diff'].shift(periods=1)<0)
                                             ,"Trend UP"
                                             ,np.where(((DF_Price_load['MACD_Signal9_diff'] <0) 
                                              & (DF_Price_load['MACD_Signal9_diff'].shift(periods=1)>0))
                                             ,"Trend Down" 
                                             , None ))
        DF_Price_load['MACD_Trend'] = DF_Price_load['MACD_Trend'].fillna(method='ffill')
        DF_Price_load['MACD_Trend_Change']= np.where( DF_Price_load['MACD_Trend']
                                                     ==
                                                     DF_Price_load['MACD_Trend'].shift(periods=1)
                                                     , "N","Y")
        DF_Price_load['MACD_BS_Flag'] = np.where((DF_Price_load['MACD_Trend_Change'] =='Y' )
                                                 & (DF_Price_load['MACD_Trend'] == "Trend UP")
                                                 ,1
                                                 , (np.where( (DF_Price_load['MACD_Trend_Change'] =='Y' )
                                                  &  (DF_Price_load['MACD_Trend'] == "Trend Down" )
                                                 ,-1 
                                                 , None)) )      

        DF_Price_load['MACD_BS_Flag'] = DF_Price_load['MACD_BS_Flag'].fillna(method='ffill') 
        
        #Stochastic Oscillator Indicator- SMI
        DF_Price_load['Min14'] = DF_Price_load['close'].rolling(14,).min()
        DF_Price_load['Max14'] = DF_Price_load['close'].rolling(14,).max()
        DF_Price_load['AVG_MIN_MAX'] = (DF_Price_load['Min14'] + DF_Price_load['Max14']) / 2
        DF_Price_load['AVG_MIN_MAX_vs_close'] = DF_Price_load['close']  - DF_Price_load['AVG_MIN_MAX']
        DF_Price_load['AVG_MIN_MAX_vs_close_EMA5'] = DF_Price_load['AVG_MIN_MAX_vs_close'].ewm(span=5, adjust=False).mean()
        DF_Price_load['AVG_MIN_MAX_vs_close_EMA5_2'] = DF_Price_load['AVG_MIN_MAX_vs_close_EMA5'].ewm(span=5, adjust=False).mean()
        DF_Price_load['DIFF_MIN_MAX'] = DF_Price_load['Max14'] - DF_Price_load['Min14']
        DF_Price_load['DIFF_MIN_MAX_EMA5'] = DF_Price_load['DIFF_MIN_MAX'].ewm(span=5, adjust=False).mean()
        DF_Price_load['DIFF_MIN_MAX_EMA5_2'] = DF_Price_load['DIFF_MIN_MAX_EMA5'].ewm(span=5, adjust=False).mean()
        DF_Price_load['SMI'] = 100*(DF_Price_load['AVG_MIN_MAX_vs_close_EMA5_2']  / DF_Price_load['DIFF_MIN_MAX_EMA5_2'])
        DF_Price_load['Signal_line'] = DF_Price_load['SMI'].ewm(span=5, adjust=False).mean()
        DF_Price_load['SMI_Signal_line_diff'] = DF_Price_load['SMI']  - DF_Price_load['Signal_line']
        
        DF_Price_load['SMI_Trend']= np.where((DF_Price_load['SMI_Signal_line_diff'] >0) 
                                          & (DF_Price_load['SMI_Signal_line_diff'].shift(periods=1)<0)
                                         ,"Trend UP"
                                         ,np.where(((DF_Price_load['SMI_Signal_line_diff'] <0) 
                                          & (DF_Price_load['SMI_Signal_line_diff'].shift(periods=1)>0))
                                         ,"Trend Down" 
                                         , None ))
        DF_Price_load['SMI_Trend'] = DF_Price_load['SMI_Trend'].fillna(method='ffill')
        DF_Price_load['SMI_Trend_Change']= np.where( DF_Price_load['SMI_Trend']
                                                 ==
                                                 DF_Price_load['SMI_Trend'].shift(periods=1)
                                                 ,"N","Y")
        DF_Price_load['SMI_BS_Flag'] = np.where((DF_Price_load['SMI_Trend_Change'] =='Y' )
                                             & (DF_Price_load['SMI_Trend'] == "Trend UP")
                                             ,1
                                             , (np.where( (DF_Price_load['SMI_Trend_Change'] =='Y' )
                                              &  (DF_Price_load['SMI_Trend'] == "Trend Down" )
                                             ,-1
                                             , None )) )      

        DF_Price_load['SMI_BS_Flag'] = DF_Price_load['SMI_BS_Flag'].fillna(method='ffill') 
        ##ALL Flags
        DF_Price_load['ALL_BS_SUM'] =DF_Price_load['MA_10_BS_Flag'] + DF_Price_load['MACD_BS_Flag'] + DF_Price_load['SMI_BS_Flag']
        DF_Price_load['ALL_BS_Flag'] =np.where((DF_Price_load['ALL_BS_SUM'] ==3) 
                                               & ((DF_Price_load['MA_10_Trend_Change']=='Y')
                                                  | (DF_Price_load['MACD_Trend_Change']=='Y')
                                                  | (DF_Price_load['SMI_Trend_Change']=='Y'))
                                               ,'BUY'
                                               , np.where((DF_Price_load['ALL_BS_SUM'] ==-3) 
                                               & ((DF_Price_load['MA_10_Trend_Change']=='Y')
                                                  | (DF_Price_load['MACD_Trend_Change']=='Y')
                                                  | (DF_Price_load['SMI_Trend_Change']=='Y'))
                                               ,'SELL'
                                               , None))
        
        DF_Price_load['ALL_BS_Flag_Seen_Prices'] = np.where( DF_Price_load['ALL_BS_Flag'].notnull()
                                                            , DF_Price_load['close']
                                                            ,None)
        
       # DF_Price_load['ALL_BS_Flag_Seen_Prices'] = DF_Price_load['ALL_BS_Flag_Seen_Prices'].fillna(method='ffill') 
       
         # # # # # # # # # # # # # # # # # # # # # # #
        #TESTING
        #Get the last before  ALL_BS_Flag_1minus
        DF_Price_load['ALL_BS_Flag_1minus'] =DF_Price_load['ALL_BS_Flag'].fillna(method='ffill').shift(periods=1)
        
        #Get the last before  ALL_BS_Flag   seen price 
        DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus']= np.where( ((DF_Price_load['ALL_BS_Flag']
                                                             !=DF_Price_load['ALL_BS_Flag_1minus'])
                                                             & 
                                                             DF_Price_load['ALL_BS_Flag'].notnull()) 
                                                           , DF_Price_load['ALL_BS_Flag_Seen_Prices'] 
                                                           , None)
        
        #If many buy/sell flags in between get seen price of first event
        DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus_Actual'] =np.where( ((DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus'].notnull())
                                                            ) 
                                                           ,DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus'].fillna(method='ffill').shift(periods=1)
                                                           , None)

        
        #Calc revene on selling from   ALL_BS_Flag_Seen_Prices_1minus_Actual  "BUY" flag 
        DF_Price_load['Revenue']= DF_Price_load['ALL_BS_Flag_Seen_Prices']  -DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus_Actual']
        DF_Price_load['Revenue']= np.where((DF_Price_load['ALL_BS_Flag']=='SELL') 
                                           |(DF_Price_load['Revenue'].isnull())
                                           ,(DF_Price_load['ALL_BS_Flag_Seen_Prices']  
                                           - DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus_Actual'])
                                           , np.where((DF_Price_load['ALL_BS_Flag']=='BUY') 
                                           |(DF_Price_load['Revenue'].isnull())
                                           ,( DF_Price_load['ALL_BS_Flag_Seen_Prices_1minus_Actual']- DF_Price_load['ALL_BS_Flag_Seen_Prices'])
                                           , 0 ) )
        DF_BUY_SELL_FLAGS= DF_BUY_SELL_FLAGS.append(DF_Price_load)
        #append to main DF
        Counting+=1
        print(ticker + ' BS flag done Load ' + str(len(DF_Price_load)) + ' Records Counting goes in ' + Counting )
        
        time.sleep(3)
    except:
        print(ticker + ' ticker error processing BS Flags, Proc has  pass')
        pass


##Fixes in DF
DF_BUY_SELL_FLAGS.reset_index(inplace=True)
DF_BUY_SELL_FLAGS=DF_BUY_SELL_FLAGS.rename(columns={"index": "date"})

DATAFRAME_COLLECTION[DF]=DF_BUY_SELL_FLAGS

###Export data to DB
for df in  DATAFRAME_COLLECTION: 
    q.imp_df_to_db(DATAFRAME_COLLECTION, df)
     
    
# DF_BUY_SELL_FLAGS.reset_index(inplace=True)
# DF_BUY_SELL_FLAGS=DF_BUY_SELL_FLAGS.rename(columns={"index": "date"})
# DF_BUY_SELL_FLAGS.to_csv(R1_PATH + 'Code\\db\\Input\\DF_BUY_SELL_FLAGS.csv')

#Connect to the db
con= psql_connect.con
#cursor
cur=con.cursor()
cur.execute("call etl06_df_buy_sell_flag();")
#commit your connection
con.commit()
#close the currsor 
cur.close()
