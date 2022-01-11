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
query_local = q.get_ticker_list (2) # default to all  KPI for consistants  select * from r1_update_versions 
#DF_TICKERS
DF_TICKERS = pd.read_sql_query(query_local, con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])
list_of_chunks = list( q.chunks(LS_TICKERS, 100))


Counting= 0
######################################################
###Storage 
######################################################
DATAFRAME_COLLECTION = {}
DF = 'DF_BUY_SELL_FLAG_IMP'
DF_close= pd.DataFrame()
DF_BUY_SELL_FLAG_IMP = pd.DataFrame()

######################################################
##time
######################################################
Today = datetime.datetime.now() 
Y2_ago = Today- relativedelta(months=12)

Counting= 0

print(str(len(list_of_chunks)) + ' lists of 100 tickets to Download')
print( 'BS Lists Dowload started at ' + str(datetime.datetime.now())+'\n' )
for l in list_of_chunks :

    try:
        data = yf.download(l,Y2_ago,progress=False)['Adj Close']
        stacked = data.stack().reset_index()
        stacked =stacked.rename(columns={"level_1":"ticker", 0:"close_adj_price" })
        DF_close= DF_close.append(stacked)
        Counting+=1
        print( str(Counting) + ' of ' + str(len(list_of_chunks))  + ' Lists Dowloaded at ' + str(datetime.datetime.now())+'\n' )
        time.sleep(3)
    except:
        print(ticker + ' ticker error processing BS Flags, Proc has  pass')
        pass

print( 'BS Lists Dowload finished at ' + str(datetime.datetime.now())+'\n' )
print( 'BS Calcs Start at ' + str(datetime.datetime.now())+'\n' )

## START CALCULATION  INDICATORS 
for ticker in LS_TICKERS:
    try:

        DF_W=DF_close.loc[DF_close['ticker']==ticker].reset_index(drop=True)

        #MA_10
        DF_W['MA_10']=DF_W['close_adj_price'].rolling(10,).mean()
        DF_W['MA_10_Trend']=np.where( DF_W['MA_10'].diff()>0 ,"Trend UP","Trend Down")
        DF_W['MA_10_Trend_Change']= np.where( DF_W['MA_10_Trend']==DF_W['MA_10_Trend'].shift(periods=1), "N","Y")

        DF_W['MA_10_BS_Flag']= np.where(((DF_W['MA_10_Trend_Change'] =="Y" )
                                                 & (DF_W['MA_10'] > DF_W['close_adj_price']))
                                                 ,-1 
                                                 ,( np.where( ( (DF_W['MA_10_Trend_Change'] =="Y" )
                                                  & (DF_W['MA_10'] < DF_W['close_adj_price'] ))
                                                 ,1 
                                                 , None)))  
        DF_W['MA_10_BS_Flag'] = DF_W['MA_10_BS_Flag'].fillna(method='ffill')
        #some fixes in between cases like LOGI  21/05/2021
        DF_W['MA_10_BS_Flag'] = np.where(((DF_W['MA_10_Trend'] == "Trend Down") 
                                                 & (DF_W['MA_10_BS_Flag']== 1))
                                                 ,0
                                                 , np.where(((DF_W['MA_10_Trend'] == "Trend UP") 
                                                 & (DF_W['MA_10_BS_Flag']== -1))
                                                 ,0
                                                 ,DF_W['MA_10_BS_Flag']))


        #Moving Average Convergence Divergence (MACD)
        #https://www.learnpythonwithrune.org/pandas-calculate-the-moving-average-convergence-divergence-macd-for-a-stock/
        DF_W['EMA8'] = DF_W['close_adj_price'].ewm(span=8, adjust=False).mean()
        DF_W['EMA18'] = DF_W['close_adj_price'].ewm(span=18, adjust=False).mean()
        DF_W['MACD'] = DF_W['EMA8']  - DF_W['EMA18']
        DF_W['Signal9'] = DF_W['MACD'].rolling(9,).mean()
        DF_W['MACD_Signal9_diff'] = DF_W['MACD']  - DF_W['Signal9']
        DF_W['MACD_Trend']= np.where((DF_W['MACD_Signal9_diff'] >0) 
                                              & (DF_W['MACD_Signal9_diff'].shift(periods=1)<0)
                                             ,"Trend UP"
                                             ,np.where(((DF_W['MACD_Signal9_diff'] <0) 
                                              & (DF_W['MACD_Signal9_diff'].shift(periods=1)>0))
                                             ,"Trend Down" 
                                             , None ))
        DF_W['MACD_Trend'] = DF_W['MACD_Trend'].fillna(method='ffill')
        DF_W['MACD_Trend_Change']= np.where( DF_W['MACD_Trend']
                                                     ==
                                                     DF_W['MACD_Trend'].shift(periods=1)
                                                     , "N","Y")
        DF_W['MACD_BS_Flag'] = np.where((DF_W['MACD_Trend_Change'] =='Y' )
                                                 & (DF_W['MACD_Trend'] == "Trend UP")
                                                 ,1
                                                 , (np.where( (DF_W['MACD_Trend_Change'] =='Y' )
                                                  &  (DF_W['MACD_Trend'] == "Trend Down" )
                                                 ,-1 
                                                 , None)) )      

        DF_W['MACD_BS_Flag'] = DF_W['MACD_BS_Flag'].fillna(method='ffill') 

        #Stochastic Oscillator Indicator- SMI
        DF_W['Min14'] = DF_W['close_adj_price'].rolling(14,).min()
        DF_W['Max14'] = DF_W['close_adj_price'].rolling(14,).max()
        DF_W['AVG_MIN_MAX'] = (DF_W['Min14'] + DF_W['Max14']) / 2
        DF_W['AVG_MIN_MAX_vs_close'] = DF_W['close_adj_price']  - DF_W['AVG_MIN_MAX']
        DF_W['AVG_MIN_MAX_vs_close_EMA5'] = DF_W['AVG_MIN_MAX_vs_close'].ewm(span=5, adjust=False).mean()
        DF_W['AVG_MIN_MAX_vs_close_EMA5_2'] = DF_W['AVG_MIN_MAX_vs_close_EMA5'].ewm(span=5, adjust=False).mean()
        DF_W['DIFF_MIN_MAX'] = DF_W['Max14'] - DF_W['Min14']
        DF_W['DIFF_MIN_MAX_EMA5'] = DF_W['DIFF_MIN_MAX'].ewm(span=5, adjust=False).mean()
        DF_W['DIFF_MIN_MAX_EMA5_2'] = DF_W['DIFF_MIN_MAX_EMA5'].ewm(span=5, adjust=False).mean()
        DF_W['SMI'] = 100*(DF_W['AVG_MIN_MAX_vs_close_EMA5_2']  / DF_W['DIFF_MIN_MAX_EMA5_2'])
        DF_W['Signal_line'] = DF_W['SMI'].ewm(span=5, adjust=False).mean()
        DF_W['SMI_Signal_line_diff'] = DF_W['SMI']  - DF_W['Signal_line']

        DF_W['SMI_Trend']= np.where((DF_W['SMI_Signal_line_diff'] >0) 
                                          & (DF_W['SMI_Signal_line_diff'].shift(periods=1)<0)
                                         ,"Trend UP"
                                         ,np.where(((DF_W['SMI_Signal_line_diff'] <0) 
                                          & (DF_W['SMI_Signal_line_diff'].shift(periods=1)>0))
                                         ,"Trend Down" 
                                         , None ))
        DF_W['SMI_Trend'] = DF_W['SMI_Trend'].fillna(method='ffill')
        DF_W['SMI_Trend_Change']= np.where( DF_W['SMI_Trend']
                                                 ==
                                                 DF_W['SMI_Trend'].shift(periods=1)
                                                 ,"N","Y")
        DF_W['SMI_BS_Flag'] = np.where((DF_W['SMI_Trend_Change'] =='Y' )
                                             & (DF_W['SMI_Trend'] == "Trend UP")
                                             ,1
                                             , (np.where( (DF_W['SMI_Trend_Change'] =='Y' )
                                              &  (DF_W['SMI_Trend'] == "Trend Down" )
                                             ,-1
                                             , None )) )      

        DF_W['SMI_BS_Flag'] = DF_W['SMI_BS_Flag'].fillna(method='ffill') 
        ##ALL Flags
        DF_W['ALL_BS_SUM'] =DF_W['MA_10_BS_Flag'] + DF_W['MACD_BS_Flag'] + DF_W['SMI_BS_Flag']
        DF_W['ALL_BS_Flag'] =np.where((DF_W['ALL_BS_SUM'] ==3) 
                                               & ((DF_W['MA_10_Trend_Change']=='Y')
                                                  | (DF_W['MACD_Trend_Change']=='Y')
                                                  | (DF_W['SMI_Trend_Change']=='Y'))
                                               ,'BUY'
                                               , np.where((DF_W['ALL_BS_SUM'] ==-3) 
                                               & ((DF_W['MA_10_Trend_Change']=='Y')
                                                  | (DF_W['MACD_Trend_Change']=='Y')
                                                  | (DF_W['SMI_Trend_Change']=='Y'))
                                               ,'SELL'
                                               , None))

        DF_W['ALL_BS_Flag_Seen_Prices'] = np.where( DF_W['ALL_BS_Flag'].notnull()
                                                            , DF_W['close_adj_price']
                                                            ,None)

        # DF_W['ALL_BS_Flag_Seen_Prices'] = DF_W['ALL_BS_Flag_Seen_Prices'].fillna(method='ffill') 

         # # # # # # # # # # # # # # # # # # # # # # #
        #TESTING
        #Get the last before  ALL_BS_Flag_1minus
        DF_W['ALL_BS_Flag_1minus'] =DF_W['ALL_BS_Flag'].fillna(method='ffill').shift(periods=1)

        #Get the last before  ALL_BS_Flag   seen price 
        DF_W['ALL_BS_Flag_Seen_Prices_1minus']= np.where( ((DF_W['ALL_BS_Flag']
                                                             !=DF_W['ALL_BS_Flag_1minus'])
                                                             & 
                                                             DF_W['ALL_BS_Flag'].notnull()) 
                                                           , DF_W['ALL_BS_Flag_Seen_Prices'] 
                                                           , None)

        #If many buy/sell flags in between get seen price of first event
        DF_W['ALL_BS_Flag_Seen_Prices_1minus_Actual'] =np.where( ((DF_W['ALL_BS_Flag_Seen_Prices_1minus'].notnull())
                                                            ) 
                                                           ,DF_W['ALL_BS_Flag_Seen_Prices_1minus'].fillna(method='ffill').shift(periods=1)
                                                           , None)
    
        DF_BUY_SELL_FLAG_IMP= DF_BUY_SELL_FLAG_IMP.append(DF_W)
    except:
        print(ticker + ' ticker error processing BS Flags, Proc has  pass')
        pass

print( 'BS Calcs End at ' + str(datetime.datetime.now())+'\n' )

DATAFRAME_COLLECTION[DF]=DF_BUY_SELL_FLAG_IMP

print( 'BS Imp to DB Start at ' + str(datetime.datetime.now())+'\n' )

###Export data to DB
for df in  DATAFRAME_COLLECTION: 
    q.imp_df_to_db(DATAFRAME_COLLECTION, df)


#Connect to the db
con= psql_connect.con
#cursor
cur=con.cursor()
cur.execute("call etl06_df_buy_sell_flag();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

print( 'BS Imp to DB End at ' + str(datetime.datetime.now())+'\n' )