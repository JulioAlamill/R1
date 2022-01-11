
###################################################################
"""BRING FUNDAMENTAL DATA FROM YAHOO TICKER FINANCE API."""
##http://theautomatic.net/yahoo_fin-documentation/#methods
###################################################################

##important

import datetime
import time
import sys
import os
import json
import numpy as np
import pandas as pd
import yahoo_fin.stock_info as si
import ut_psql_connect 
import ut_update_queries as q

######################################################
##option 1 - START UP LIST
######################################################
##Import TICKERS TO LOOK FOR


PATH_DFS_TICKERS = q.PATH_DFS_TICKERS 

##engine
engine=ut_psql_connect.engine


print( 'Fundamentals tickers list update start at ' + str(datetime.datetime.now())+'\n' )
 
 #Connect to the db
con= ut_psql_connect.con
#cursor
cur=con.cursor()
cur.execute("Call public.run_etl_fundamentals_tickers_update();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

print( 'Fundamentals tickers list update Ended at ' + str(datetime.datetime.now())+'\n' )


#query
try:
    query
except NameError:
    query = q.get_ticker_list (1) # default to relevant KPI
    print('query created')
else:
    print('query already declared')
    
#pull data
DF_TICKERS = pd.read_sql_query(query,con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])


######################################################
## step two -Filter all not in list "LOGS"
##- which normally are those were updated alrady actual month see "00 LOGS Table.py"
######################################################
TIME_NOW = datetime.datetime.now()
MONTH_NOW = TIME_NOW.year*100+TIME_NOW.month


######################################################
##START UPDATING DATA
######################################################

##LS_TICKERS=['LOGI' ] 
counting_imps= 0



for ticker in LS_TICKERS:
    
	##Final PATH to save file
    saving_to = PATH_DFS_TICKERS + ticker + '\\'

    ##Creates needed folders, If already exists, Pass
    try:
        os.mkdir(saving_to)
    except (NameError, FileExistsError):
        print(ticker +  ' folder Already Exists')
        pass

   

    #df_income_statement
    try:
        df = si.get_income_statement(ticker)
        df.columns =df.columns.map(lambda t: t.strftime('%Y'))
        df= df.T
        df.reset_index(inplace=True)
        save_file=saving_to+ "df_income_statement.csv"
        df.to_csv( save_file)
        print(ticker + ' ticker df_income_statement file done')
    except:
        print(ticker + ' ticker error processing df_income_statement pass')
        pass



        ###df_balance 
    try:
        df = si.get_balance_sheet(ticker)
        df.columns =df.columns.map(lambda t: t.strftime('%Y'))
        df= df.T
        df.reset_index(inplace=True)
        save_file=saving_to+ "df_balance.csv"
        df.to_csv( save_file)
        print(ticker + ' ticker df_balance file done')
    except:
        print(ticker + ' ticker error processing df_balance pass')
        pass
     
          ###dc_cash_flow_statement Transform
    try:
        df = si.get_cash_flow(ticker)
        df.columns =df.columns.map(lambda t: t.strftime('%Y'))
        df= df.T
        df.reset_index(inplace=True)
        save_file=saving_to+ "df_cash_flow_statement.csv"
        df.to_csv( save_file)
        print(ticker + ' ticker df_cash_flow_statement file done')
    except:
        print(ticker + ' ticker error processing df_cash_flow_statement pass')
        pass

    ###df_quote_table 
    try:
        dc = {}
        dc[ticker] =si.get_quote_table(ticker,  dict_result=True)
        df = pd.DataFrame(dc).T
        df.reset_index(inplace=True)
        df.replace("∞", 0)
        save_file=saving_to+ "df_quote_table.csv"
        df.to_csv( save_file)
        print(ticker + ' ticker df_quote_table file done')
    except:
        print(ticker + ' ticker error processing df_quote_table pass')
        pass  

    ##df_stats
    try:
        df = si.get_stats(ticker)
        df = df.set_index('Attribute')
        df= df.T
        save_file=saving_to+ "df_stats.csv"

        df.columns=[
                        'sph_beta_5y_monthly'
            ,'sph_52_week_change'
            ,'sph_sp500_52_week_change'
            ,'sph_52_week_high'
            ,'sph_52_week_low'
            ,'sph_50_day_moving_average'
            ,'sph_200_day_moving_average'
            ,'ss_avg_vol_3month'
            ,'ss_avg_vol_10day'
            ,'ss_shares_outstanding'
            ,'ss_implied_shares_outstanding'
            ,'ss_shares_float'
            ,'ss_perc_held_by_insiders'
            ,'ss_perc_held_by_institutions'
            ,'ss_shares_short_prev_month'
            ,'ss_short_ratio_prev_month'
            ,'ss_short_perc_float_prev_month'
            ,'ss_short_perc_shares_outst_prev_month'
            ,'ss_shares_short_prior_prev_month'
            ,'ds_forward_annual_div_rate'
            ,'ds_forward_annual_div_yield'
            ,'ds_trailing_annual_div_rate'
            ,'ds_trailing_annual_div_yield'
            ,'ds_5_year_average_div_yield'
            ,'ds_payout_ratio'
            ,'ds_div_date'
            ,'ds_ex_div_date'
            ,'ds_last_split_factor'
            ,'ds_last_split_date'
            ,'fiscal_year_ends'
            ,'most_recent_quarter'
            ,'p_profit_margin'
            ,'p_operating_margin_ttm'
            ,'me_return_on_assets_ttm'
            ,'me_return_on_equity_ttm'
            ,'is_revenue_ttm'
            ,'is_revenue_per_share_ttm'
            ,'is_quarterly_revenue_growth_yoy'
            ,'is_gross_profit_ttm'
            ,'is_ebitda'
            ,'is_net_income_avi_to_common_ttm'
            ,'is_diluted_eps_ttm'
            ,'is_quarterly_earnings_growth_yoy'
            ,'bs_total_cash_mrq'
            ,'bs_total_cash_per_share_mrq'
            ,'bs_total_debt_mrq'
            ,'bs_total_debtequity_mrq'
            ,'bs_current_ratio_mrq'
            ,'bs_book_value_per_share_mrq'
            ,'cfs_operating_cash_flow_ttm'
            ,'cfs_levered_free_cash_flow_ttm'


                    ]
        
        df.to_csv( save_file)
        print(ticker + ' ticker df_stats file done')
    except:
        print(ticker + ' ticker error processing df_stats pass')
    pass

 
    ###df_Earnings Transform
    try:
        dc_s= si.get_earnings(ticker)
        df1 = dc_s['quarterly_results']
        df2 = dc_s['quarterly_revenue_earnings']
        df3 = dc_s['yearly_revenue_earnings']
        
        save_file1=saving_to+ "df_earnings_q_results.csv"
        save_file2=saving_to+ "df_earnings_q_revenue.csv"
        save_file3=saving_to+ "df_earnings_y_revenue.csv"
        
        df1.to_csv( save_file1)
        df2.to_csv( save_file2)
        df3.to_csv( save_file3)
        
        print(ticker + ' ticker df_Earnings file done')
    except:
        print(ticker + ' ticker error processing df_Earnings pass')
        pass


    ###dc_analysis_info Transform
    try:
    
        dc_s= si.get_analysts_info(ticker)
        df1 = dc_s['Growth Estimates']
        df2 = dc_s['Revenue Estimate']
        df3 = dc_s['Earnings Estimate']
        df4 = dc_s['Earnings History']
        df5 = dc_s['EPS Trend']
            
        df1= df1.T
        df1.columns = df1.iloc[0]
        df1= df1.drop(df1.index[0])
        df1.reset_index(inplace=True)

        
        df2= df2.T
        df2.columns = df2.iloc[0]
        df2= df2.drop(df2.index[0])
        df2.reset_index(inplace=True)

        
        df3= df3.T
        df3.columns = df3.iloc[0]
        df3= df3.drop(df3.index[0])
        df3.reset_index(inplace=True)


        df4= df4.T
        df4.columns = df4.iloc[0]
        df4= df4.drop(df4.index[0])
        df4.reset_index(inplace=True)


        save_file1=saving_to+ "df_growth_est.csv"
        save_file2=saving_to+ "df_revenue_est.csv"
        save_file3=saving_to+ "df_earnings_est.csv"
        save_file4=saving_to+ "df_earnings_hist.csv"
        save_file5=saving_to+ "df_eps_trend.csv"
        
        df1.to_csv(save_file1)
        df2.to_csv(save_file2)
        df3.to_csv(save_file3)
        df4.to_csv(save_file4)
        df5.to_csv(save_file5)
        
        print(ticker +  ' dc_analysis_info done')
    except:
        print(ticker + ' ticker error processing dc_analysis_info pass')
        pass
    
    counting_imps+=1
    FILE1 = open( PATH_DFS_TICKERS +  'logs_etl06.txt', 'w')
    Log_end = ticker+' Logged at ' + str(datetime.datetime.now())+'\n'
    FILE1.write(Log_end)
    time.sleep(3)
    print(Log_end + ', ' + 'Counting: ' + str (counting_imps))
    
