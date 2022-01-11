
###################################################################
"""Import  Back FUNDAMENTAL DATA FROM YAHOO to start  calculation .
#################################################
####Import data to PostgraseSQL
#################################################
# df_income_statement
# df_balance
# df_cash_flow_statement
# df_quote_table
# df_earnings_q_results
# df_earnings_q_revenue
# df_earnings_y_revenue
# df_growth_est
# df_revenue_est
# df_earnings_est
# df_earnings_hist
# df_eps_trend
"""
###################################################################
import ut_psql_connect 
import io
import pandas as pd
import ut_update_queries as q


##PATH
PATH_DFS_TICKERS = q.PATH_DFS_TICKERS 


##GET psql engine on - sqlalchemy
engine=ut_psql_connect.engine

#Connect to the db
con= ut_psql_connect.con

#query
try:
    query
except NameError:
    query = q.get_ticker_list (2) # default to relevant KPI
    print('query created')
else:
    print('query already declared')
    
######################################################
##Import TICKERS TO LOOK FOR
######################################################
DF_TICKERS = pd.read_sql_query(query,con=engine)
LS_TICKERS = list(DF_TICKERS['ticker'])
######################################################
#LS_TICKERS = ['V']

##DF TO BE IMPORTED 
YEARLY_DF = ('df_income_statement'      
                ,'df_balance'
                ,'df_cash_flow_statement')
        
SNAPSHOT_DF=   ('df_quote_table'
                ,'df_earnings_q_results'
                ,'df_earnings_q_revenue'
                ,'df_earnings_y_revenue'
                ,'df_growth_est'
                ,'df_revenue_est'
                ,'df_earnings_est'
                ,'df_earnings_hist'
                ,'df_eps_trend'
                ,'df_stats'
                )
      
        
DF_ALL = YEARLY_DF + SNAPSHOT_DF


##RE-SET STAGING TABLES IN PSQL 
#cursor
cur=con.cursor()
for df in  DF_ALL:
    cur.execute("delete from "+df + "_s;" )
    #commit your connection
    con.commit()
#close the currsor 
cur.close()

#CREATED DATAFRAME COLLECTION
DATAFRAME_COLLECTION = {}
#set collection dict 
for df in  DF_ALL:
    ##get expected columns
    column_names  = pd.read_sql_query('select * from ' + df +  ' where ticker is not null;',con=engine)
    column_names  = list(column_names )
    DATAFRAME_COLLECTION[df]=pd.DataFrame( columns = column_names ) 

#################################################
####INTINERATION PER FOLDER/TICKET TO IMPORT DATA
#################################################

for ticker in LS_TICKERS:
   
    ## PATH to get file from
    getting_from = PATH_DFS_TICKERS + ticker + '\\'

    try:
    
        # Import Data frames
        for df in  DF_ALL:
            get_file = getting_from + df + '.csv'
            df_w = pd.read_csv(get_file, index_col=0)
             ##Work on column names
            col_names= df_w.columns.str.lower()
            col_names=col_names.str.replace("[&.'''-]", "")
            col_names=col_names.str.replace(" \\(.*?\\)", '')
            col_names=col_names.str.replace(" %", '')
            col_names=col_names.str.replace(" ", '_')
            col_names=col_names.str.replace("__", '_')
            df_w.columns=col_names
            df_w['ticker']=ticker
            DATAFRAME_COLLECTION[df] = DATAFRAME_COLLECTION[df].append(df_w, sort=True)
            print(df + ' ' + ticker + ' file fase 1  imported')

    except:
        print(df + ' ' + ticker + ' error processing  and passed')
        pass
 

       
for df in  DF_ALL:
       ##Work on column names
    df_w= DATAFRAME_COLLECTION[df] 
    
    db_T=df+ '_s' # Staging Table name 
    #Insert to staging table
    df_w.head(0).to_sql( db_T, engine, if_exists='replace',index=False) 
    
    ##conn cursor UP
    conn = engine.raw_connection()
    cur = conn.cursor()
    ##Saving the file to StringIO format 
    output = io.StringIO()
    df_w.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    ##contents = output.getvalue() No need?
    ##Copy the file to table 
    cur.copy_from(output, db_T, null="") # null values become ''
    conn.commit()
    cur.close()      
    print(df + ' ' + 'BULK' + ' file fase 2  imported')


print( 'Fundamentals DB proc update start at ' + str(datetime.datetime.now())+'\n' )
 
 #Connect to the db
con= ut_psql_connect.con
#cursor
cur=con.cursor()
cur.execute("Call public.run_etl_fundamentals_update();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

print( 'All Fundamentals were updated in DB,  End at ' + str(datetime.datetime.now())+'\n' )