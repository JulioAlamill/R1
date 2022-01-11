import ut_psql_connect 
import pandas as pd

#Paths 
PATH_PY = 'C:\\Users\\jeaje\\Documents\\JA_docs\\Dev\\Rule1_Investment\\Proc\\2.0\\code\\py\\'
PATH_DFS_OTHERS = PATH_PY + 'dfs_others\\'
PATH_DFS_TICKERS = PATH_PY + 'dfs_tickers\\'


#HEADER
HEADER = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) Gecko/20100101 Firefox/32.0',} #pylint: disable=line-too-long
HEADER2 = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

 ##Get  Files and seq  according to  config 
def get_proc_update (proc) :
    engine=ut_psql_connect.engine
    proc_r1_run = pd.read_sql_query("select seq, script FROM py_proc_seq where proc = '"  + proc + "';" ,con=engine)
    proc_r1_run=proc_r1_run['script']
    return proc_r1_run
    
def get_ticker_list (num) :
    #global query_wanted
    engine=ut_psql_connect.engine
    query_wanted= pd.read_sql_query("select query from r1_update_versions where id_proc = '"  + str(num) + "';"  ,con=engine)
    query_wanted=query_wanted['query'][0]
    return query_wanted


def imp_df_to_db (dic, file) :
    engine=ut_psql_connect.engine
    df_w=dic[file]
    col_names= df_w.columns.str.lower()
    col_names=col_names.str.replace("[&.'''-]", "" , regex=True)
    col_names=col_names.str.replace(" \\(.*?\\)", '', regex=True )
    col_names=col_names.str.replace(" %", '', regex=True)
    col_names=col_names.str.replace(" ", '_', regex=True)
    col_names=col_names.str.replace("__", '_', regex=True)
    df_w.columns=col_names
    df_w.to_sql(file.lower(),engine , if_exists='replace',index=False )
    print(file + ' import to db succeed')
    

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# get_ticker_list(1)
# engine=ut_psql_connect.engine
# LS_TICKERS = pd.read_sql_query(query_wanted,con=engine)