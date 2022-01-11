
# ######################################################################################
"""IMPORT Other utitlities files to PQSL."""
# ######################################################################################
import pandas as pd
import ut_update_queries as q

#PATH 
PATH_DFS_OTHERS = q.PATH_DFS_OTHERS

#  File to import  here 
FILES = [ 'py_proc_seq']  ##'df_etoro_imp'

DATAFRAME_COLLECTION = {}

for file in FILES:
    file_to_imp = file.lower() + '.csv'
    DATAFRAME_COLLECTION[file] = pd.read_csv(PATH_DFS_OTHERS+ file_to_imp, index_col=0)  ## Import
 
    
for file in FILES:
    q.imp_df_to_db(DATAFRAME_COLLECTION , file)
    
    
  ##20211112 old version  -- delete if all goes well 
# for file in FILES:
    #import ut_psql_connect 
    #engine=ut_psql_connect.engine
    # df_w=DATAFRAME_COLLECTION[file]
    # col_names= df_w.columns.str.lower()
    # col_names=col_names.str.replace("[&.'''-]", "" , regex=True )
    # col_names=col_names.str.replace(" \\(.*?\\)", '', regex=True)
    # col_names=col_names.str.replace(" %", '', regex=True)
    # col_names=col_names.str.replace(" ", '_', regex=True)
    # col_names=col_names.str.replace("__", '_', regex=True)
    # df_w.columns=col_names
    # df_w.to_sql(file.lower(),engine , if_exists='replace',index=False )
    
 