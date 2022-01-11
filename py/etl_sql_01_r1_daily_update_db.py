import pandas as pd
import ut_psql_connect 


#Connect to the db
con= ut_psql_connect.con
#cursor
cur=con.cursor()
cur.execute("call run_proc_r1_KPI_versions();")
#commit your connection
con.commit()
#close the currsor 
cur.close()

