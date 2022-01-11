import pandas as pd
import psql_connect 

#engine
engine=psql_connect.engine
#Connect to the db
con= psql_connect.con

#cursor
cur=con.cursor()
cur.execute("Call public.etl_r1_all_proc();")

#commit your connection
con.commit()
#close the currsor 
cur.close()
