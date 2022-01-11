#!/usr/bin/env Python_Home
#!/usr/bin/python
import io #StringIO module an in-memory file-like object
import pandas as pd
import numpy as np
import ut_psql_connect 
import ut_update_queries as q
import datetime
import sys


## Config
## from  daily / "weekly" /  ""monthly""  
proc = sys.argv[1]   ## SEE select * FROM py_proc_seq order by proc_id,seq
 ## **For daily updated this is not relevant as it is defaault to all KPY** .  
 ##--SEE select * from r1_update_versions  for list 
list_num = sys.argv[2] 

##engine
engine=ut_psql_connect.engine

PATH_PY= q.PATH_PY###Path to scripts 
query = q.get_ticker_list ( list_num )##query
proc_r1_to_run =q.get_proc_update(proc)##Get  Files and seq  according to  config 

print(datetime.datetime.now() )


## Run the proces man !
for script in  proc_r1_to_run:
    with open(PATH_PY + script) as f:
        contents = f.read()
    exec(contents)
    print (script + ' ' + 'is Done')
    print(datetime.datetime.now())