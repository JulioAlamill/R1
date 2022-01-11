
import io #StringIO module an in-memory file-like object
import pandas as pd
import numpy as np
import psql_connect 


##engine
engine=psql_connect.engine

#engine
DF_FUNDAMENTALS_S1 = pd.read_sql_query('''SELECT 
T1.fundamental_id
,T1.r1_bigfive_flag
,T2.fundamental_name
,T1.TICKER
,T1.VALUE
,T1.YEAR
FROM DF_FUNDAMENTALS_S1 T1
LEFT  JOIN   public.DF_FUNDAMENTAL_ID T2  ON  T1.fundamental_id=T2.fundamental_id''',con=engine)

years= DF_FUNDAMENTALS_S1['year'].apply(str).drop_duplicates().sort_values()
DF_FUNDAMENTALS_S=DF_FUNDAMENTALS_S1[['ticker','fundamental_name','fundamental_id', 'r1_bigfive_flag']].drop_duplicates()

for y in  years:
    df_w=DF_FUNDAMENTALS_S1[['fundamental_name', 'value' , 'ticker']].loc[DF_FUNDAMENTALS_S1['year']==int(y)]
    df_w = df_w.rename(columns={"value":y})
    DF_FUNDAMENTALS_S=  DF_FUNDAMENTALS_S.merge( df_w, how="left", on=['ticker','fundamental_name'])
DF_FUNDAMENTALS_S['values_avg'] = DF_FUNDAMENTALS_S[years].mean(axis=1)
DF_FUNDAMENTALS_S= DF_FUNDAMENTALS_S.fillna(0)

DF_FUNDAMENTALS_S['latest_year_values']=  np.where(DF_FUNDAMENTALS_S[years.max()].replace(0, np.nan).isnull()
                                       , DF_FUNDAMENTALS_S[str(int(years.max())-1)]
                                       , DF_FUNDAMENTALS_S[years.max()])
                                       
                                       
##reset Index                                       
DF_FUNDAMENTALS_S = DF_FUNDAMENTALS_S.sort_values(by=['ticker','fundamental_id']).reset_index(drop=True)


## Rule_1_Flag
DF_FUNDAMENTALS_S['rule_1_flag']= np.where((DF_FUNDAMENTALS_S['values_avg'] >= .095) 
                                                & (DF_FUNDAMENTALS_S['r1_bigfive_flag'].isin({1})
                                            ), 1, 0)


####################
##Fixes in fundamental_id when  growth > 10% but values_avg is negative 
#####################

fundamental_ids = np.array([[ 10,13],[12,13],[1,14],[5,15],[9,16],[6,17]])
df_ref = pd.DataFrame({'Value_id': fundamental_ids[:, 0], 'GW_Rate_id': fundamental_ids[:, 1]})


for index, id in df_ref.iterrows():
     
    df1=DF_FUNDAMENTALS_S.loc[DF_FUNDAMENTALS_S['fundamental_id']== id['GW_Rate_id'], ('rule_1_flag')].to_frame()
    df1['values_avg']=DF_FUNDAMENTALS_S.loc[DF_FUNDAMENTALS_S['fundamental_id']==id['Value_id'], ('values_avg')].array
    df1['rule_1_flag_fix']=np.where(df1['values_avg']<0,0,df1['rule_1_flag'])
    DF_FUNDAMENTALS_S.loc[DF_FUNDAMENTALS_S['fundamental_id']==id['GW_Rate_id'], ('rule_1_flag')]=df1['rule_1_flag_fix']
####################

## add BIG5_TOTAL
big5_total= pd.Series (DF_FUNDAMENTALS_S.groupby(['ticker'])['rule_1_flag'].sum(),  name="big5_total").to_frame()
DF_FUNDAMENTALS_S = DF_FUNDAMENTALS_S.merge(big5_total, on='ticker')

DF_FUNDAMENTALS_S.columns=DF_FUNDAMENTALS_S.columns.str.replace('^2', 'y2')## Fix in Column Names
## Save to PSQL
DF_FUNDAMENTALS_S.to_sql('DF_FUNDAMENTALS_S'.lower(),engine , if_exists='replace',index=False )
