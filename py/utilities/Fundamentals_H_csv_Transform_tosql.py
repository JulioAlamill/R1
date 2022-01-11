
R1_PATH = 'C:\\Users\\jeaje\\Documents\\JA_docs\\Dev\\Rule1_Investment\\'
PATH = R1_PATH + 'Outputs\\BackUps\\'
df_H_fundamentals_S_3=pd.read_csv(PATH+ 'df_H_fundamentals_S_3.csv', index_col=0) 

##Get funadamentalls tidy up 

df_H_fundamentals_S_3_202012=df_H_fundamentals_S_3.loc[(df_H_fundamentals_S_3['period']==202012)  ]
df_H_fundamentals_S_3_202105=df_H_fundamentals_S_3.loc[(df_H_fundamentals_S_3['period']==202105) ]

df_H_fundamentals_S_3_202012=df_H_fundamentals_S_3_202012[['Breakdown','ticker', '2009','2010','2012','2013','2014','2015'
                                                   ,'2016','2017','2018','2019']]

df_H_fundamentals_S_3_202105= df_H_fundamentals_S_3_202105[['ticker' , 'Breakdown','2020','2021']] 

df_H_fundamentals_S_3_new=pd.merge(  df_H_fundamentals_S_3_202012,df_H_fundamentals_S_3_202105 , on=['ticker' , 'Breakdown'], how="inner") 


df_H_fundamentals_S_3_new.to_csv(R1_PATH + 'Outputs\\' + 'df_H_fundamentals_S_3_new.csv')


##Transform fundamentals 
years = ['2009','2010','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']

df_H_fundamentals_S_3_imp=pd.DataFrame()

for year in years:
    working=df_H_fundamentals_S_3_new[['Breakdown','ticker', year]]
    working=working.rename(columns = {year: "VALUE","Breakdown": "FUNDAMENTAL_ID"})
    working['year'] = year
    df_H_fundamentals_S_3_imp=df_H_fundamentals_S_3_imp.append(working)

df_H_fundamentals_S_3_imp.to_csv(R1_PATH + 'Outputs\\' + 'df_H_fundamentals_S_3_imp.csv')
