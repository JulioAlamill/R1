DROP TABLE IF EXISTS DF_FUNDAMENTALS_S3;
CREATE TABLE DF_FUNDAMENTALS_S3 AS (
	
SELECT 
t1.ticker
,t1.fundamental_id
,t1.fundamental_name
,t1.y2016
,t1.y2017
,t1.y2018
,t1.y2019
,t1.y2020
,t1.y2021
,t1.values_avg
,case when t1.fundamental_id between 13 and 16  and t1.values_avg >.0999 then 1 else 0 end as Rule_1_Flag
	,t2.target_est_1y as target_est_1y
	,t2.quote_price as stiker_price
	,t2.eps as eps
	,t2.pe_ratio as pe_ratio
	,t3.next_5_years

FROM public.df_fundamentals_s2 as t1
	left join  public.df_quote_table as  t2 on t1.ticker=t2.ticker
	left join  public.df_growth_est as  t3 on t1.ticker=t3.ticker and t3.sector = t3.ticker
where t1.ticker ='LOGI'
	);
	
	
alter table df_fundamentals_s3 add RULE_1_TOTAL int default 0 ; 

update df_fundamentals_s3 
set RULE_1_TOTAL =t2.RULE_1_TOTAL

--RULE_1_TOTAL
UPDATE df_fundamentals_s3  AS T1 
SET RULE_1_TOTAL =t2.RULE_1_TOTAL
FROM  (
SELECT 
	TICKER
,	sum( Rule_1_Flag) as RULE_1_TOTAL
FROM PUBLIC.df_fundamentals_s3
	group by TICKER
) AS T2
WHERE ( T1.TICKER=T2.TICKER  );


select *
from public.df_fundamentals_s3

