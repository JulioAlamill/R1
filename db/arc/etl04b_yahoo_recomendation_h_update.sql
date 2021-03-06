CREATE OR REPLACE PROCEDURE etl04b_yahoo_recomendation_h_update ()
LANGUAGE SQL
as $$


/* UPDATED ZACKS HISTORY  */

--delet if data exists 

DELETE FROM DF_YAHOO_RECOMMENDATIONS T1
WHERE   (T1.TICKER , LOAD_DATE  )  IN ( 
 SELECT T1A.TICKER , CAST (LOAD_DATE AS DATE ) as  LOAD_DATE  
 FROM DF_YAHOO_RECOMMENDATIONS_IMP T1A);


--INSERT
INSERT INTO DF_YAHOO_RECOMMENDATIONS 
 ( TICKER , RANK , y_rank, SECTOR_CLASS ,	INDUSTRY_RANK  ,EXP_GROWTH, y_rank_analyst_num ,profit_margins , y_price_1y_est , PERIOD  ,LOAD_DATE )
SELECT 
TICKER
,	 case when recommendationkey = 'underperform' then '4-Sell' 
when recommendationkey = 'hold' then '3-Hold' 
when recommendationkey = 'Buy' then '2-Buy' 
when recommendationkey = 'strong_buy' then '1-Strong' 
else  null  as RANK
, y_rank
,	null as SECTOR_CLASS
,	null as  INDUSTRY_RANK
,	null as   EXP_GROWTH
, 	y_rank_analyst_num	
,   profit_margins
,	CAST (y_price_1y_est AS DECIMAL (10,2)) AS y_price_1y_est
,	 EXTRACT(YEAR FROM LOAD_DATE )  *100 + EXTRACT(Month FROM LOAD_DATE) as   PERIOD
,	LOAD_DATE AS LOAD_DATE

FROM DF_YAHOO_RECOMMENDATIONS_IMP;

/*

ALTER TABLE DF_YAHOO_RECOMMENDATIONS  
          ADD YR_VAR_LASTLOAD	int default 0
          ,ADD YR_VAR_LASTLOAD_DAYSDIFF 	int  default 0
		   ,ADD YR_VAR_ACTUAL	int default 0
		  ,ADD YR_VAR_ACTUAL_DAYSDIFF 	int  default 0
		  ;
            
            
  
*/  

--- Variances since last LOAD
UPDATE DF_YAHOO_RECOMMENDATIONS  T1
SET
YR_VAR_LASTLOAD=T2.R_VAR
,YR_VAR_LASTLOAD_DAYSDIFF=T2.R_VAR_LASTLOAD_DAYSDIFF
FROM (

SELECT 
T1.TICKER
,T1.RANK
,T1.LOAD_DATE
,T2.RANK AS LAST_RANK
,T2.LOAD_DATE AS LAST_LOAD_DATE
, (( CAST ( SUBSTRING(T1.RANK FROM '[0-9]+') AS INT ) ) - ( CAST ( SUBSTRING(T2.RANK FROM '[0-9]+') AS INT )) ) *-1 AS R_VAR 

,cast ( T1.LOAD_DATE - T2.LOAD_DATE as int )   AS R_VAR_LASTLOAD_DAYSDIFF 
FROM DF_YAHOO_RECOMMENDATIONS T1 
LEFT JOIN   DF_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER=T2.TICKER

WHERE T2.LOAD_DATE = (SELECT MAX (T2A.LOAD_DATE)
					 FROM DF_YAHOO_RECOMMENDATIONS T2A
					  WHERE T2A.TICKER=T2.TICKER
                      
					  AND T2A.LOAD_DATE<T1.LOAD_DATE)



      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
and  T1.LOAD_DATE =T2.LOAD_DATE ;


--just actual Variances 
UPDATE DF_YAHOO_RECOMMENDATIONS  T1
SET
YR_VAR_ACTUAL=T2.R_VAR
,YR_VAR_ACTUAL_DAYSDIFF=T2.R_VAR_ACTUAL_DAYSDIFF
FROM (

SELECT 
T1.TICKER
,T1.RANK
,T1.LOAD_DATE
,T2.RANK AS LAST_RANK
,T2.LOAD_DATE AS LAST_LOAD_DATE
, (( CAST ( SUBSTRING(T1.RANK FROM '[0-9]+') AS INT ) ) - ( CAST ( SUBSTRING(T2.RANK FROM '[0-9]+') AS INT )) ) *-1 AS R_VAR 

,cast ( T1.LOAD_DATE - T2.LOAD_DATE as int )   AS R_VAR_ACTUAL_DAYSDIFF 
FROM DF_YAHOO_RECOMMENDATIONS T1 
LEFT JOIN   DF_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER=T2.TICKER

WHERE T2.LOAD_DATE = (SELECT MAX (T2A.LOAD_DATE)
					 FROM DF_YAHOO_RECOMMENDATIONS T2A
					  WHERE T2A.TICKER=T2.TICKER
                       and T2A.RANK <>T1.RANK   --here the diff 
					  AND T2A.LOAD_DATE<T1.LOAD_DATE)



      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
and  T1.LOAD_DATE =T2.LOAD_DATE ;



 $$;