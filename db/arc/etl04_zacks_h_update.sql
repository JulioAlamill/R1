CREATE OR REPLACE PROCEDURE etl04_zacks_h_update ()
LANGUAGE SQL
as $$


/* UPDATED ZACKS HISTORY  */

--delet if data exists 

DELETE FROM DF_H_ZACKS_INDICATORS T1
WHERE   (T1.TICKER , LOAD_DATE  )  IN (  SELECT T1A.TICKER , CAST (LOAD_DATE AS DATE ) as  LOAD_DATE  FROM DF_ZACKS_INDICATORS T1A);


--INSERT
INSERT INTO DF_H_ZACKS_INDICATORS 
 ( TICKER , RANK ,SECTOR_CLASS ,	INDUSTRY_RANK  ,EXP_GROWTH , PERIOD  ,LOAD_DATE )
SELECT 
	TICKER
,	RANK
,	SECTOR_CLASS
,	INDUSTRY_RANK
,	CAST (EXP_GROWTH AS DECIMAL (10,2)) AS EXP_GROWTH
,	PERIOD
,	CAST (LOAD_DATE AS DATE ) AS LOAD_DATE

FROM DF_ZACKS_INDICATORS;

/*

ALTER TABLE DF_H_ZACKS_INDICATORS  
          ADD ZR_VAR_LASTLOAD	int default 0
          ,ADD ZR_VAR_LASTLOAD_DAYSDIFF 	int  default 0
		   ,ADD ZR_VAR_ACTUAL	int default 0
		  ,ADD ZR_VAR_ACTUAL_DAYSDIFF 	int  default 0
		  ;
            
  
*/  

--- Variances since last LOAD
UPDATE DF_H_ZACKS_INDICATORS  T1
SET
ZR_VAR_LASTLOAD=T2.R_VAR
,ZR_VAR_LASTLOAD_DAYSDIFF=T2.R_VAR_LASTLOAD_DAYSDIFF
FROM (

SELECT 
T1.TICKER
,T1.RANK
,T1.LOAD_DATE
,T2.RANK AS LAST_RANK
,T2.LOAD_DATE AS LAST_LOAD_DATE
, (( CAST ( SUBSTRING(T1.RANK FROM '[0-9]+') AS INT ) ) - ( CAST ( SUBSTRING(T2.RANK FROM '[0-9]+') AS INT )) ) *-1 AS R_VAR 

,cast ( T1.LOAD_DATE - T2.LOAD_DATE as int )   AS R_VAR_LASTLOAD_DAYSDIFF 
FROM DF_H_ZACKS_INDICATORS T1 
LEFT JOIN   DF_H_ZACKS_INDICATORS T2 ON T1.TICKER=T2.TICKER

WHERE T2.LOAD_DATE = (SELECT MAX (T2A.LOAD_DATE)
					 FROM DF_H_ZACKS_INDICATORS T2A
					  WHERE T2A.TICKER=T2.TICKER
                      
					  AND T2A.LOAD_DATE<T1.LOAD_DATE)



      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
and  T1.LOAD_DATE =T2.LOAD_DATE ;


--just actual Variances 
UPDATE DF_H_ZACKS_INDICATORS  T1
SET
ZR_VAR_ACTUAL=T2.R_VAR
,ZR_VAR_ACTUAL_DAYSDIFF=T2.R_VAR_ACTUAL_DAYSDIFF
FROM (

SELECT 
T1.TICKER
,T1.RANK
,T1.LOAD_DATE
,T2.RANK AS LAST_RANK
,T2.LOAD_DATE AS LAST_LOAD_DATE
, (( CAST ( SUBSTRING(T1.RANK FROM '[0-9]+') AS INT ) ) - ( CAST ( SUBSTRING(T2.RANK FROM '[0-9]+') AS INT )) ) *-1 AS R_VAR 

,cast ( T1.LOAD_DATE - T2.LOAD_DATE as int )   AS R_VAR_ACTUAL_DAYSDIFF 
FROM DF_H_ZACKS_INDICATORS T1 
LEFT JOIN   DF_H_ZACKS_INDICATORS T2 ON T1.TICKER=T2.TICKER

WHERE T2.LOAD_DATE = (SELECT MAX (T2A.LOAD_DATE)
					 FROM DF_H_ZACKS_INDICATORS T2A
					  WHERE T2A.TICKER=T2.TICKER
                       and T2A.RANK <>T1.RANK   --here the diff 
					  AND T2A.LOAD_DATE<T1.LOAD_DATE)



      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
and  T1.LOAD_DATE =T2.LOAD_DATE ;



 $$;