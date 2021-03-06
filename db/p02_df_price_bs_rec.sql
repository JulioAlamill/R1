CREATE OR REPLACE PROCEDURE p02_df_price_bs_rec ( MAX_DATE DATE  DEFAULT CURRENT_DATE )
LANGUAGE plpgsql AS $PROCEDURE$
BEGIN

--      DF_PRICE_BS_REC
DROP TABLE IF EXISTS DF_PRICE_BS_REC_S;
CREATE TABLE DF_PRICE_BS_REC_S  AS 

SELECT 
T1.	TICKER
,T1. PRICE_DATE
,T1. CLOSE_ADJ_PRICE
,T1. MA_10_BS_FLAG
,T1. MACD_BS_FLAG
,T1. SMI_BS_FLAG
,T1. ALL_BS_SUM
,T1. SEQ_EVENT
,T1. LOAD_DATE
FROM DF_BUY_SELL_FLAG T1
WHERE  1=1
--AND TICKER  IN ('LOGI', 'AMZN', 'TPL' , 'DQ' )
AND T1. PRICE_DATE  = (SELECT  MAX (T1A. PRICE_DATE)
						FROM DF_BUY_SELL_FLAG AS T1A
						WHERE  T1A. PRICE_DATE <=  MAX_DATE 
						AND T1.TICKER = T1A.TICKER);
				
				

-- Index created

CREATE  INDEX DF_PRICE_BS_REC_S_IX_1 ON DF_PRICE_BS_REC_S  (TICKER);  
CREATE  INDEX DF_PRICE_BS_REC_S_IX_2 ON DF_PRICE_BS_REC_S  (TICKER,SEQ_EVENT );  
CREATE  INDEX DF_PRICE_BS_REC_S_IX_3 ON DF_PRICE_BS_REC_S  (TICKER,SEQ_EVENT desc );  
CREATE  INDEX DF_PRICE_BS_REC_S_IX_4 ON DF_PRICE_BS_REC_S  (TICKER , SEQ_EVENT asc  );  

-- ADD NEDED COLUMNS 
ALTER TABLE DF_PRICE_BS_REC_S  

ADD ALL_BS_SUM_DB INT
,ADD BS_FLAG  TEXT
,ADD DIFF_PRICE_PRC NUMERIC (10,6)
,ADD bs_flag_trend_days_in INT

,ADD Z_RANK	TEXT
,ADD Z_RANK_NUM INT 
,ADD Z_SECTOR_CLASS	TEXT
,ADD Z_INDUSTRY_RANK	TEXT
,ADD Z_EXP_GROWTH	DECIMAL (10,2)
,ADD Z_LOAD_DATE	DATE

,ADD Z_RANK_LB TEXT
,ADD Z_RANK_LB_NUM INT
,ADD Z_LOAD_DATE_LB DATE

,ADD Y_RANK	TEXT
,ADD Y_RANK_NUM INT 
,ADD Y_PRICE_1Y_EST	DECIMAL (10,2)
,ADD Y_LOAD_DATE	DATE


,ADD Y_RANK_LB TEXT
,ADD Y_RANK_LB_NUM INT
,ADD Y_LOAD_DATE_LB  DATE

,ADD REC_RANK TEXT
,ADD REC_RANK_IND TEXT
,ADD REC_RANK_NUM INT 
,ADD REC_VAR_ACTUAL	INT DEFAULT 0

,ADD REC_RANK_LB_NUM INT    

,ADD Z_VAR_ACTUAL_DAYSDIFF INT       
,ADD Y_VAR_ACTUAL_DAYSDIFF INT     
  
,ADD REC_VAR_ACTUAL_DAYSDIFF INT       

 --quote   

,ADD EPS	DECIMAL (16,6)
,ADD PE_RATIO	DECIMAL (16,6)
,ADD MARKET_CAP	VARCHAR(20)
,ADD AVG_VOLUME	DECIMAL (15,2)

,ADD Y_GROWTH_EST	DECIMAL (16,6)


--analitics
,ADD SS_SHARES_OUTSTANDING NUMERIC
,ADD DS_TRAILING_ANNUAL_DIV_RATE DECIMAL (12,6)

;



 ---BUY SELL FLAGS  AND CLOSE PRICE UPDATES STIKER_PRICE IF IT IS MORE RECENT 
 ---DAILY UPDATED WILL BE FROM THIS UPDATED
UPDATE DF_PRICE_BS_REC_S T1
SET
    ALL_BS_SUM_DB	=	T2.	ALL_BS_SUM_DB
    ,BS_FLAG = T2.BS_FLAG
    ,DIFF_PRICE_PRC = T2.DIFF_PRICE_PRC
    
FROM (

            SELECT 
            T1.TICKER
            ,T1.PRICE_DATE
            ,T2.PRICE_DATE  AS  DATE_DB
            ,T1.ALL_BS_SUM      
            ,T2.ALL_BS_SUM    AS  ALL_BS_SUM_DB
            , CASE WHEN T1.ALL_BS_SUM = 3 THEN  'BUY ++'
            WHEN T1.ALL_BS_SUM = -3 THEN  'SELL ++'
            WHEN T1.ALL_BS_SUM > 0 AND T2.ALL_BS_SUM  < T1.ALL_BS_SUM  THEN  'BUY +'
            WHEN T1.ALL_BS_SUM < 0 AND T2.ALL_BS_SUM  > T1.ALL_BS_SUM  THEN  'SELL +'
            WHEN T1.ALL_BS_SUM > 0 AND T2.ALL_BS_SUM  >= T1.ALL_BS_SUM  THEN  'BUY -'
            WHEN T1.ALL_BS_SUM < 0 AND T2.ALL_BS_SUM  <= T1.ALL_BS_SUM  THEN  'SELL -'

            ELSE NULL END AS BS_FLAG
            ,T1.CLOSE_ADJ_PRICE AS CLOSE_STIKER_PRICE
            ,T1.CLOSE_ADJ_PRICE-T2.CLOSE_ADJ_PRICE AS DIFF_PRICE
            ,(T1.CLOSE_ADJ_PRICE-T2.CLOSE_ADJ_PRICE )/T1.CLOSE_ADJ_PRICE AS DIFF_PRICE_PRC
        FROM PUBLIC.DF_PRICE_BS_REC_S  T1
        LEFT JOIN PUBLIC.DF_BUY_SELL_FLAG  T2 ON T1.TICKER =T2.TICKER AND T1.SEQ_EVENT+1 =T2.SEQ_EVENT 
    

  )  AS T2 
  WHERE  T1.TICKER=T2.TICKER;
  



--bs_flag_trend_days_in                                    
      
UPDATE DF_PRICE_BS_REC_S T1
SET
 	bs_flag_trend_days_in	=	T2.	bs_flag_trend_days_in

FROM (

   SELECT 
    T1.TICKER
	,T1.PRICE_DATE 
    ,T1.ALL_BS_SUM
	,T2.PRICE_DATE AS BS_FLAG_SINCE_DATE
    ,T2.ALL_BS_SUM AS ALL_BS_SUM_START
	,T2.SEQ_EVENT- T1.SEQ_EVENT +1 AS bs_flag_trend_days_in  -- EXCLUDING  NO WORKING DAYS 
    FROM DF_PRICE_BS_REC_S  T1 
	LEFT JOIN PUBLIC.DF_BUY_SELL_FLAG  T2 ON   T1.TICKER=T2.TICKER 
    WHERE  1=1
    AND  T2.SEQ_EVENT+1 = ( SELECT  MIN (T2A.SEQ_EVENT)
                    FROM PUBLIC.DF_BUY_SELL_FLAG  T2A
                    WHERE CASE WHEN T1.ALL_BS_SUM > 0  THEN T2A.ALL_BS_SUM < 0 
                                    ELSE  T2A.ALL_BS_SUM > 0 END  -- FIND THE LATES OPOSITE , TO GET DAY AFTER 
                    AND T2A.TICKER = T2.TICKER
                    AND T2A.PRICE_DATE< T1.PRICE_DATE 
                   ) 
  
  )  AS T2 
  WHERE  T1.TICKER=T2.TICKER;

  

/*

-- ZACKS DATA 
*/

--LATEST 

UPDATE  DF_PRICE_BS_REC_S  T1
SET

Z_RANK = T2.Z_RANK
,Z_SECTOR_CLASS = T2.Z_SECTOR_CLASS
,Z_INDUSTRY_RANK = T2.Z_INDUSTRY_RANK
,Z_EXP_GROWTH = T2.Z_EXP_GROWTH
,Z_LOAD_DATE = T2.Z_LOAD_DATE
,Z_RANK_NUM=T2.Z_RANK_NUM
FROM (
    SELECT
     T1.PRICE_DATE
    ,T2.TICKER
    ,T2.Z_RANK 
    ,T2.Z_RANK_NUM AS Z_RANK_NUM
    ,T2.Z_SECTOR_CLASS AS Z_SECTOR_CLASS
    ,T2.Z_INDUSTRY_RANK AS Z_INDUSTRY_RANK
    ,T2.Z_EXP_GROWTH AS Z_EXP_GROWTH
    ,T2.LOAD_DATE AS Z_LOAD_DATE
    FROM DF_PRICE_BS_REC_S   T1
    LEFT JOIN  DF_H_ZACKS_INDICATORS T2 ON T1.TICKER= T2.TICKER  
                                    AND T2.LOAD_DATE = ( SELECT  MAX ( T2A.LOAD_DATE )
                                                          FROM DF_H_ZACKS_INDICATORS T2A
                                                          WHERE T2.TICKER = T2A.TICKER
                                                          AND T2A.LOAD_DATE<= T1.PRICE_DATE ) 
    WHERE 1=1
    
    --AND T1.TICKER  = 'LOGI'

      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
AND  T1.PRICE_DATE=T2.PRICE_DATE;


/*
LAST BEFORE 
*/



UPDATE  DF_PRICE_BS_REC_S  T1
SET

Z_RANK_LB = T2.Z_RANK
,Z_RANK_LB_NUM =T2.Z_RANK_NUM
,Z_LOAD_DATE_LB = T2.Z_LOAD_DATE

FROM (
    SELECT
     T1.PRICE_DATE
    ,T2.TICKER
    ,T2.Z_RANK AS Z_RANK
    ,T2.Z_RANK_NUM AS Z_RANK_NUM
    ,T2.Z_SECTOR_CLASS AS Z_SECTOR_CLASS
    ,T2.Z_INDUSTRY_RANK AS Z_INDUSTRY_RANK
    ,T2.Z_EXP_GROWTH AS Z_EXP_GROWTH
    ,T2.LOAD_DATE AS Z_LOAD_DATE
    FROM DF_PRICE_BS_REC_S   T1
    LEFT JOIN  DF_H_ZACKS_INDICATORS T2 ON T1.TICKER= T2.TICKER  
                                    AND T2.LOAD_DATE = ( SELECT  MAX ( T2A.LOAD_DATE )
                                                          FROM DF_H_ZACKS_INDICATORS T2A
                                                          WHERE T2.TICKER = T2A.TICKER
														 AND T2A.Z_RANK <>T1.Z_RANK 
                                                          AND T2A.LOAD_DATE<= T1.Z_LOAD_DATE ) 

      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
AND  T1.PRICE_DATE=T2.PRICE_DATE;


/*

-- YAHOO 
*/

--LATEST
UPDATE  DF_PRICE_BS_REC_S  T1
SET
Y_RANK = T2.Y_RANK
,Y_RANK_NUM = T2.Y_RANK_NUM
,Y_PRICE_1Y_EST = T2.Y_PRICE_1Y_EST
,Y_LOAD_DATE = T2.Y_LOAD_DATE



,	EPS=T2.EPS
,	AVG_VOLUME=T2.AVG_VOLUME
,Y_GROWTH_EST=t2.Y_GROWTH_EST
,	SS_SHARES_OUTSTANDING=T2.SS_SHARES_OUTSTANDING
,	DS_TRAILING_ANNUAL_DIV_RATE=T2.DS_TRAILING_ANNUAL_DIV_RATE   

FROM (

SELECT 
        T1.PRICE_DATE
        ,T1.TICKER
        ,T2.Y_RANK
        , T2.Y_RANK_NUM AS Y_RANK_NUM 
        ,	CAST (T2.Y_PRICE_1Y_EST AS DECIMAL (10,2)) AS Y_PRICE_1Y_EST
        
,	T2.EPS
,	T2.PE_RATIO
,	T2.MARKET_CAP
,	T2.AVG_VOLUME
,COALESCE( T2.GROWTH_EST_NEXT_5_YEARS, T2.GROWTH_EST_NEXT_YEAR, 0 )AS Y_GROWTH_EST
,	T2.SS_SHARES_OUTSTANDING
,	T2.DIVIDEND_RATE_PER_SHARE AS DS_TRAILING_ANNUAL_DIV_RATE   
,	T2.LOAD_DATE AS Y_LOAD_DATE

FROM DF_PRICE_BS_REC_S   T1
LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER AND T2.LOAD_DATE = ( SELECT  MAX ( T2A.LOAD_DATE )
																			  FROM DF_H_YAHOO_RECOMMENDATIONS T2A
																			  WHERE T2.TICKER = T2A.TICKER
																			  AND T2A.LOAD_DATE<= T1.PRICE_DATE )  

    WHERE 1=1
    
  --AND T1.TICKER  = 'LOGI'
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
AND  T1.PRICE_DATE=T2.PRICE_DATE;

/*
--LAST BEFORE 
*/

UPDATE  DF_PRICE_BS_REC_S  T1
SET


Y_RANK_LB  = T2.Y_RANK
,Y_RANK_LB_NUM = T2.Y_RANK_NUM
,Y_LOAD_DATE_LB  = T2.Y_LOAD_DATE

FROM (

SELECT 
        T1.PRICE_DATE
        ,T1.TICKER
        ,T2.Y_RANK
        , T2.Y_RANK_NUM AS Y_RANK_NUM 
        ,	CAST (T2.Y_PRICE_1Y_EST AS DECIMAL (10,2)) AS Y_PRICE_1Y_EST
        ,	T2.LOAD_DATE AS Y_LOAD_DATE

FROM DF_PRICE_BS_REC_S   T1
LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER AND T2.LOAD_DATE = ( SELECT  MAX ( T2A.LOAD_DATE )
																			  FROM DF_H_YAHOO_RECOMMENDATIONS T2A
																			  WHERE T2.TICKER = T2A.TICKER
                                                                              AND T2A.Y_RANK <>T1.Y_RANK 
																			  AND T2A.LOAD_DATE<= T1.Y_LOAD_DATE )  

    WHERE 1=1
    
  --AND T1.TICKER  = 'LOGI'
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER
AND  T1.PRICE_DATE=T2.PRICE_DATE;




/*
------------------------------------------
COMBAINED 
------------------------------------------

*/
--REC_RANK_NUM
UPDATE  DF_PRICE_BS_REC_S  SET 
REC_RANK_NUM = ROUND (((COALESCE (Y_RANK_NUM, Z_RANK_NUM)
                                                    + 
                                                     COALESCE ( Z_RANK_NUM,Y_RANK_NUM)) 
                                                        / 2)
                                                        -.01)  
 , REC_RANK_LB_NUM = ROUND (((COALESCE (Y_RANK_LB_NUM, Z_RANK_LB_NUM)
                                                    + 
                                    COALESCE ( Z_RANK_LB_NUM,Y_RANK_LB_NUM)) 
                                                        / 2)
                                                        -.01)  ; 




--REC_RANK 
UPDATE  DF_PRICE_BS_REC_S  SET REC_RANK =  CASE WHEN REC_RANK_NUM =5 THEN '5-STRONG' 
                                            WHEN REC_RANK_NUM =4 THEN '4-SELL' 
                                            WHEN REC_RANK_NUM =3 THEN '3-HOLD' 
                                            WHEN REC_RANK_NUM =2 THEN '2-BUY' 
                                            WHEN REC_RANK_NUM =1 THEN '1-STRONG' 
                                            ELSE  NULL END;



-- ACTUAL VARIANCES 
UPDATE DF_PRICE_BS_REC_S  T1 SET REC_VAR_ACTUAL = (REC_RANK_NUM- REC_RANK_LB_NUM ) * -1 ;
UPDATE DF_PRICE_BS_REC_S  T1 SET REC_VAR_ACTUAL = 0 WHERE  REC_RANK_LB_NUM =0;

--Z_VAR_ACTUAL_DAYSDIFF
UPDATE DF_PRICE_BS_REC_S  T1 SET 
Z_VAR_ACTUAL_DAYSDIFF=   Z_LOAD_DATE - Z_LOAD_DATE_LB; 
--Y_VAR_ACTUAL_DAYSDIFF
UPDATE DF_PRICE_BS_REC_S  T1 SET 
Y_VAR_ACTUAL_DAYSDIFF=   Y_LOAD_DATE - Y_LOAD_DATE_LB ; 
--REC_VAR_ACTUAL_DAYSDIFF
UPDATE DF_PRICE_BS_REC_S  T1 SET
 REC_VAR_ACTUAL_DAYSDIFF =   ( COALESCE ( Z_VAR_ACTUAL_DAYSDIFF , Y_VAR_ACTUAL_DAYSDIFF) 
                                +
                                 COALESCE ( Y_VAR_ACTUAL_DAYSDIFF ,Z_VAR_ACTUAL_DAYSDIFF)  
                              )/ 2 ;                                 


--REC_RANK_IND
UPDATE DF_PRICE_BS_REC_S  T1 SET REC_RANK_IND = 'Z' WHERE  Z_RANK IS NOT NULL; 
UPDATE DF_PRICE_BS_REC_S  T1 SET REC_RANK_IND = COALESCE (REC_RANK_IND, '' )||'Y' WHERE  Y_RANK IS NOT NULL;


DROP TABLE IF EXISTS DF_PRICE_BS_REC;
CREATE TABLE DF_PRICE_BS_REC  AS 
SELECT 

	TICKER
,	PRICE_DATE
,	CLOSE_ADJ_PRICE
,	DIFF_PRICE_PRC
,	ALL_BS_SUM
,	ALL_BS_SUM_DB
,	BS_FLAG
,	bs_flag_trend_days_in
,	REC_RANK
,	REC_RANK_IND
,	REC_VAR_ACTUAL_DAYSDIFF
,	REC_VAR_ACTUAL
,	REC_RANK_NUM
,	REC_RANK_LB_NUM
,	Z_RANK
,	Z_RANK_LB
,	Z_LOAD_DATE
,	Z_LOAD_DATE_LB
,	Z_VAR_ACTUAL_DAYSDIFF
,	Y_RANK
,	Y_RANK_LB
,	Y_LOAD_DATE
,	Y_LOAD_DATE_LB
,	Y_VAR_ACTUAL_DAYSDIFF
,	Z_RANK_NUM
,	Z_RANK_LB_NUM
,	Z_SECTOR_CLASS
,	Z_INDUSTRY_RANK
,	Z_EXP_GROWTH
,	Y_RANK_NUM
,	Y_PRICE_1Y_EST
,	Y_RANK_LB_NUM
,	MA_10_BS_FLAG
,	MACD_BS_FLAG
,	SMI_BS_FLAG

            , EPS	
            , PE_RATIO	
            , MARKET_CAP	
            , AVG_VOLUME	
    
            , Y_GROWTH_EST	
                , SS_SHARES_OUTSTANDING 
            , DS_TRAILING_ANNUAL_DIV_RATE
            
,	SEQ_EVENT

,	LOAD_DATE
FROM 	DF_PRICE_BS_REC_S;


END
$PROCEDURE$

