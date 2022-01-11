CREATE OR REPLACE PROCEDURE p02_df_kpi ()

LANGUAGE plpgsql
AS $procedure$
BEGIN


/*
--#CREATED A FINAL S_PNT_FUNDAMENTALS TABLES FROM THE ONE CREATED WITH PYTHON SCRIPT : ETL_SQL_02_DF_FUNDAMENTALS_S2
*/


DROP TABLE IF EXISTS DF_FUNDAMENTALS;  CREATE TABLE DF_FUNDAMENTALS  AS   SELECT * FROM DF_FUNDAMENTALS_S2;



/*
--#CREATE KPI SATGING  TABLE
*/



DROP TABLE IF EXISTS DF_KPI_S;
CREATE TABLE DF_KPI_S AS (

     SELECT 
        T1 .TICKER
        ,T1.BIG5_TOTAL
        ,CAST(  T1.VALUES_AVG AS DECIMAL (14,2) ) AS ROIC
        ,CAST(  T2.VALUES_AVG AS DECIMAL (14,2) ) AS REVENUE_GW
        ,CAST(  T3.VALUES_AVG AS DECIMAL (14,2) ) AS EARNINGS_GW
        ,CAST(  T4.VALUES_AVG AS DECIMAL (14,2) ) AS EQUITY_GW
        ,CAST(  T5.VALUES_AVG AS DECIMAL (14,2) ) AS FREECASHFLOW_GW
        ,	COALESCE ( T6.FUNDAMENTAL_NAME ||  ' ' ||CAST(  T6.VALUES_AVG*100 AS DECIMAL (14,2) ) ||'%' , 'NONE') AS BIG5_FAIL      
        ,CAST(  T6.VALUES_AVG AS DECIMAL (14,2) ) AS BIG5_FAIL_VALUE
        ,T6 .FUNDAMENTAL_NAME

        ,CAST(  T4.VALUES_AVG AS DECIMAL (14,2) ) AS  HISTORICAL_GROWTH 

        ,CAST(  T7.VALUE AS DECIMAL (14,2) ) AS  DEBT_LT_RATIO
        ,CAST(  T8.VALUE AS DECIMAL (14,2) ) AS  DEBT_ST_RATIO

    FROM PUBLIC.DF_FUNDAMENTALS T1 -- ROIC
    LEFT JOIN DF_FUNDAMENTALS T2 ON T1.TICKER=T2.TICKER AND  T2.FUNDAMENTAL_ID  =14 --REVENUE_GW
    LEFT JOIN DF_FUNDAMENTALS T3 ON T1.TICKER=T3.TICKER AND  T3.FUNDAMENTAL_ID  =15 --EARNINGS_GW
    LEFT JOIN DF_FUNDAMENTALS T4 ON T1.TICKER=T4.TICKER AND  T4.FUNDAMENTAL_ID  =16--EQUITY_GW
    LEFT JOIN DF_FUNDAMENTALS T5 ON T1.TICKER=T5.TICKER AND  T5.FUNDAMENTAL_ID  =17--FREECASHFLOW_GW
    
    LEFT JOIN DF_FUNDAMENTALS T6 ON T1.TICKER=T6.TICKER AND T6.RULE_1_FLAG= 0  
                                                        AND T6 .FUNDAMENTAL_ID  IN (
                                                        SELECT FUNDAMENTAL_ID 
                                                        FROM DF_FUNDAMENTAL_ID  
                                                        WHERE R1_BIGFIVE_FLAG = 1 ) --FAIL
                                                        
    
    
    LEFT JOIN  DF_FUNDAMENTALS_S1 T7 ON T1.TICKER=T7.TICKER AND T7.FUNDAMENTAL_ID = 21
   LEFT JOIN  DF_FUNDAMENTALS_S1 T8 ON T1.TICKER=T8.TICKER AND  T7.YEAR=T8.YEAR AND T8.FUNDAMENTAL_ID = 22 
    
    
    WHERE  T1.BIG5_TOTAL >3
    AND T1 .FUNDAMENTAL_ID  =13
	
	 AND T7.YEAR  = (SELECT MAX (T7A.YEAR)
                        FROM  DF_FUNDAMENTALS_S1 T7A
                   WHERE T7.TICKER=T7A.TICKER)
                   
                   
  ) ;                 
                   


    
ALTER TABLE DF_KPI_S  

--Yahoo 
ADD Y_PRICE_1Y_EST	DECIMAL (16,6)
,ADD EPS	DECIMAL (16,6)
,ADD PE_RATIO	DECIMAL (16,6)
,ADD MARKET_CAP	VARCHAR(20)
,ADD AVG_VOLUME	DECIMAL (15,2)
,ADD STIKER_PRICE	DECIMAL (16,6)
,ADD Y_GROWTH_EST	DECIMAL (16,6)
,ADD NAME VARCHAR (100)
,ADD EXCHDISP  VARCHAR (100)
,ADD FOLLOW_UP_FLAG INT DEFAULT 0
,ADD PROFOLIO_FLAG INT DEFAULT 0
,ADD BS_FLAG VARCHAR(20)
,ADD STIKER_PRICE_DATE DATE

--zacks
,ADD Z_GROWTH_EST DECIMAL (16,6)
,ADD Z_SECTOR_CLASS VARCHAR(100)
,ADD Z_RANK VARCHAR(20)
,ADD Z_INDUSTRY_RANK VARCHAR(50)

--Calcs
,ADD GOODPRICE_R1_FLAG INT
,ADD GOODPRICE_AVG_FLAG INT

,ADD R1_GROWTH_EST	DECIMAL (16,6)
,ADD MIN_RATE_RETURN 	DECIMAL (16,6)


,ADD PROFIT_Y_1Y_EST	DECIMAL (16,6)
,ADD PROFIT_R1_1Y_EST	DECIMAL (16,6)
,ADD PROFIT_COMBINE_1Y_EST DECIMAL (10,2) DEFAULT 0


-- Score
,ADD S_PNT_BIG_FIVE DECIMAL (10,2)
,ADD S_PNT_PRICE  DECIMAL (10,2)
,ADD S_PNT_VALUATION_PROFIT  DECIMAL (10,2)
,ADD S_PNT_GROWTH   DECIMAL (10,2)
,ADD S_PNT_Y_PROFIT  DECIMAL (10,2)
,ADD S_PNT_Z_RANK DECIMAL (10,2)
,ADD S_PNT_MISS_DATA  DECIMAL (10,2)
,ADD S_PNT_SECTOR  DECIMAL (10,2)
,ADD S_PNT_DEBT  DECIMAL (10,2)
,ADD S_PNT_FUNDAMENTALS DECIMAL (10,2)
,ADD S_PNT_PROFIT_PROJECTION DECIMAL (10,2)
,ADD S_PNT_MARKET_CONSENSUS DECIMAL (10,2)
,ADD S_PNT_OTHERS_KPI DECIMAL (10,2)
,ADD SCORE_TICKER DECIMAL (10,2)

--VALUATION
,ADD VALUATION_R1 DECIMAL (10,2)
,ADD VALUATION_DCF DECIMAL (10,2)
,ADD VALUATION_ROE DECIMAL (10,2)
,ADD VALUATION_AVG DECIMAL (10,2)
,ADD VALUATION_R1_1Y_EST DECIMAL (10,2)
,ADD VALUATION_NUM DECIMAL (10,2)
,ADD VALUATION_R1_MOS	DECIMAL (14,2) 
,ADD VALUATION_R1_VS_CURRENT 	DECIMAL (16,6)
,ADD VALUATION_AVG_VS_CURRENT 	DECIMAL (16,6)
;




--updates from  DF_QUOTE_H  1st
UPDATE DF_KPI_S T1
SET
 	Y_PRICE_1Y_EST	=	T2.Y_PRICE_1Y_EST
,	EPS	=	T2.	EPS
,	PE_RATIO	=	T2.	PE_RATIO
,	MARKET_CAP	=	T2.	MARKET_CAP
,	AVG_VOLUME	=	T2.	AVG_VOLUME
,	STIKER_PRICE	=	T2.	STIKER_PRICE
,	Y_GROWTH_EST	=	T2.	Y_GROWTH_EST
,STIKER_PRICE_DATE = T2.LOAD_DATE
FROM (
    SELECT 
    TICKER
    ,CAST (T1.TARGET_EST_1Y AS DECIMAL (14,2)  ) AS  Y_PRICE_1Y_EST
    ,T1.EPS
    ,T1.PE_RATIO
    ,T1.MARKET_CAP
    ,T1.AVG_VOLUME
    , T1.QUOTE_PRICE AS STIKER_PRICE
    ,COALESCE( T1.GROWTH_EST_NEXT_5_YEARS, T1.GROWTH_EST_NEXT_YEAR, 0 )AS Y_GROWTH_EST
     ,T1.LOAD_DATE
    FROM PUBLIC.DF_QUOTE_H T1
    WHERE T1.LOAD_DATE = (SELECT MAX (T1A.LOAD_DATE )
                             FROM DF_QUOTE_H AS  T1A
                            WHERE T1A.TICKER=T1.TICKER)
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER ;
  
                        


--updates from  DF_QUOTE_H  2st  in case there  is miss data this mean to fill up with the lates complete if exists 

UPDATE DF_KPI_S T1
SET
 	Y_PRICE_1Y_EST	=	T2.	Y_PRICE_1Y_EST
,	EPS	=	T2.	EPS
,	PE_RATIO	=	T2.	PE_RATIO
,	MARKET_CAP	=	T2.	MARKET_CAP
,	AVG_VOLUME	=	T2.	AVG_VOLUME
,	STIKER_PRICE	=	T2.	STIKER_PRICE
,	Y_GROWTH_EST	=	T2.	Y_GROWTH_EST
, STIKER_PRICE_DATE = T2.LOAD_DATE
FROM (
    SELECT 
    TICKER
    ,CAST (T1.TARGET_EST_1Y AS DECIMAL (14,2)  ) AS  Y_PRICE_1Y_EST
    ,T1.EPS
    ,T1.PE_RATIO
    ,T1.MARKET_CAP
    ,T1.AVG_VOLUME
    , T1.QUOTE_PRICE AS STIKER_PRICE
    ,COALESCE( T1.GROWTH_EST_NEXT_5_YEARS, 0 )AS Y_GROWTH_EST
    ,T1.LOAD_DATE
    FROM PUBLIC.DF_QUOTE_H T1
    WHERE T1.LOAD_DATE = (SELECT MAX (T1A.LOAD_DATE )
                             FROM DF_QUOTE_H AS  T1A
                            WHERE T1A.TICKER=T1.TICKER
                            and T1A.TARGET_EST_1Y is not null)  --here the look to bring only full data
                                                        
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER and T1.Y_PRICE_1Y_EST is null ;
  
 


 ---BUY SELL FALGS  AND CLOSE PRICE UPDATES STIKER_PRICE IF IT IS MORE RECENT 
 ---Daily updated will be from this updated
UPDATE DF_KPI_S T1
SET
 	BS_FLAG	=	T2.	ALL_BS_FLAG_1MINUS
    ,STIKER_PRICE= CASE when  T2.DATE >=  T1.STIKER_PRICE_DATE THEN  T2.CLOSE_STIKER_PRICE ELSE T1.STIKER_PRICE END 
    , STIKER_PRICE_DATE = CASE when  T2.DATE >=  T1.STIKER_PRICE_DATE THEN  T2.DATE ELSE T1.STIKER_PRICE_DATE END 
FROM (
        SELECT 
        T2.TICKER
        ,T2.DATE
        ,CASE WHEN ABS (ALL_BS_SUM) = 3 
                    THEN T2.ALL_BS_FLAG_1MINUS || ' STRONG' 
                    ELSE  T2.ALL_BS_FLAG_1MINUS  END 
                    AS ALL_BS_FLAG_1MINUS
        ,t2.CLOSE as CLOSE_STIKER_PRICE
         FROM PUBLIC.DF_BUY_SELL_FLAG  T2
        WHERE T2.SEQ_EVENT = 1

  )  AS T2 
  WHERE  T1.TICKER=T2.TICKER;


-- NAMES FROM DF_H_TICKER_NAME
UPDATE DF_KPI_S T1
SET 
NAME= T2.NAME
,EXCHDISP =  T2.EXCHDISP
FROM  DF_H_TICKER_NAME AS T2 
WHERE  T1.TICKER=T2.TICKER;


-- Follow up flag 
UPDATE DF_KPI_S T1
SET 
FOLLOW_UP_FLAG=t2.TYPE
FROM  DF_FOLLOW_UP_LIST AS T2 
WHERE  T1.TICKER=T2.TICKER;


--DF_PORFOLIO_IMP
UPDATE DF_KPI_S T1
SET PROFOLIO_FLAG =  1
WHERE TICKER IN (
        SELECT  TICKER 
        FROM DF_PORFOLIO_IMP);



 
 ---Zacks Data 
UPDATE DF_KPI_S t1
SET
 	Z_GROWTH_EST	=	t2.	Z_GROWTH_EST
,	Z_SECTOR_CLASS	=	t2.	Z_SECTOR_CLASS
,	Z_RANK	=	t2.	Z_RANK
,	Z_INDUSTRY_RANK	=	t2.	INDUSTRY_RANK

from (
select 
ticker

,COALESCE( T1.EXP_GROWTH, 0 )AS Z_GROWTH_EST
,T1.SECTOR_CLASS AS Z_SECTOR_CLASS
,T1.RANK as Z_RANK
,T1.INDUSTRY_RANK
,T1.LOAD_DATE
from PUBLIC.df_H_Zacks_Indicators  t1
where t1.LOAD_DATE = (SELECT MAX (t1A.LOAD_DATE )
						 FROM df_H_Zacks_Indicators AS  t1A
						WHERE t1A.TICKER=t1.TICKER)

  )  as t2 
  where  t1.ticker=t2.ticker ;
  
                        
/*                        
---------------------------------------------------------------
----Calculation Start from here 
---------------------------------------------------------------
*/

--MIN RETURN
UPDATE DF_KPI_S SET MIN_RATE_RETURN = .15;

/*
************************************************************************************************************************
--GROWTH CALC 
************************************************************************************************************************
*/
UPDATE DF_KPI_S SET Z_GROWTH_EST= nullif( Z_GROWTH_EST,0);
UPDATE DF_KPI_S SET Y_GROWTH_EST= nullif( Y_GROWTH_EST,0);

UPDATE DF_KPI_S SET R1_GROWTH_EST = LEAST( ( --Average of Zacks and Yahoo - if null get the 
                                                    (coalesce (nullif( Z_GROWTH_EST,0),Y_GROWTH_EST )  
                                                  + coalesce (nullif( Y_GROWTH_EST,0) , Z_GROWTH_EST ))/2
                                                  )
                                                , nullif( HISTORICAL_GROWTH,0) );

--WHERE ZACKS  and  YAHOO EXPECTED GROWTH NOT FOUND BRING HISTORIC GROWTH AT 75%
UPDATE DF_KPI_S SET R1_GROWTH_EST = HISTORICAL_GROWTH * .75 WHERE nullif( Y_GROWTH_EST,0) =0 AND nullif( Z_GROWTH_EST,0) =0 ; 

/*
************************************************************************************************************************
--GROWHT CALC *END*
************************************************************************************************************************ */

/* ************************************************************************************************************************
--VALUATIONS START 
*************************************************************************************************************************/

/*
--VALUATIONS R1
# ########################################################################################
##PERS  HISTORIC AND  DEFAULT 
# ########################################################################################
# --	+  # default PER	price-to-earnings ratio (P/E ratio) --	
# R1 Page 154- a quick rule of thumb for figuring out PE is to double the Rule #1 growth rate. 
#Thus. if we think a company will grow its earnings at 8% for
# Next 10 years , then we can expect to see a PE of 16, 10 years from now.
#(Considerign the growth will be the same)
*/
DROP TABLE VALUATION_R1; 
CREATE TABLE VALUATION_R1 AS 

SELECT 
TICKER 
,STIKER_PRICE
, EPS
,STIKER_PRICE / nullif(EPS, 0) AS PER_H
,R1_GROWTH_EST
,R1_GROWTH_EST * (2 * 100 ) AS per_default
,MIN_RATE_RETURN
,cast (0 as decimal (22,6)) as   r1_future_per
,cast (0 as decimal (22,6)) as   CALC_EPS_10_YEARS
,cast (0 as decimal (22,6)) as   CALC_FUTURE_STROCK_PRICE
,cast (0 as decimal (22,6)) as   valuation_R1
FROM  DF_KPI_S T1;



---   MERGE  IN MISSING  eps 
UPDATE VALUATION_R1 T1
SET 
EPS=COALESCE (T1.EPS, T2.EPS )
FROM (

SELECT  
T1.TICKER
 ,T1.LATEST_YEAR_VALUES* 1000000 AS EARNINGS
 ,T2.SS_SHARES_OUTSTANDING
 , CASE WHEN (SS_SHARES_OUTSTANDING IS NULL OR SS_SHARES_OUTSTANDING = 0) THEN NULL ELSE (T1.LATEST_YEAR_VALUES* 1000000) / SS_SHARES_OUTSTANDING END AS EPS
FROM DF_FUNDAMENTALS  AS T1 
LEFT JOIN DF_STATS AS T2 ON T1.TICKER=T2.TICKER
WHERE UPPER(T1.FUNDAMENTAL_NAME)=   'EARNINGS'

) t2
WHERE  t1.TICKER=T2.TICKER   ;




---r1_future_per
UPDATE VALUATION_R1 SET 
r1_future_per = CASE WHEN  PER_H > 0 THEN LEAST( PER_H , per_default)
                                         ELSE  per_default END ;                                       
--CALC_EPS_10_YEARS - 10 YEARS EXP PRICE EXPECTED MIN PROFIT IN INVERSIONS  
UPDATE VALUATION_R1 SET CALC_EPS_10_YEARS = EPS * (POWER((1+R1_GROWTH_EST),10 ) ) where EPS > 0;
UPDATE VALUATION_R1 SET CALC_FUTURE_STROCK_PRICE= GREATEST ((CALC_EPS_10_YEARS * r1_future_per), 0);
--# 15% EXPECTED MIN PROFIT IN INVERSIONS AND 10 YEARS
UPDATE VALUATION_R1 SET  valuation_R1  = CALC_FUTURE_STROCK_PRICE * (1 / (POWER ((1+MIN_RATE_RETURN),10)));
UPDATE VALUATION_R1 SET valuation_R1 = NULL WHERE valuation_R1 =0;


---2.- DCF VALUATION		
--FUNDAMENTAL ARE IN MILLION UNITS 

DROP TABLE VALUATION_DCF; 
CREATE TABLE VALUATION_DCF AS 

SELECT distinct
    t1.ticker
     , t3.latest_year_values * 1000000 as  cashandcashequivalents
    ,t4.latest_year_values * 1000000 as  totalliab
    ,t5.latest_year_values * 1000000 as  freecashflow
    ,t2.ss_shares_outstanding
    ,t1. R1_GROWTH_EST
     , .0 as Growth_decline_rate
    , T1.MIN_RATE_RETURN

        , 12 as Y10_FCF_multiplier
    ,cast (0 as decimal (22,6)) as FCF_y1
    ,cast (0 as decimal (22,6)) as FCF_y2
    ,cast (0 as decimal (22,6)) as FCF_y3
    ,cast (0 as decimal (22,6)) as FCF_y4
    ,cast (0 as decimal (22,6)) as FCF_y5
    ,cast (0 as decimal (22,6)) as FCF_y6
    ,cast (0 as decimal (22,6)) as FCF_y7
    ,cast (0 as decimal (22,6)) as FCF_y8
    ,cast (0 as decimal (22,6)) as FCF_y9
    ,cast (0 as decimal (22,6)) as FCF_y10

    ,cast (0 as decimal (22,6)) as NPV_FCF_y1
    ,cast (0 as decimal (22,6)) as NPV_FCF_y2
    ,cast (0 as decimal (22,6)) as NPV_FCF_y3
    ,cast (0 as decimal (22,6)) as NPV_FCF_y4
    ,cast (0 as decimal (22,6)) as NPV_FCF_y5
    ,cast (0 as decimal (22,6)) as NPV_FCF_y6
    ,cast (0 as decimal (22,6)) as NPV_FCF_y7
    ,cast (0 as decimal (22,6)) as NPV_FCF_y8
    ,cast (0 as decimal (22,6)) as NPV_FCF_y9
    ,cast (0 as decimal (22,6)) as NPV_FCF_y10
    
    , CAST (0 AS DECIMAL (22,6)) AS  TOTAL_NPV_FCF
        , CAST (0 AS DECIMAL (22,6)) AS  Y10_FCF_VALUE
            , CAST (0 AS DECIMAL (22,6)) AS  COMPANY_VALUE
            , CAST (0 AS DECIMAL (22,6)) AS  valuation_DCF
            
FROM   DF_KPI_S AS T1  
LEFT JOIN DF_STATS AS T2 ON T1.TICKER=T2.TICKER
LEFT JOIN DF_FUNDAMENTALS  AS T3 ON T1.TICKER=T3.TICKER  AND UPPER (T3.FUNDAMENTAL_NAME)= 'CASHANDCASHEQUIVALENTS'
LEFT JOIN DF_FUNDAMENTALS  AS T4 ON T1.TICKER=T4.TICKER  AND UPPER (T4.FUNDAMENTAL_NAME)= 'TOTALLIAB'
LEFT JOIN DF_FUNDAMENTALS  AS T5 ON T1.TICKER=T5.TICKER  AND UPPER (T5.FUNDAMENTAL_NAME)= 'FREECASHFLOW';


UPDATE valuation_dcf SET FCF_y1  = FREECASHFLOW*(1+R1_GROWTH_EST);
UPDATE valuation_dcf SET  FCF_y2	= FCF_y1	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	1	)));
UPDATE valuation_dcf SET  FCF_y3	= FCF_y2	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	2	)));
UPDATE valuation_dcf SET  FCF_y4	= FCF_y3	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	3	)));
UPDATE valuation_dcf SET  FCF_y5	= FCF_y4	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	4	)));
UPDATE valuation_dcf SET  FCF_y6	= FCF_y5	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	5	)));
UPDATE valuation_dcf SET  FCF_y7	= FCF_y6	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	6	)));
UPDATE valuation_dcf SET  FCF_y8	= FCF_y7	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	7	)));
UPDATE valuation_dcf SET  FCF_y9	= FCF_y8	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	8	)));
UPDATE valuation_dcf SET  FCF_y10	= FCF_y9	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	9	)));


UPDATE valuation_dcf SET NPV_FCF_Y1 = FCF_Y1 /  POWER(  (1 + MIN_RATE_RETURN ), 1 );
UPDATE valuation_dcf SET NPV_FCF_Y2 = FCF_Y2 /  POWER(  (1 + MIN_RATE_RETURN ), 2 );
UPDATE valuation_dcf SET NPV_FCF_Y3 = FCF_Y3 /  POWER(  (1 + MIN_RATE_RETURN ), 3 );
UPDATE valuation_dcf SET NPV_FCF_Y4 = FCF_Y4 /  POWER(  (1 + MIN_RATE_RETURN ), 4 );
UPDATE valuation_dcf SET NPV_FCF_Y5 = FCF_Y5 /  POWER(  (1 + MIN_RATE_RETURN ), 5 );
UPDATE valuation_dcf SET NPV_FCF_Y6 = FCF_Y6 /  POWER(  (1 + MIN_RATE_RETURN ), 6 );
UPDATE valuation_dcf SET NPV_FCF_Y7 = FCF_Y7 /  POWER(  (1 + MIN_RATE_RETURN ), 7 );
UPDATE valuation_dcf SET NPV_FCF_Y8 = FCF_Y8 /  POWER(  (1 + MIN_RATE_RETURN ), 8 );
UPDATE valuation_dcf SET NPV_FCF_Y9 = FCF_Y9 /  POWER(  (1 + MIN_RATE_RETURN ), 9 );
UPDATE valuation_dcf SET NPV_FCF_Y10 = FCF_Y10 /  POWER(  (1 + MIN_RATE_RETURN ), 10 );


UPDATE valuation_dcf SET Total_NPV_FCF= 	NPV_FCF_Y1 
+	NPV_FCF_Y2
+	NPV_FCF_Y3
+	NPV_FCF_Y4
+	NPV_FCF_Y5
+	NPV_FCF_Y6
+	NPV_FCF_Y7
+	NPV_FCF_Y8
+	NPV_FCF_Y9
+	NPV_FCF_Y10;


UPDATE valuation_dcf SET  Y10_FCF_VALUE= NPV_FCF_Y10 *Y10_FCF_MULTIPLIER;
UPDATE valuation_dcf SET COMPANY_VALUE= TOTAL_NPV_FCF + Y10_FCF_VALUE + CASHANDCASHEQUIVALENTS -TOTALLIAB;
UPDATE valuation_dcf SET valuation_DCF = case when  SS_SHARES_OUTSTANDING = 0 then null else COMPANY_VALUE / SS_SHARES_OUTSTANDING end ;
UPDATE valuation_dcf SET valuation_DCF = null where valuation_DCF =0;
 
                        

/*
---
3.-  valuation ROE
*/


DROP TABLE valuation_roe; 
CREATE TABLE valuation_roe AS 
SELECT  distinct 
    T1.TICKER
    ,T3.latest_year_values * 1000000 AS  SE -- TOTALSTOCKHOLDEREQUITY
    ,T4.latest_year_values * 1000000 AS    earnings
    , (T4.latest_year_values * 1000000) / ( NULLIF (T3.values_avg, 0 ) * 1000000)  AS ROE  --- earnings / EQUITY
    ,T2.SS_SHARES_OUTSTANDING   
    ,COALESCE(T2.DS_TRAILING_ANNUAL_DIV_RATE,0) AS DIVIDEND_RATE_PER_SHARE
    ,T1.R1_GROWTH_EST
    ,T1.MIN_RATE_RETURN
    
    ,cast (0 as decimal (22,6)) as SES_y1
    ,cast (0 as decimal (22,6)) as SES_y2
    ,cast (0 as decimal (22,6)) as SES_y3
    ,cast (0 as decimal (22,6)) as SES_y4
    ,cast (0 as decimal (22,6)) as SES_y5
    ,cast (0 as decimal (22,6)) as SES_y6
    ,cast (0 as decimal (22,6)) as SES_y7
    ,cast (0 as decimal (22,6)) as SES_y8
    ,cast (0 as decimal (22,6)) as SES_y9
    ,cast (0 as decimal (22,6)) as SES_y10
    
     ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y1
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y2
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y3
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y4
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y5
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y6
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y7
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y8
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y9
    ,CAST (0 AS DECIMAL (22,6)) AS DIV_Y10
    
    
         ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y1
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y2
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y3
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y4
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y5
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y6
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y7
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y8
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y9
    ,CAST (0 AS DECIMAL (22,6)) AS NPV_DIV_Y10
    
    
      ,CAST (0 AS DECIMAL (22,6)) AS   Y10_NET_INCOME
       ,CAST (0 AS DECIMAL (22,6)) AS  ROE_REQUIRED_VALUE
        ,CAST (0 AS DECIMAL (22,6)) AS NPV_ROE_REQUIRED_VALUE
        ,CAST (0 AS DECIMAL (22,6)) AS TOTAL_NPV_DIV
        ,CAST (0 AS DECIMAL (22,6)) AS valuation_ROE

FROM   DF_KPI_S AS T1  
LEFT JOIN DF_STATS AS T2 ON T1.TICKER=T2.TICKER
LEFT JOIN DF_FUNDAMENTALS  AS T3 ON T1.TICKER=T3.TICKER  AND  UPPER(T3.FUNDAMENTAL_NAME)=   'TOTALSTOCKHOLDEREQUITY'
LEFT JOIN DF_FUNDAMENTALS  AS T4 ON T1.TICKER=T4.TICKER  AND UPPER(T4.FUNDAMENTAL_NAME)=   'EARNINGS' ;


--SES_* = Share HOLDER EQUITY  per share 
UPDATE valuation_ROE SET SES_y1 = case when SS_SHARES_OUTSTANDING =0 then null else  (SE*(1+R1_GROWTH_EST)) / SS_SHARES_OUTSTANDING end ; 
UPDATE valuation_ROE SET SES_y2 = SES_y1*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y3 = SES_y2*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y4 = SES_y3*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y5 = SES_y4*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y6 = SES_y5*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y7 = SES_y6*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y8 = SES_y7*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y9 = SES_y8*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET SES_y10 = SES_y9*(1+R1_GROWTH_EST);

--DIV_*
UPDATE valuation_ROE SET DIV_y1 = DIVIDEND_RATE_PER_SHARE*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y2 = DIV_y1*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y3 = DIV_y2*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y4 = DIV_y3*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y5 = DIV_y4*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y6 = DIV_y5*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y7 = DIV_y6*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y8 = DIV_y7*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y9 = DIV_y8*(1+R1_GROWTH_EST); 
UPDATE valuation_ROE SET DIV_y10 = DIV_y9*(1+R1_GROWTH_EST);
 
--NPV_DIV_*
UPDATE valuation_ROE SET npv_DIV_y1 = DIV_y1/ (POWER (( 1 + MIN_RATE_RETURN ),0)); 
UPDATE valuation_ROE SET npv_DIV_y2 = DIV_y2/ (POWER (( 1 + MIN_RATE_RETURN ),1));
UPDATE valuation_ROE SET npv_DIV_y3 = DIV_y3/ (POWER (( 1 + MIN_RATE_RETURN ),2));
UPDATE valuation_ROE SET npv_DIV_y4 = DIV_y4/ (POWER (( 1 + MIN_RATE_RETURN ),3));
UPDATE valuation_ROE SET npv_DIV_y5 = DIV_y5/ (POWER (( 1 + MIN_RATE_RETURN ),4));
UPDATE valuation_ROE SET npv_DIV_y6 = DIV_y6/ (POWER (( 1 + MIN_RATE_RETURN ),5));
UPDATE valuation_ROE SET npv_DIV_y7 = DIV_y7/ (POWER (( 1 + MIN_RATE_RETURN ),6));
UPDATE valuation_ROE SET npv_DIV_y8 = DIV_y8/ (POWER (( 1 + MIN_RATE_RETURN ),7));
UPDATE valuation_ROE SET npv_DIV_y9 = DIV_y9/ (POWER (( 1 + MIN_RATE_RETURN ),8));
UPDATE valuation_ROE SET npv_DIV_y10 = DIV_y10/ (POWER (( 1 + MIN_RATE_RETURN ),9));

--Y10_net_income = eaning the equaty will give you in 10 years 
--- SES_y10 = (SE/shares in 10 years) *   (%ROE = earnings / EQUITY  today ) 
UPDATE valuation_ROE SET Y10_net_income = SES_y10 *ROE;  
--ROE_REQUIRED_VALUE  =  the amount of money you will need to get Y10_net_income in 10 years.
UPDATE valuation_ROE SET ROE_REQUIRED_VALUE= Y10_net_income /MIN_RATE_RETURN ;

--NPV_ROE_REQUIRED_VALUE =ROE_REQUIRED_VALUE at today price  using MIN_RATE_RETURN
UPDATE valuation_ROE SET NPV_ROE_REQUIRED_VALUE= ROE_REQUIRED_VALUE/ (POWER (( 1 + MIN_RATE_RETURN ),10)); 


UPDATE valuation_ROE SET TOTAL_NPV_DIV= 	(NPV_DIV_Y1 
+	NPV_DIV_Y2
+	NPV_DIV_Y3
+	NPV_DIV_Y4
+	NPV_DIV_Y5
+	NPV_DIV_Y6
+	NPV_DIV_Y7
+	NPV_DIV_Y8
+	NPV_DIV_Y9
+	NPV_DIV_Y10);



UPDATE VALUATION_ROE SET valuation_ROE = NPV_ROE_REQUIRED_VALUE + TOTAL_NPV_DIV ;
UPDATE VALUATION_ROE SET valuation_ROE = null where valuation_ROE =0;

CREATE  INDEX VALUATION_R1_IX_1 ON VALUATION_R1  (TICKER);
CREATE  INDEX VALUATION_DCF_IX_1 ON VALUATION_DCF  (TICKER);
CREATE  INDEX valuation_ROE_IX_1 ON VALUATION_ROE  (TICKER);
   

/* *****************************************************************************************************
---VALUATIONS * ENDS * 
    *****************************************************************************************************
*/



--- VALUATION  MERGE 
UPDATE DF_KPI_S T1
SET 
valuation_R1	=	T2.	valuation_R1
,valuation_DCF	=	T2.	valuation_DCF
,valuation_ROE	=	T2.	valuation_ROE
,valuation_AVG=T2.valuation_AVG
,valuation_NUM = T2.valuation_NUM
FROM (

        SELECT 
        T1.TICKER 
        ,T1.valuation_R1
        ,T2.valuation_DCF
        ,T3.valuation_ROE
        ,	(CASE WHEN valuation_R1 IS NOT NULL THEN  1  ELSE 0 END 
            +CASE WHEN valuation_DCF IS NOT NULL THEN  1  ELSE 0 END 
            +CASE WHEN valuation_ROE IS NOT NULL THEN  1  ELSE 0 END ) AS valuation_NUM
            
        , case when  coalesce (  valuation_R1, valuation_R1 ,  valuation_ROE ) is null then null 
            else (COALESCE ( valuation_R1,0) + COALESCE (valuation_DCF,0) + COALESCE (valuation_ROE,0)  ) 
                                    / 
            (CASE WHEN valuation_R1  IS NULL THEN  0  ELSE 1 END 
            +CASE WHEN valuation_DCF IS NULL THEN  0  ELSE 1 END 
            +CASE WHEN valuation_ROE IS NULL THEN  0  ELSE 1 END ) end AS  valuation_AVG
            
            
        FROM VALUATION_R1 T1
        LEFT JOIN VALUATION_DCF T2 ON T1. TICKER =T2.TICKER
        LEFT JOIN VALUATION_ROE T3 ON T1. TICKER =T3.TICKER
        
  
) t2
WHERE  t1.TICKER=T2.TICKER ;

/*
******************************
******************************
-- PRICES  & PROFITS VS 
--valuation_R1_MOS  50%  valuation_R1_MOS
******************************
******************************
*/

UPDATE DF_KPI_S SET 
valuation_R1_MOS = valuation_R1/2;


--valuation_R1_VS_CURRENT 
UPDATE DF_KPI_S SET 
valuation_R1_VS_CURRENT = STIKER_PRICE /  NULLIF(valuation_R1,0)
,valuation_AVG_VS_CURRENT = STIKER_PRICE /  NULLIF(valuation_AVG,0);


UPDATE DF_KPI_S SET 
goodprice_r1_flag = CASE WHEN  valuation_R1_VS_CURRENT < .51 THEN 1 
                                            WHEN  valuation_R1_VS_CURRENT > 1 THEN -1 
                                            ELSE  0 END
,goodprice_avg_flag = CASE WHEN  valuation_AVG_VS_CURRENT < .51 THEN 1 
                                            WHEN  valuation_AVG_VS_CURRENT > 1 THEN -1 
                                            ELSE  0 END

                                            ;
--valuation_R1_1Y_EST
UPDATE DF_KPI_S  SET  valuation_R1_1Y_EST  =  valuation_R1 * (1 + R1_GROWTH_EST );  
--profit_r1_1y_est
UPDATE DF_KPI_S  SET  profit_r1_1y_est   = (valuation_R1_1Y_EST - STIKER_PRICE)  /  STIKER_PRICE  ;
--PROFIT_Y_1Y_EST
UPDATE DF_KPI_S  SET  PROFIT_Y_1Y_EST   = (Y_PRICE_1Y_EST - STIKER_PRICE)  /  STIKER_PRICE ;
--profit_combine_1y_est
UPDATE DF_KPI_S  SET profit_combine_1y_est  =   (profit_r1_1y_est +	PROFIT_Y_1Y_EST) / 2 ; 


 /*
---SCORE_TICKER 


*/

--RESET 
UPDATE DF_KPI_S  SET 
SCORE_TICKER = 0
,S_PNT_FUNDAMENTALS= 0
,S_PNT_PROFIT_PROJECTION= 0
,S_PNT_MARKET_CONSENSUS= 0
,S_PNT_OTHERS_KPI= 0
,S_PNT_Y_PROFIT = 0
,S_PNT_VALUATION_PROFIT = 0
,S_PNT_BIG_FIVE  = 0
,S_PNT_PRICE = 0 
,S_PNT_DEBT = 0 --not in use 
,S_PNT_SECTOR = 0
,S_PNT_MISS_DATA = 0
,S_PNT_Z_RANK = 0
,S_PNT_GROWTH = 0; 

-----------------------------
--      a) S_PNT_FUNDAMENTALS  max 500
-----------------------------
--S_PNT_BIG_FIVE max 400
-----------------------------
UPDATE DF_KPI_S  SET   S_PNT_BIG_FIVE  = 4 WHERE big5_total = 5;
UPDATE DF_KPI_S  SET   S_PNT_BIG_FIVE  = 3 WHERE big5_total = 4;
UPDATE DF_KPI_S  SET   S_PNT_BIG_FIVE  = S_PNT_BIG_FIVE +.5 
WHERE big5_total = 4 
and big5_fail_value >.05
and big5_fail like '%roic%';



-----------------------------
--S_PNT_DEBT   max 100
-----------------------------
--long term max 70
UPDATE DF_KPI_S  SET  S_PNT_DEBT = .70 where DEBT_LT_RATIO <1;
UPDATE DF_KPI_S  SET  S_PNT_DEBT = -.70 where DEBT_LT_RATIO > 2;
UPDATE DF_KPI_S  SET  S_PNT_DEBT = -.5 where DEBT_LT_RATIO between  1 and 1.99;

--short term  max 30 
UPDATE DF_KPI_S  SET  S_PNT_DEBT = S_PNT_DEBT + .30 where DEBT_ST_RATIO < 1.25;
UPDATE DF_KPI_S  SET  S_PNT_DEBT = S_PNT_DEBT + .15 where DEBT_ST_RATIO between  1.25 and 1.499;

UPDATE DF_KPI_S  SET  S_PNT_DEBT =  S_PNT_DEBT -.30 where DEBT_ST_RATIO > 2.5;
UPDATE DF_KPI_S  SET  S_PNT_DEBT = S_PNT_DEBT -.25 where DEBT_ST_RATIO between  2 and 2.49;



-----------------------------
--          b)S_PNT_PROFIT_PROJECTION 250
-----------------------------
-----------------------------
--R1 CALC S_PNT_PRICE RULES  max 100
-----------------------------
UPDATE DF_KPI_S  SET   S_PNT_PRICE  =  .5 WHERE  valuation_R1_VS_CURRENT BETWEEN   0 and .5;
UPDATE DF_KPI_S  SET   S_PNT_PRICE  =  .25  WHERE  valuation_R1_VS_CURRENT BETWEEN  .501 AND .75;
UPDATE DF_KPI_S  SET   S_PNT_PRICE  = - 1 WHERE  valuation_R1_VS_CURRENT >1;
UPDATE DF_KPI_S  SET   S_PNT_PRICE  = S_PNT_PRICE + .5 where  goodprice_avg_flag= 1;
-----------------------------
--S_PNT_VALUATION_PROFIT max 50
-----------------------------
UPDATE DF_KPI_S  SET   S_PNT_VALUATION_PROFIT  = .50 WHERE profit_r1_1y_est> .25001;
UPDATE DF_KPI_S  SET   S_PNT_VALUATION_PROFIT  = .25  WHERE profit_r1_1y_est  BETWEEN  .15 AND .25;
UPDATE DF_KPI_S  SET   S_PNT_VALUATION_PROFIT  = -.25 WHERE profit_r1_1y_est between .001 and .10;
UPDATE DF_KPI_S  SET   S_PNT_VALUATION_PROFIT  = -1 WHERE profit_r1_1y_est< 0;
-----------------------------
--S_PNT_GROWTH MAX 100
-----------------------------
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH +.25 WHERE Y_GROWTH_EST >0 AND Z_GROWTH_EST > 0  AND HISTORICAL_GROWTH > 0;


UPDATE DF_KPI_S  SET   S_PNT_GROWTH  = S_PNT_GROWTH + .10 WHERE Y_GROWTH_EST >= .10 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH + .10 WHERE Z_GROWTH_EST >= .10 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH +.10 WHERE HISTORICAL_GROWTH >= .10 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  = S_PNT_GROWTH + .15 WHERE Y_GROWTH_EST >= .15 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH + .15 WHERE Z_GROWTH_EST >= .15 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH +.15 WHERE HISTORICAL_GROWTH >= .15 ;



UPDATE DF_KPI_S  SET   S_PNT_GROWTH  = S_PNT_GROWTH - .25 WHERE Y_GROWTH_EST <= 0 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH - .25 WHERE Z_GROWTH_EST <= 0 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH  =  S_PNT_GROWTH - .25 WHERE HISTORICAL_GROWTH <= 0 ;
UPDATE DF_KPI_S  SET   S_PNT_GROWTH    =  S_PNT_GROWTH   - .25 WHERE R1_GROWTH_EST <0;

-----------------------------
--      c)S_PNT_MARKET_CONSENSUS 250%
-----------------------------
-----------------------------
--YAHOO PROFIT CALC PRICE RULES  max 100
-----------------------------
UPDATE DF_KPI_S  SET   S_PNT_Y_PROFIT   = 1 WHERE PROFIT_Y_1Y_EST > .2501;
UPDATE DF_KPI_S  SET   S_PNT_Y_PROFIT   = .75 WHERE PROFIT_Y_1Y_EST BETWEEN  .150001 AND .25;
UPDATE DF_KPI_S  SET   S_PNT_Y_PROFIT   = .5 WHERE PROFIT_Y_1Y_EST BETWEEN  .10001 AND .15;
UPDATE DF_KPI_S  SET   S_PNT_Y_PROFIT   = .25 WHERE PROFIT_Y_1Y_EST BETWEEN  .0001 AND .10;
UPDATE DF_KPI_S  SET   S_PNT_Y_PROFIT   = - 1 WHERE PROFIT_Y_1Y_EST < 0;
-- S_PNT_Z_RANK  max 150
UPDATE DF_KPI_S  SET   S_PNT_Z_RANK   =  1.50 WHERE UPPER(Z_RANK)  IN ('1-STRONG');
UPDATE DF_KPI_S  SET   S_PNT_Z_RANK  = 1 WHERE UPPER(Z_RANK)  IN ( '2-BUY');
UPDATE DF_KPI_S  SET   S_PNT_Z_RANK  =  .5  WHERE UPPER(Z_RANK)  IN ('3-HOLD');
UPDATE DF_KPI_S  SET   S_PNT_Z_RANK  =  - .5  WHERE UPPER(Z_RANK)  IN ('4-SELL') ;
UPDATE DF_KPI_S  SET   S_PNT_Z_RANK  =  - 2   WHERE UPPER(Z_RANK)  IN ('5-STRONG') ;



-----------------------------
--d)Others KPI
-----------------------------
--MISSING DATA AND FAILS  Max  none 
UPDATE DF_KPI_S  SET   S_PNT_MISS_DATA    =  S_PNT_MISS_DATA   - .5 WHERE valuation_dcf IS NULL ;
UPDATE DF_KPI_S  SET   S_PNT_MISS_DATA    =  S_PNT_MISS_DATA   - .5 WHERE valuation_roe IS NULL ;
UPDATE DF_KPI_S  SET   S_PNT_MISS_DATA    =  S_PNT_MISS_DATA   - 1 WHERE valuation_r1 IS NULL ;
UPDATE DF_KPI_S  SET   S_PNT_MISS_DATA   = S_PNT_MISS_DATA   - .5 WHERE BS_FLAG IS NULL  ;
UPDATE DF_KPI_S  SET   S_PNT_MISS_DATA  =  S_PNT_MISS_DATA - .5   WHERE  UPPER(Z_RANK) is null ;
--PENALISISNG SOME S_PNT_SECTORS 
UPDATE DF_KPI_S  SET   S_PNT_SECTOR    =  - 1 WHERE UPPER(Z_SECTOR_CLASS) LIKE '%BANK%';


--GROUPING 
--Trying to get  a balnce  beteeew  Past 50 % / Future 30% / Present-Market Consensus  +20% / Others like missing Data -10%

UPDATE DF_KPI_S  SET
    S_PNT_FUNDAMENTALS =S_PNT_BIG_FIVE + S_PNT_DEBT
    ,S_PNT_PROFIT_PROJECTION =S_PNT_PRICE +  S_PNT_VALUATION_PROFIT + S_PNT_GROWTH
    ,S_PNT_MARKET_CONSENSUS = S_PNT_Y_PROFIT + S_PNT_Z_RANK
    ,S_PNT_OTHERS_KPI = S_PNT_MISS_DATA + S_PNT_SECTOR;



--SCORE_TICKER
UPDATE DF_KPI_S  SET SCORE_TICKER =  S_PNT_FUNDAMENTALS + S_PNT_PROFIT_PROJECTION +  S_PNT_MARKET_CONSENSUS + S_PNT_OTHERS_KPI;



--OUTPUT TABLE 
DROP TABLE IF EXISTS DF_KPI;
CREATE TABLE DF_KPI AS  
SELECT 
    TICKER
    ,	SCORE_TICKER
    ,	NAME
    ,	 STIKER_PRICE 
    ,	 Y_PRICE_1Y_EST 
    ,	Z_RANK
    ,	R1_GROWTH_EST
    ,	GOODPRICE_R1_FLAG
    ,	GOODPRICE_AVG_FLAG
    ,	valuation_R1
    ,	PROFIT_R1_1Y_EST
    ,	PROFIT_Y_1Y_EST
    ,	PROFIT_COMBINE_1Y_EST
    ,	BIG5_TOTAL
    ,	BIG5_FAIL
    ,	BS_FLAG
    ,	valuation_R1_VS_CURRENT
    ,	valuation_R1_MOS
    ,	Y_GROWTH_EST
    ,	Z_GROWTH_EST
    ,	HISTORICAL_GROWTH
    ,	PROFOLIO_FLAG
    ,	Z_SECTOR_CLASS
    ,	Z_INDUSTRY_RANK
    ,	MARKET_CAP
    ,	AVG_VOLUME
    ,	DEBT_LT_RATIO
    ,	DEBT_ST_RATIO
    ,S_PNT_FUNDAMENTALS 
    ,S_PNT_PROFIT_PROJECTION 
    ,S_PNT_MARKET_CONSENSUS 
    , S_PNT_OTHERS_KPI
    ,	STIKER_PRICE_DATE

FROM DF_KPI_S 
WHERE valuation_R1 IS NOT NULL  ;

DROP TABLE IF EXISTS DF_PORFOLIO_VIEW;
CREATE TABLE DF_PORFOLIO_VIEW AS  
SELECT 

    T1.ID,
    CASE WHEN  T1.ID LIKE '%S%' THEN  'SELL' ELSE 'BUY' END AS  POSITION_TYPE,
    T1.TICKER,
    T2.NAME,
    t2.Z_RANK,
    T1.SHARES,
    T1.DATE_ as TRANSACCION_DATE ,
    T1.ACTIVE_FLAG,
    (T1.BUY_PRICE) * T1.SHARES::DOUBLE PRECISION AS INVESMENT,
    T1.BUY_PRICE,
    T2.STIKER_PRICE,
    (T2.STIKER_PRICE::DOUBLE PRECISION - T1.BUY_PRICE) * T1.SHARES::DOUBLE PRECISION AS PROFIT,
    ((T2.STIKER_PRICE::DOUBLE PRECISION - T1.BUY_PRICE) * T1.SHARES::DOUBLE PRECISION / T1.USD_AMOUNT ::DOUBLE PRECISION)::NUMERIC(10,6) AS PROFIT_PERC,
    T2.BS_FLAG,
    T2.SCORE_TICKER,
    T2.STIKER_PRICE_DATE
FROM df_porfolio_imp  T1
LEFT JOIN DF_KPI T2 ON T1.TICKER = T2.TICKER;
  
  

/*
Create historic Table 
*/
-- 

 -- DROP TABLE DF_KPI_S_H;
 -- CREATE TABLE  DF_KPI_S_H AS SELECT T1.*,  CURRENT_DATE AS LOAD_DATE  FROM DF_KPI_S T1;

--DELETE FROM  DF_KPI_S_H where STIKER_PRICE_DATE = STIKER_PRICE_DATE; --- DELETE IF THERE  DATA WITH SAME PRICE DATEEXTRACTED
--INSERT INTO DF_KPI_S_H  SELECT T1.*,  CURRENT_DATE AS LOAD_DATE  FROM DF_KPI_S T1;

-- Index created 

-- CREATE  INDEX DF_KPI_S_H_IX_2 ON DF_KPI_S_H  (TICKER);  
-- CREATE  INDEX DF_KPI_S_H_IX_3 ON DF_KPI_S_H  (TICKER , LOAD_DATE desc  );  
 

-- CREATE  INDEX DF_KPI_IX_2 ON DF_KPI  (TICKER);  
-- CREATE  INDEX DF_KPI_IX_3 ON DF_KPI  (TICKER , LOAD_DATE desc  );  



--output 
--select *  from DF_KPI;
 
END
$procedure$

