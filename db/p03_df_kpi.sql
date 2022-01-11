CREATE OR REPLACE PROCEDURE p03_df_kpi ( MAX_DATE DATE  DEFAULT CURRENT_DATE )
LANGUAGE plpgsql
AS $procedure$
BEGIN

/*
--#CREATE KPI TABLE
*/

DROP TABLE IF EXISTS DF_KPI_S;
CREATE TABLE DF_KPI_S AS (

            SELECT 
            T1.	TICKER
        ,	T1.	BIG5_TOTAL
        ,	T1.	ROIC
        ,	T1.	REVENUE_GW
        ,	T1.	EARNINGS_GW
        ,	T1.	EQUITY_GW
        ,	T1.	FREECASHFLOW_GW
        ,	T1.	BIG5_FAIL
        ,   T1.BIG5_FAIL_VALUE
        ,	T1.BIG5_FAIL_NAME
        ,	T1.EQUITY_GW AS 	HISTORICAL_GROWTH
        ,	t2.	earnings* 1000000 AS ly_earnings
        ,	t2.	freecashflow* 1000000 AS ly_freecashflow
        ,	t2.	cashandcashequivalents* 1000000 AS ly_cashandcashequivalents
        ,	t2.	totalliab* 1000000 AS ly_totalliab
        ,	t2.	totalstockholderequity* 1000000 AS ly_totalstockholderequity
        ,	t2.	debt_lt_ratio AS ly_debt_lt_ratio
        ,	t2.	debt_st_ratio AS ly_debt_st_ratio
        ,T2.YEARS_CNT
        FROM DF_FUNDAMENTALS  T1
        LEFT JOIN DF_FUNDAMENTALS  T2 ON T1.TICKER =  T2.TICKER and t2.LASTEST_YEAR_FLAG= 1
        WHERE    T1.TYPE= 'AVERAGE'
        AND    T1.KPI_FLAG > 0 ) ;                 

                 
     
ALTER TABLE DF_KPI_S  
    --Yahoo 
    ADD Y_PRICE_1Y_EST	DECIMAL (16,6)
    ,ADD EPS	DECIMAL (16,6)
    ,ADD PE_RATIO	DECIMAL (16,6)
    ,ADD MARKET_CAP	VARCHAR(20)
    ,ADD MARKET_EXC	VARCHAR(20)
    ,ADD COUNTRY VARCHAR(20)

    ,ADD AVG_VOLUME	DECIMAL (15,2)
    ,ADD STIKER_PRICE	DECIMAL (16,6)
    ,add DIFF_PRICE_PRC DECIMAL (16,6)
    ,ADD Y_GROWTH_EST	DECIMAL (16,6)
    ,ADD NAME VARCHAR (200)

 
    ,ADD PROFOLIO_FLAG INT DEFAULT 0
    --BS
    ,ADD BS_FLAG VARCHAR(20)
    ,ADD BS_LAST_STRONG_DAYS_SINCE INT 
    ,ADD  BS_LAST_STRONG VARCHAR(20)
    , ADD ALL_BS_SUM INT
    , ADD ALL_BS_SUM_DB  INT

    , ADD bs_flag_trend_days_in INT 

    ,ADD STIKER_PRICE_DATE DATE
    ,ADD SS_SHARES_OUTSTANDING NUMERIC
    ,ADD DS_TRAILING_ANNUAL_DIV_RATE DECIMAL (12,6)
    ,ADD DROP_UPS_DECILE INT DEFAULT 0 
    --zacks
    ,ADD Z_EXP_GROWTH DECIMAL (16,6)
    ,ADD Z_SECTOR_CLASS VARCHAR(100)
    ,ADD Z_INDUSTRY_RANK VARCHAR(50)

    --RECOMENDATION YAHOO  AND ZACKS COMBINED
    ,ADD REC_RANK VARCHAR(20)
    ,ADD REC_RANK_NUM INT 
    ,ADD REC_VAR_ACTUAL	INT DEFAULT 0
    ,ADD REC_VAR_ACTUAL_DAYSDIFF 	INT  DEFAULT 0
    ,ADD REC_RANK_IND TEXT 

    --Calcs
    ,ADD GOODPRICE_R1_FLAG INT
    ,ADD GOODPRICE_AVG_FLAG INT

    ,ADD R1_GROWTH_EST	DECIMAL (16,6)
    ,ADD MIN_RATE_RETURN 	DECIMAL (16,6)

    ,ADD PROFIT_Y_1Y_EST	DECIMAL (16,6)
    ,ADD PROFIT_R1_1Y_EST	DECIMAL (16,6)
    ,ADD PROFIT_COMBINE_1Y_EST DECIMAL (16,6) DEFAULT 0

    -- Score Valuetion 
    ,ADD S_VA_PNT_BIG_FIVE DECIMAL (10,2)
    ,ADD S_VA_PNT_PRICE  DECIMAL (10,2)
    ,ADD S_VA_PNT_VALUATION_PROFIT  DECIMAL (10,2)
    ,ADD S_VA_PNT_GROWTH   DECIMAL (10,2)
    ,ADD S_VA_PNT_Y_PROFIT  DECIMAL (10,2)
    ,ADD S_VA_PNT_Z_PROFIT DECIMAL (10,2)
    ,ADD S_VA_PNT_MISS_DATA  DECIMAL (10,2)
    ,ADD S_VA_PNT_SECTOR  DECIMAL (10,2)
    ,ADD S_VA_PNT_DEBT  DECIMAL (10,2)
    ,ADD S_VA_PNT_FUNDAMENTALS DECIMAL (10,2)
    ,ADD S_VA_PNT_PROFIT_PROJECTION DECIMAL (10,2)
    ,ADD S_VA_PNT_MARKET_CONSENSUS DECIMAL (10,2)
    ,ADD S_VA_PNT_OTHERS_KPI DECIMAL (10,2)
    ,ADD SCORE_VALUATION DECIMAL (10,2)

    -- Score Momentum
    ,ADD S_MO_PNT_RR DECIMAL (10,2) DEFAULT 0
    ,ADD S_MO_PNT_RR_P1 DECIMAL (10,2) DEFAULT 0
    ,ADD S_MO_PNT_RR_P2 DECIMAL (10,2) DEFAULT 0
    ,ADD S_MO_PNT_Z_IND DECIMAL (10,2) DEFAULT 0

    ,ADD S_MO_PNT_VALUATION DECIMAL (10,2) DEFAULT 0
    ,ADD S_MO_PNT_BS_FLAG DECIMAL (10,2) DEFAULT 0
    , add S_MO_PNT_BS_1 DECIMAL (10,2) DEFAULT 0
    , ADD S_MO_PNT_BS_2 DECIMAL (10,2) DEFAULT 0

    ,ADD S_MO_PNT_DROP_UPS_DECILE DECIMAL (10,2)DEFAULT 0
    ,ADD SCORE_MOMENTUM DECIMAL (10,2) DEFAULT 0
    ,ADD S_MO_ALL_DATA_FLAG INT DEFAULT 1

    --VALUATION
    ,ADD VALUATION_R1 DECIMAL (20,2)
    ,ADD VALUATION_DCF DECIMAL (20,2)
    ,ADD VALUATION_ROE DECIMAL (20,2)
    ,ADD VALUATION_AVG DECIMAL (20,2)
    ,ADD VALUATION_R1_1Y_EST DECIMAL (20,2)
    ,ADD VALUATION_NUM DECIMAL (10,2)
    ,ADD VALUATION_R1_MOS	DECIMAL (20,2)
    ,ADD VALUATION_R1_VS_CURRENT 	DECIMAL (16,6)
    ,ADD VALUATION_AVG_VS_CURRENT 	DECIMAL (16,6)

    ,add SCORE_TICKER DECIMAL (16,6)
    ,ADD TODAYS_PICKS INT DEFAULT 0
    ,add VOLUME_DECILE  DECIMAL (10,2)
    ,ADD ETORO_FLAG int default 0;


UPDATE DF_KPI_S T1 set  ETORO_FLAG =1
 where  TICKER in 
(select TICKER
from  df_etoro_tickers_imp
);

--- DF_PRICE_BS_REC
UPDATE DF_KPI_S T1
SET
ALL_BS_SUM	=	T2.	ALL_BS_SUM
,ALL_BS_SUM_DB	=	T2.	ALL_BS_SUM_DB
,BS_FLAG = T2.BS_FLAG
,STIKER_PRICE= T2.STIKER_PRICE
,STIKER_PRICE_DATE = T2.STIKER_PRICE_DATE
,DIFF_PRICE_PRC = T2.DIFF_PRICE_PRC
,bs_flag_trend_days_in	=	T2.	bs_flag_trend_days_in
,Z_EXP_GROWTH	=	t2.	Z_EXP_GROWTH
,Z_SECTOR_CLASS	=	t2.	Z_SECTOR_CLASS
,Z_INDUSTRY_RANK	=	T2.	Z_INDUSTRY_RANK
,REC_RANK =	T2.REC_RANK
,REC_RANK_NUM =	T2.REC_RANK_NUM
,REC_VAR_ACTUAL=	T2.REC_VAR_ACTUAL
,REC_VAR_ACTUAL_DAYSDIFF=	T2.REC_VAR_ACTUAL_DAYSDIFF
,REC_RANK_IND=	T2.REC_RANK_IND
,Y_PRICE_1Y_EST	=	T2.Y_PRICE_1Y_EST
,EPS	=	T2.	EPS
,PE_RATIO	=	T2.	PE_RATIO
,MARKET_CAP	=	T2.	MARKET_CAP
,AVG_VOLUME	=	T2.	AVG_VOLUME
,Y_GROWTH_EST	=	T2.	Y_GROWTH_EST
,SS_SHARES_OUTSTANDING	=	T2.	SS_SHARES_OUTSTANDING
,DS_TRAILING_ANNUAL_DIV_RATE	=	T2.	DS_TRAILING_ANNUAL_DIV_RATE

FROM (
    SELECT 
            TICKER
            ,ALL_BS_SUM
            ,ALL_BS_SUM_DB
            ,BS_FLAG 
            ,CLOSE_ADJ_PRICE AS STIKER_PRICE
            ,PRICE_DATE AS STIKER_PRICE_DATE 
            ,DIFF_PRICE_PRC 
            ,bs_flag_trend_days_in
            ,Z_EXP_GROWTH
            ,Z_SECTOR_CLASS
            ,Z_INDUSTRY_RANK
            ,Y_PRICE_1Y_EST
            ,REC_RANK 
            ,REC_RANK_NUM 
            ,REC_VAR_ACTUAL
            ,REC_VAR_ACTUAL_DAYSDIFF
            ,REC_RANK_IND
            , EPS	
            , PE_RATIO	
            , MARKET_CAP	
            , AVG_VOLUME	
            , Y_GROWTH_EST	
            , SS_SHARES_OUTSTANDING 
            , DS_TRAILING_ANNUAL_DIV_RATE
            ,LOAD_DATE
    FROM DF_PRICE_BS_REC

  )  AS T2 
  WHERE  T1.TICKER=T2.TICKER;

--TOP DROPS  AND UPS 

-- DROPS
UPDATE DF_KPI_S T1
SET
 	DROP_UPS_DECILE	=	T2.	DROP_UPS_DECILE *-1 
    ,VOLUME_DECILE=T2.VOLUME_DECILE
FROM (
    SELECT 
        TICKER
        ,T1.DIFF_PRICE_PRC 
        ,NTILE(10) OVER(    ORDER BY  T1.DIFF_PRICE_PRC  ASC )   AS DROP_UPS_DECILE
        ,NTILE(100) OVER(   ORDER BY  AVG_VOLUME  DESC )   AS VOLUME_DECILE
    FROM DF_KPI_S T1
    WHERE  T1.DIFF_PRICE_PRC  < 0 
)  AS T2 
  WHERE  T1.TICKER=T2.TICKER;


-- UPS
UPDATE DF_KPI_S T1
SET
 	DROP_UPS_DECILE	=	T2.	DROP_UPS_DECILE 
 ,VOLUME_DECILE=t2.VOLUME_DECILE
FROM (   SELECT 
            TICKER
            ,T1.DIFF_PRICE_PRC 
            ,NTILE(10) OVER(    ORDER BY  T1.DIFF_PRICE_PRC  desc )   AS DROP_UPS_DECILE
            ,NTILE(100) OVER(   ORDER BY  avg_volume  desc )   AS VOLUME_DECILE
            FROM DF_KPI_S T1
            WHERE  T1.DIFF_PRICE_PRC  >= 0 

  )  AS T2 
  WHERE  T1.TICKER=T2.TICKER;
  
-- NAMES FROM DF_H_TICKER_NAME
UPDATE DF_KPI_S T1
SET 
NAME= T2.NAME
,country=t2.country
,market_exc =  T2.market
,market_cap=t2.market_cap
,z_sector_class= case when z_sector_class is null 
                    then t2.sector || ': ' ||  t2.industry || '- NON ZACKS'
                        else z_sector_class end 

FROM  DF_H_TICKER_NAME AS T2 
WHERE  T1.TICKER=T2.TICKER;


--DF_PORFOLIO_IMP
UPDATE DF_KPI_S T1
SET PROFOLIO_FLAG =  1
WHERE TICKER IN (
        SELECT  TICKER 
        FROM DF_PORFOLIO);
                      
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
UPDATE DF_KPI_S SET Z_EXP_GROWTH= NULLIF( Z_EXP_GROWTH,0);
UPDATE DF_KPI_S SET Y_GROWTH_EST= NULLIF( Y_GROWTH_EST,0);

UPDATE DF_KPI_S SET R1_GROWTH_EST = LEAST( ( --AVERAGE OF ZACKS AND YAHOO - IF NULL GET THE 
                                                    (COALESCE (NULLIF( Z_EXP_GROWTH,0),Y_GROWTH_EST )  
                                                  + COALESCE (NULLIF( Y_GROWTH_EST,0) , Z_EXP_GROWTH ))/2
                                                  )
                                                , NULLIF( HISTORICAL_GROWTH,0) );

--WHERE ZACKS  AND  YAHOO EXPECTED GROWTH NOT FOUND BRING HISTORIC GROWTH AT 75%
UPDATE DF_KPI_S SET R1_GROWTH_EST = HISTORICAL_GROWTH * .75 WHERE NULLIF( Y_GROWTH_EST,0) =0 AND NULLIF( Z_EXP_GROWTH,0) =0 ; 

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



---   MERGE  IN MISSING  EPS  or less than  0 
UPDATE VALUATION_R1 T1 SET 
EPS= CASE  WHEN T1.EPS IS NULL  THEN   T2.EPS
            WHEN T1.EPS  <=0  THEN GREATEST(T1.EPS, T2.EPS)
            ELSE  T1.EPS  END 
                   
    FROM (
       SELECT  
        T1.TICKER
         ,T1.LY_EARNINGS 
         ,T1.SS_SHARES_OUTSTANDING
         , CASE WHEN (T1.SS_SHARES_OUTSTANDING IS NULL OR T1.SS_SHARES_OUTSTANDING = 0) THEN NULL ELSE (T1.LY_EARNINGS) / T1.SS_SHARES_OUTSTANDING END AS EPS
        FROM DF_KPI_S  AS T1 
   ) t2
    WHERE  t1.TICKER=T2.TICKER;


---r1_future_per
UPDATE VALUATION_R1 SET 
r1_future_per = CASE WHEN  PER_H > 0 THEN LEAST( PER_H , per_default)
                                         ELSE  per_default END ;                                       
--CALC_EPS_10_YEARS - 10 YEARS EXP PRICE EXPECTED MIN PROFIT IN INVERSIONS  
UPDATE VALUATION_R1 SET CALC_EPS_10_YEARS = EPS * (POWER((1+R1_GROWTH_EST),10 ) ) where EPS > 0;
UPDATE VALUATION_R1 SET CALC_FUTURE_STROCK_PRICE= GREATEST ((CALC_EPS_10_YEARS * r1_future_per), 0);
--# 15% EXPECTED MIN PROFIT IN INVERSIONS AND 10 YEARS
UPDATE VALUATION_R1 SET  valuation_R1  = CALC_FUTURE_STROCK_PRICE * (1 / (POWER ((1+MIN_RATE_RETURN),10)));
--JA NOTE: Most of this  0 are  Tickers with  either EPS or r1_future_per  is negative value 
UPDATE VALUATION_R1 SET valuation_R1 = NULL WHERE valuation_R1 =0;


---2.- DCF VALUATION		
--FUNDAMENTAL ARE IN MILLION UNITS 
DROP TABLE VALUATION_DCF; 
CREATE TABLE VALUATION_DCF AS 
SELECT distinct
    t1.ticker
     , T1.ly_cashandcashequivalents::REAL  as  cashandcashequivalents
    ,T1.ly_totalliab::REAL  as  totalliab
    ,T1.ly_freecashflow::REAL  as  freecashflow
    ,t1.ss_shares_outstanding
    ,t1. R1_GROWTH_EST
     , .0 as Growth_decline_rate
    , T1.MIN_RATE_RETURN

    , 12 as Y10_FCF_multiplier
    ,cast (0 as Numeric) as FCF_y1
    ,cast (0 as Numeric) as FCF_y2
    ,cast (0 as Numeric) as FCF_y3
    ,cast (0 as Numeric) as FCF_y4
    ,cast (0 as Numeric) as FCF_y5
    ,cast (0 as Numeric) as FCF_y6
    ,cast (0 as Numeric) as FCF_y7
    ,cast (0 as Numeric) as FCF_y8
    ,cast (0 as Numeric) as FCF_y9
    ,cast (0 as Numeric) as FCF_y10

    ,cast (0 as Numeric) as NPV_FCF_y1
    ,cast (0 as Numeric) as NPV_FCF_y2
    ,cast (0 as Numeric) as NPV_FCF_y3
    ,cast (0 as Numeric) as NPV_FCF_y4
    ,cast (0 as Numeric) as NPV_FCF_y5
    ,cast (0 as Numeric) as NPV_FCF_y6
    ,cast (0 as Numeric) as NPV_FCF_y7
    ,cast (0 as Numeric) as NPV_FCF_y8
    ,cast (0 as Numeric) as NPV_FCF_y9
    ,cast (0 as Numeric) as NPV_FCF_y10

    , CAST (0 AS Numeric) AS  TOTAL_NPV_FCF
    , CAST (0 AS Numeric) AS  Y10_FCF_VALUE
    , CAST (0 AS Numeric) AS  COMPANY_VALUE
    , CAST (0 AS Numeric) AS  valuation_DCF
            
FROM   DF_KPI_S AS T1  ;


UPDATE valuation_dcf SET FCF_y1  = FREECASHFLOW*(1+R1_GROWTH_EST);
UPDATE valuation_dcf SET  FCF_y2	= (FCF_y1	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	1 ))))::REAL ;
UPDATE valuation_dcf SET  FCF_y3	=( FCF_y2	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	2 ))))::REAL ;
UPDATE valuation_dcf SET  FCF_y4	= (FCF_y3	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	3 ))))::REAL;
UPDATE valuation_dcf SET  FCF_y5	= (FCF_y4	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	4 ))))::REAL;
UPDATE valuation_dcf SET  FCF_y6	= (FCF_y5	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	5 ))))::REAL;
UPDATE valuation_dcf SET  FCF_y7	= (FCF_y6	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	6 ))))::REAL;
UPDATE valuation_dcf SET  FCF_y8	= (FCF_y7	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	7 ))))::REAL;
UPDATE valuation_dcf SET  FCF_y9	= (FCF_y8	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	8 ))))::REAL;
UPDATE valuation_dcf SET  FCF_y10	= (FCF_y9	* (1 +( R1_GROWTH_EST * power( (1- Growth_decline_rate), 	9 ))))::REAL;


UPDATE valuation_dcf SET NPV_FCF_Y1 = (FCF_Y1 /  POWER(  (1 + MIN_RATE_RETURN ), 1 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y2 = (FCF_Y2 /  POWER(  (1 + MIN_RATE_RETURN ), 2 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y3 = (FCF_Y3 /  POWER(  (1 + MIN_RATE_RETURN ), 3 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y4 = (FCF_Y4 /  POWER(  (1 + MIN_RATE_RETURN ), 4 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y5 = (FCF_Y5 /  POWER(  (1 + MIN_RATE_RETURN ), 5 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y6 = (FCF_Y6 /  POWER(  (1 + MIN_RATE_RETURN ), 6 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y7 = (FCF_Y7 /  POWER(  (1 + MIN_RATE_RETURN ), 7 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y8 = (FCF_Y8 /  POWER(  (1 + MIN_RATE_RETURN ), 8 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y9 = (FCF_Y9 /  POWER(  (1 + MIN_RATE_RETURN ), 9 ))::REAL;
UPDATE valuation_dcf SET NPV_FCF_Y10 = (FCF_Y10 /  POWER(  (1 + MIN_RATE_RETURN ), 10 ))::REAL;


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
UPDATE valuation_dcf SET valuation_DCF = (case when  SS_SHARES_OUTSTANDING = 0 then null else COMPANY_VALUE / SS_SHARES_OUTSTANDING end);
UPDATE valuation_dcf SET valuation_DCF = valuation_DCF::REAL;
UPDATE valuation_dcf SET valuation_DCF = null where valuation_DCF =0;
                         

/*
---
3.-  valuation ROE
*/


DROP TABLE valuation_roe; 
CREATE TABLE valuation_roe AS 
SELECT  distinct 
    T1.TICKER
    ,T1.ly_TOTALSTOCKHOLDEREQUITY AS  SE -- TOTALSTOCKHOLDEREQUITY
    ,T1.ly_earnings AS    earnings
    , (T1.ly_earnings ) / ( NULLIF (T1.ly_TOTALSTOCKHOLDEREQUITY, 0 ) )  AS ROE  --- earnings / EQUITY
    ,T1.SS_SHARES_OUTSTANDING   
    ,COALESCE(T1.DS_TRAILING_ANNUAL_DIV_RATE,0) AS DIVIDEND_RATE_PER_SHARE
    ,T1.R1_GROWTH_EST
    ,T1.MIN_RATE_RETURN
    
    ,cast (0 as Numeric) as SES_y1
    ,cast (0 as Numeric) as SES_y2
    ,cast (0 as Numeric) as SES_y3
    ,cast (0 as Numeric) as SES_y4
    ,cast (0 as Numeric) as SES_y5
    ,cast (0 as Numeric) as SES_y6
    ,cast (0 as Numeric) as SES_y7
    ,cast (0 as Numeric) as SES_y8
    ,cast (0 as Numeric) as SES_y9
    ,cast (0 as Numeric) as SES_y10
    
     ,CAST (0 AS Numeric) AS DIV_Y1
    ,CAST (0 AS Numeric) AS DIV_Y2
    ,CAST (0 AS Numeric) AS DIV_Y3
    ,CAST (0 AS Numeric) AS DIV_Y4
    ,CAST (0 AS Numeric) AS DIV_Y5
    ,CAST (0 AS Numeric) AS DIV_Y6
    ,CAST (0 AS Numeric) AS DIV_Y7
    ,CAST (0 AS Numeric) AS DIV_Y8
    ,CAST (0 AS Numeric) AS DIV_Y9
    ,CAST (0 AS Numeric) AS DIV_Y10
    ,CAST (0 AS Numeric) AS NPV_DIV_Y1
    ,CAST (0 AS Numeric) AS NPV_DIV_Y2
    ,CAST (0 AS Numeric) AS NPV_DIV_Y3
    ,CAST (0 AS Numeric) AS NPV_DIV_Y4
    ,CAST (0 AS Numeric) AS NPV_DIV_Y5
    ,CAST (0 AS Numeric) AS NPV_DIV_Y6
    ,CAST (0 AS Numeric) AS NPV_DIV_Y7
    ,CAST (0 AS Numeric) AS NPV_DIV_Y8
    ,CAST (0 AS Numeric) AS NPV_DIV_Y9
    ,CAST (0 AS Numeric) AS NPV_DIV_Y10
    ,CAST (0 AS Numeric) AS   Y10_NET_INCOME
    ,CAST (0 AS Numeric) AS  ROE_REQUIRED_VALUE
    ,CAST (0 AS Numeric) AS NPV_ROE_REQUIRED_VALUE
    ,CAST (0 AS Numeric) AS TOTAL_NPV_DIV
    ,CAST (0 AS decimal(30,2) ) AS VALUATION_ROE


FROM   DF_KPI_S AS T1  ;


--SES_* = Share HOLDER EQUITY  per share 
UPDATE valuation_ROE SET SES_y1 = case when SS_SHARES_OUTSTANDING =0 then null else  (SE*(1+R1_GROWTH_EST)) / SS_SHARES_OUTSTANDING end ; 
UPDATE valuation_ROE SET SES_y2 = (SES_y1*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y3 = (SES_y2*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y4 = (SES_y3*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y5 = (SES_y4*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y6 = (SES_y5*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y7 = (SES_y6*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y8 = (SES_y7*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y9 = (SES_y8*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET SES_y10 = (SES_y9*(1+R1_GROWTH_EST))::REAL; 

--DIV_*
UPDATE valuation_ROE SET DIV_y1 = (DIVIDEND_RATE_PER_SHARE*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y2 = (DIV_y1*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y3 = (DIV_y2*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y4 = (DIV_y3*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y5 = (DIV_y4*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y6 = (DIV_y5*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y7 = (DIV_y6*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y8 = (DIV_y7*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y9 = (DIV_y8*(1+R1_GROWTH_EST))::REAL; 
UPDATE valuation_ROE SET DIV_y10 = (DIV_y9*(1+R1_GROWTH_EST))::REAL; 
 
--NPV_DIV_*
UPDATE valuation_ROE SET npv_DIV_y1 = (DIV_y1/ (POWER (( 1 + MIN_RATE_RETURN ),0)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y2 = (DIV_y2/ (POWER (( 1 + MIN_RATE_RETURN ),1)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y3 = (DIV_y3/ (POWER (( 1 + MIN_RATE_RETURN ),2)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y4 = (DIV_y4/ (POWER (( 1 + MIN_RATE_RETURN ),3)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y5 = (DIV_y5/ (POWER (( 1 + MIN_RATE_RETURN ),4)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y6 = (DIV_y6/ (POWER (( 1 + MIN_RATE_RETURN ),5)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y7 = (DIV_y7/ (POWER (( 1 + MIN_RATE_RETURN ),6)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y8 = (DIV_y8/ (POWER (( 1 + MIN_RATE_RETURN ),7)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y9 = (DIV_y9/ (POWER (( 1 + MIN_RATE_RETURN ),8)))::REAL; 
UPDATE valuation_ROE SET npv_DIV_y10 = (DIV_y10/ (POWER (( 1 + MIN_RATE_RETURN ),9)))::REAL; 

--Y10_net_income = eaning the equaty will give you in 10 years 
--- SES_y10 = (SE/shares in 10 years) *   (%ROE = earnings / EQUITY  today ) 
UPDATE valuation_ROE SET Y10_net_income = SES_y10 *ROE;  
--ROE_REQUIRED_VALUE  =  the amount of money you will need to get Y10_net_income in 10 years.
UPDATE valuation_ROE SET ROE_REQUIRED_VALUE= Y10_net_income /MIN_RATE_RETURN ;

--NPV_ROE_REQUIRED_VALUE =ROE_REQUIRED_VALUE at today price  using MIN_RATE_RETURN
UPDATE valuation_ROE SET NPV_ROE_REQUIRED_VALUE= (ROE_REQUIRED_VALUE/ (POWER (( 1 + MIN_RATE_RETURN ),10)))::REAL; 

UPDATE valuation_ROE SET TOTAL_NPV_DIV= 	(NPV_DIV_Y1 
+	NPV_DIV_Y2
+	NPV_DIV_Y3
+	NPV_DIV_Y4
+	NPV_DIV_Y5
+	NPV_DIV_Y6
+	NPV_DIV_Y7
+	NPV_DIV_Y8
+	NPV_DIV_Y9
+	NPV_DIV_Y10)::REAL ;



UPDATE VALUATION_ROE SET valuation_ROE = (NPV_ROE_REQUIRED_VALUE + TOTAL_NPV_DIV );
UPDATE VALUATION_ROE SET valuation_ROE =valuation_ROE::REAL ;
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
        ,T2.valuation_DCF as valuation_DCF
        ,T3.valuation_ROE as valuation_ROE
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
-- PRICES  & PROFITS VS 
--VALUATION_R1_MOS  50%  VALUATION_R1_MOS
*/
UPDATE DF_KPI_S SET  VALUATION_R1_MOS = VALUATION_R1/2;


--VALUATION_R1_VS_CURRENT 
UPDATE DF_KPI_S SET 
VALUATION_R1_VS_CURRENT = STIKER_PRICE /  NULLIF(VALUATION_R1,0)
,VALUATION_AVG_VS_CURRENT = STIKER_PRICE /  NULLIF(VALUATION_AVG,0);

--GOODPRICE_R1_FLAG  &  GOODPRICE_AVG_FLAG
UPDATE DF_KPI_S SET 
GOODPRICE_R1_FLAG = CASE WHEN  VALUATION_R1_VS_CURRENT < .51 THEN 1 
                                            WHEN  VALUATION_R1_VS_CURRENT > 1 THEN -1 
                                            ELSE  0 END
,GOODPRICE_AVG_FLAG = CASE WHEN  VALUATION_AVG_VS_CURRENT < .51 THEN 1 
                                            WHEN  VALUATION_AVG_VS_CURRENT > 1 THEN -1 
                                            ELSE  0 END;
--VALUATION_R1_1Y_EST
UPDATE DF_KPI_S  SET  VALUATION_R1_1Y_EST  =  VALUATION_R1 * (1 + R1_GROWTH_EST );  
--PROFIT_R1_1Y_EST
UPDATE DF_KPI_S  SET  PROFIT_R1_1Y_EST   = (VALUATION_R1_1Y_EST - STIKER_PRICE)  /  STIKER_PRICE  ;
--PROFIT_Y_1Y_EST
UPDATE DF_KPI_S  SET  PROFIT_Y_1Y_EST   = (Y_PRICE_1Y_EST - STIKER_PRICE)  /  STIKER_PRICE ;
--PROFIT_COMBINE_1Y_EST
UPDATE DF_KPI_S  SET PROFIT_COMBINE_1Y_EST  =   (PROFIT_R1_1Y_EST +	PROFIT_Y_1Y_EST) / 2 ; 

/*
STARTS  SCORE_VALUATION
*/

--RESET 
UPDATE DF_KPI_S  SET 
SCORE_VALUATION = 0
,S_VA_PNT_FUNDAMENTALS= 0
,S_VA_PNT_PROFIT_PROJECTION= 0
,S_VA_PNT_MARKET_CONSENSUS= 0
,S_VA_PNT_OTHERS_KPI= 0
,S_VA_PNT_Y_PROFIT = 0
,S_VA_PNT_VALUATION_PROFIT = 0
,S_VA_PNT_BIG_FIVE  = 0
,S_VA_PNT_PRICE = 0 
,S_VA_PNT_DEBT = 0 
,S_VA_PNT_SECTOR = 0
,S_VA_PNT_MISS_DATA = 0
,S_VA_PNT_Z_PROFIT = 0
,S_VA_PNT_GROWTH = 0; 

-----------------------------
--      a) S_VA_PNT_FUNDAMENTALS  max 500
-----------------------------
--S_VA_PNT_BIG_FIVE max 400
-----------------------------
UPDATE DF_KPI_S  SET   S_VA_PNT_BIG_FIVE  = 4 WHERE BIG5_TOTAL = 5;
UPDATE DF_KPI_S  SET   S_VA_PNT_BIG_FIVE  = 3 WHERE BIG5_TOTAL = 4;


--GIVING EXTRA TO MID OK 
UPDATE DF_KPI_S  
SET   S_VA_PNT_BIG_FIVE  = S_VA_PNT_BIG_FIVE +.5 
WHERE BIG5_TOTAL = 4 
AND BIG5_FAIL_VALUE >.05;


--PENALISING  NEGATIVE FIGURES IN THE BIG
UPDATE DF_KPI_S  
SET   S_VA_PNT_BIG_FIVE  = CASE WHEN BIG5_FAIL_VALUE < -.05  THEN S_VA_PNT_BIG_FIVE -1 
                                        ELSE  S_VA_PNT_BIG_FIVE - .5  END 
WHERE BIG5_TOTAL = 4 
AND BIG5_FAIL_VALUE <0.05;

-----------------------------
--S_VA_PNT_DEBT   max 100
-----------------------------
--LONG TERM MAX .70
UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT = .70 where LY_DEBT_LT_RATIO <1;
UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT = -.70 where LY_DEBT_LT_RATIO > 2;
UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT = -.5 where LY_DEBT_LT_RATIO between  1 and 1.99;

--SHORT TERM  MAX 30 
UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT = S_VA_PNT_DEBT + .30 where LY_DEBT_ST_RATIO < 1.25;
UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT = S_VA_PNT_DEBT + .15 where LY_DEBT_ST_RATIO between  1.25 and 1.499;

UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT =  S_VA_PNT_DEBT -.30 where LY_DEBT_ST_RATIO > 2.5;
UPDATE DF_KPI_S  SET  S_VA_PNT_DEBT = S_VA_PNT_DEBT -.25 where LY_DEBT_ST_RATIO between  2 and 2.49;

-----------------------------
--          b)S_VA_PNT_PROFIT_PROJECTION 250
-----------------------------
-----------------------------
--R1 CALC S_VA_PNT_PRICE RULES  max 100
-----------------------------
UPDATE DF_KPI_S  SET   S_VA_PNT_PRICE  =  .5 WHERE  VALUATION_R1_VS_CURRENT BETWEEN   0 AND .5;
UPDATE DF_KPI_S  SET   S_VA_PNT_PRICE  =  .25  WHERE  VALUATION_R1_VS_CURRENT BETWEEN  .501 AND .75;
UPDATE DF_KPI_S  SET   S_VA_PNT_PRICE  = - 1 WHERE  VALUATION_R1_VS_CURRENT >1;
UPDATE DF_KPI_S  SET   S_VA_PNT_PRICE  = S_VA_PNT_PRICE + .5 WHERE  GOODPRICE_AVG_FLAG= 1;
-----------------------------
--S_VA_PNT_VALUATION_PROFIT max 50
-----------------------------
UPDATE DF_KPI_S  SET   S_VA_PNT_VALUATION_PROFIT  = .50 WHERE PROFIT_R1_1Y_EST> .25001;
UPDATE DF_KPI_S  SET   S_VA_PNT_VALUATION_PROFIT  = .25  WHERE PROFIT_R1_1Y_EST  BETWEEN  .15 AND .25;
UPDATE DF_KPI_S  SET   S_VA_PNT_VALUATION_PROFIT  = -.25 WHERE PROFIT_R1_1Y_EST BETWEEN .001 AND .10;
UPDATE DF_KPI_S  SET   S_VA_PNT_VALUATION_PROFIT  = -1 WHERE PROFIT_R1_1Y_EST< 0;
-----------------------------
--S_VA_PNT_GROWTH MAX 100
-----------------------------
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH +.25 WHERE Y_GROWTH_EST >0 AND Z_EXP_GROWTH > 0  AND HISTORICAL_GROWTH > 0;


UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  = S_VA_PNT_GROWTH + .10 WHERE Y_GROWTH_EST >= .10 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH + .10 WHERE Z_EXP_GROWTH >= .10 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH +.10 WHERE HISTORICAL_GROWTH >= .10 ;

UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  = S_VA_PNT_GROWTH + .15 WHERE Y_GROWTH_EST >= .15 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH + .15 WHERE Z_EXP_GROWTH >= .15 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH +.15 WHERE HISTORICAL_GROWTH >= .15 ;
--NEGATIVE GROWTH 
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  = S_VA_PNT_GROWTH - .25 WHERE Y_GROWTH_EST <= 0 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH - .25 WHERE Z_EXP_GROWTH <= 0 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH  =  S_VA_PNT_GROWTH - .25 WHERE HISTORICAL_GROWTH <= 0 ;
UPDATE DF_KPI_S  SET   S_VA_PNT_GROWTH    =  S_VA_PNT_GROWTH   - .25 WHERE R1_GROWTH_EST <0;


-----------------------------
--      c)S_VA_PNT_MARKET_CONSENSUS 200%
-----------------------------
-----------------------------
--YAHOO PROFIT CALC PRICE RULES  max 100
-----------------------------
UPDATE DF_KPI_S  SET   S_VA_PNT_Y_PROFIT   = 1 WHERE PROFIT_Y_1Y_EST > .2501;
UPDATE DF_KPI_S  SET   S_VA_PNT_Y_PROFIT   = .75 WHERE PROFIT_Y_1Y_EST BETWEEN  .150001 AND .25;
UPDATE DF_KPI_S  SET   S_VA_PNT_Y_PROFIT   = .5 WHERE PROFIT_Y_1Y_EST BETWEEN  .10001 AND .15;
UPDATE DF_KPI_S  SET   S_VA_PNT_Y_PROFIT   = .25 WHERE PROFIT_Y_1Y_EST BETWEEN  .0001 AND .10;
UPDATE DF_KPI_S  SET   S_VA_PNT_Y_PROFIT   = - 1 WHERE PROFIT_Y_1Y_EST < 0;
-- S_VA_PNT_Z_PROFIT  max 100
-- AS WE DO NOT HAVE  EST_PRICE FROM ZACKS WE CACL  EXP PROFT BASED  ON GROWTH 
UPDATE DF_KPI_S  SET   S_VA_PNT_Z_PROFIT   = 1 WHERE Z_EXP_GROWTH > .2501;
UPDATE DF_KPI_S  SET   S_VA_PNT_Z_PROFIT   = .75 WHERE Z_EXP_GROWTH BETWEEN  .150001 AND .25;
UPDATE DF_KPI_S  SET   S_VA_PNT_Z_PROFIT   = .5 WHERE Z_EXP_GROWTH BETWEEN  .10001 AND .15;
UPDATE DF_KPI_S  SET   S_VA_PNT_Z_PROFIT   = .25 WHERE Z_EXP_GROWTH BETWEEN  .0001 AND .10;
UPDATE DF_KPI_S  SET   S_VA_PNT_Z_PROFIT   = - 1 WHERE Z_EXP_GROWTH < 0;


-----------------------------
--d)Others KPI
-----------------------------
--MISSING DATA AND FAILS  MAX  NONE 
UPDATE DF_KPI_S  SET   S_VA_PNT_MISS_DATA    =  S_VA_PNT_MISS_DATA   - .15 WHERE VALUATION_DCF IS NULL ;
UPDATE DF_KPI_S  SET   S_VA_PNT_MISS_DATA    =  S_VA_PNT_MISS_DATA   - .15 WHERE VALUATION_ROE IS NULL ;
UPDATE DF_KPI_S  SET   S_VA_PNT_MISS_DATA    =  S_VA_PNT_MISS_DATA   - 1 WHERE VALUATION_R1 IS NULL ;
UPDATE DF_KPI_S  SET   S_VA_PNT_MISS_DATA   = S_VA_PNT_MISS_DATA   - .15 WHERE BS_FLAG IS NULL  ;
UPDATE DF_KPI_S  SET   S_VA_PNT_MISS_DATA  =  S_VA_PNT_MISS_DATA - .15   WHERE  UPPER(REC_RANK) IS NULL ;
--PENALISISNG SOME S_VA_PNT_SECTORS 
UPDATE DF_KPI_S  SET   S_VA_PNT_SECTOR    =  - .5  WHERE UPPER(Z_SECTOR_CLASS) LIKE '%BANK%';
-- LESS THAN 4 YEARS DATA 
UPDATE DF_KPI_S  SET   S_VA_PNT_SECTOR    =  - .5  WHERE  YEARS_CNT<=3;

/*
--GROUPING 
--TRYING TO GET  A BALNCE  BETEEEW  PAST 50 % / FUTURE 30% / PRESENT-MARKET CONSENSUS  +20% / OTHERS LIKE MISSING DATA -10%
*/
UPDATE DF_KPI_S  SET
    S_VA_PNT_FUNDAMENTALS =S_VA_PNT_BIG_FIVE + S_VA_PNT_DEBT
    ,S_VA_PNT_PROFIT_PROJECTION =S_VA_PNT_PRICE +  S_VA_PNT_VALUATION_PROFIT + S_VA_PNT_GROWTH
    ,S_VA_PNT_MARKET_CONSENSUS = S_VA_PNT_Y_PROFIT + S_VA_PNT_Z_PROFIT
    ,S_VA_PNT_OTHERS_KPI = S_VA_PNT_MISS_DATA + S_VA_PNT_SECTOR;

--SCORE_VALUATION
UPDATE DF_KPI_S  SET SCORE_VALUATION =  S_VA_PNT_FUNDAMENTALS + S_VA_PNT_PROFIT_PROJECTION +  S_VA_PNT_MARKET_CONSENSUS + S_VA_PNT_OTHERS_KPI;

/* ********************************
Ends SCORE_VALUATION
******************************** */

/* ********************************
STARTS  SCORE_MOMENTUM
******************************** */

-- RESET 
UPDATE DF_KPI_S SET
 S_MO_PNT_RR_P1=0
,S_MO_PNT_RR_P2=0
,S_MO_PNT_Z_IND=0
,S_MO_PNT_VALUATION=0
,S_MO_PNT_BS_FLAG=0
,S_MO_PNT_DROP_UPS_DECILE=0
,S_MO_ALL_DATA_FLAG=    1  --FLAG
,SCORE_MOMENTUM=0
;

UPDATE DF_KPI_S SET  S_MO_ALL_DATA_FLAG =  0 WHERE REC_RANK IS NULL ;
UPDATE DF_KPI_S SET  S_MO_ALL_DATA_FLAG =  0 WHERE BS_FLAG IS NULL ;

/* ********************
RECOMENDATIONS Start +5
*********************** */
--S_MO_PNT_RR_P1
--  POINTS FOR GOOD FLAGS 
UPDATE DF_KPI_S SET S_MO_PNT_RR_P1 = S_MO_PNT_RR_P1 + 1
where  REC_RANK_NUM <= 3; 
-- GIVING  MOMETUM POINTS WHEN INCREASING   FROM 5 OR 4 TO  4, 3 OR 2 
--TRYING  TO PREDICT move TO 2 OR 1 AT SOME POINT 
UPDATE DF_KPI_S SET S_MO_PNT_RR_P1 = S_MO_PNT_RR_P1 + 2
where REC_RANK_NUM in (  2,3,4 )
and REC_VAR_ACTUAL> 0; 
-- SAME FOR  INCREASING  FROM 5 TO 4  OR ANY TO 1 , 
--ONE COULD BE IN THE HIGHEST  ALREADY THOSE LESS POINTS
UPDATE DF_KPI_S SET S_MO_PNT_RR_P1 = S_MO_PNT_RR_P1 + 1
where REC_RANK_NUM in (  1 )
and REC_VAR_ACTUAL> 0; 
-- IF POSITIVE CHANGE HAPPENED JUST FEW DAYS AGO EXTRA POINT
UPDATE DF_KPI_S SET S_MO_PNT_RR_P1 = S_MO_PNT_RR_P1 +  1 
WHERE  REC_VAR_ACTUAL_DAYSDIFF <9  AND REC_VAR_ACTUAL> 0  ; 

--S_MO_PNT_RR_P2
-- worsening  scores  which would lead decrease  price 
UPDATE DF_KPI_S SET S_MO_PNT_RR_P2 = S_MO_PNT_RR_P2 -2
where  REC_VAR_ACTUAL< 0 ;


UPDATE DF_KPI_S  SET  S_MO_PNT_RR = S_MO_PNT_RR_P1 + S_MO_PNT_RR_P2 ;

--FACTORIN IN Z_INDUSTRY_RANK
-- IF THE INDUSTRY IS NOT GETTING MONEY THEY GOOD FOR US  , SAME  BUY WHEN NOBODY ELSE DO
UPDATE DF_KPI_S  SET   S_MO_PNT_Z_IND   = +.5
WHERE UPPER(Z_INDUSTRY_RANK)  LIKE '%BOTTOM%';



/* ********************
--Good price:3 
*********************** */
UPDATE DF_KPI_S SET 
S_MO_PNT_VALUATION = S_MO_PNT_VALUATION
                +(CASE WHEN GOODPRICE_R1_FLAG = 1 THEN 2  
                  ELSE -3  END ) ;
                
UPDATE DF_KPI_S SET 
S_MO_PNT_VALUATION =  S_MO_PNT_VALUATION 
                +( CASE WHEN GOODPRICE_AVG_FLAG = 1 THEN 1 
                   WHEN GOODPRICE_AVG_FLAG = 0 THEN  .5 ELSE 0  END ) ;


/* ********************
BS :5
*********************** */
--GIVE POINTS FOR  BS ON SELL PROGRESSIVE ACCORDING TO DAYS.
--WE CAN   TO BUY WHEN EVERYBODY IS SEELLING, BU ON THE DEEP. NORMALLY ONE DAY 10.
   
UPDATE DF_KPI_S SET
S_MO_PNT_BS_1 =  ( CAST ( 2   AS FLOAT )
                    /
                  CAST ( 10 AS  FLOAT ) )
                    * LEAST (BS_FLAG_TREND_DAYS_IN , 10 ) 
WHERE  BS_FLAG LIKE '%SELL%' ;
-- GIVE POINT ON STRONG SELLS 
UPDATE DF_KPI_S SET S_MO_PNT_BS_2 =  + 3 WHERE   BS_FLAG = 'SELL ++' ;
-- REST POINT ON BUYS
UPDATE DF_KPI_S SET S_MO_PNT_BS_2 = -2 WHERE   BS_FLAG LIKE '%BUY%' ; 
--TOTAL
UPDATE DF_KPI_S SET  S_MO_PNT_BS_FLAG  = S_MO_PNT_BS_1+ S_MO_PNT_BS_2;
 
/* ********************
BS  ENDS 
*********************** */ 
--DROPS AND UPS : 2 
UPDATE DF_KPI_S SET 
S_MO_PNT_DROP_UPS_DECILE = CASE WHEN DROP_UPS_DECILE BETWEEN -2 AND -1 THEN  2
                                WHEN DROP_UPS_DECILE BETWEEN -5 AND -3 THEN  1
                                WHEN DROP_UPS_DECILE = 1 THEN  -2
                                WHEN DROP_UPS_DECILE BETWEEN 2 AND 3 THEN  -1
                                  ELSE 0 END ;
 /* ********************                               
   --TOTAL SCORE_MOMENTUM
*********************** */ 
UPDATE DF_KPI_S SET  SCORE_MOMENTUM    =  LEAST (S_MO_PNT_RR + S_MO_PNT_VALUATION + S_MO_PNT_BS_FLAG + S_MO_PNT_DROP_UPS_DECILE  +S_MO_PNT_Z_IND ,10  );

/* ********************************
ENDS   SCORE_MOMENTUM
******************************** */



--SCORE_TICKER
UPDATE DF_KPI_S SET  SCORE_TICKER    = ( ( SCORE_VALUATION * .75 ) +  ( SCORE_MOMENTUM*.25  ) );


--TODAYS_PICKS 
UPDATE DF_KPI_S SET  TODAYS_PICKS = 1 
WHERE TICKER IN (
SELECT 
TICKER
FROM DF_KPI_S
WHERE  SCORE_VALUATION  > 7
AND  SCORE_TICKER >7
AND GOODPRICE_R1_FLAG >=0
AND VOLUME_DECILE < 80
and   YEARS_CNT>3
);

--OUTPUT TABLE 
DROP TABLE IF EXISTS DF_KPI;
CREATE TABLE DF_KPI AS  
SELECT 
    TODAYS_PICKS    
    ,ETORO_FLAG
    ,PROFOLIO_FLAG
    ,TICKER
    ,SCORE_VALUATION
    ,SCORE_MOMENTUM
    ,SCORE_TICKER
    ,STIKER_PRICE 
    ,Y_PRICE_1Y_EST 
    ,REC_RANK
    ,BS_FLAG    
    ,bs_flag_trend_days_in
    ,DROP_UPS_DECILE
    ,DIFF_PRICE_PRC
    ,BIG5_TOTAL
    ,BIG5_FAIL 
    ,R1_GROWTH_EST
    ,GOODPRICE_R1_FLAG
    ,GOODPRICE_AVG_FLAG

    ,LY_DEBT_LT_RATIO
    ,LY_DEBT_ST_RATIO
    ,S_VA_PNT_FUNDAMENTALS 
    ,S_VA_PNT_PROFIT_PROJECTION 
    ,S_VA_PNT_MARKET_CONSENSUS 
    ,S_VA_PNT_OTHERS_KPI
    ,S_MO_ALL_DATA_FLAG 
    ,S_MO_PNT_RR
    ,S_MO_PNT_VALUATION
    ,S_MO_PNT_BS_FLAG
    ,S_MO_PNT_DROP_UPS_DECILE
    ,VOLUME_DECILE
    ,Z_SECTOR_CLASS
    ,Z_INDUSTRY_RANK
    ,COUNTRY
    ,NAME
    ,STIKER_PRICE_DATE
FROM DF_KPI_S 
WHERE VALUATION_R1 IS NOT NULL  

ORDER BY score_ticker
 DESC;

--this is a testing line 
END
$procedure$




