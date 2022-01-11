CREATE OR REPLACE PROCEDURE etl05_df_fundamentals_s1 ()
LANGUAGE plpgsql
AS $procedure$
BEGIN

-- Drotp table Only 
DROP TABLE IF EXISTS DF_FUNDAMENTALS_S1;



CREATE TABLE DF_FUNDAMENTALS_S1 AS (
    SELECT DISTINCT 
        T1.FUNDAMENTAL_ID
        ,T1.TICKER
        ,T1.VALUE
        ,T1.YEAR
        ,T1.LOAD_DATE
        ,CAST (0 AS INT ) AS   R1_BIGFIVE_FLAG
        ,ROW_NUMBER() OVER(   PARTITION BY T1.TICKER,  T1.FUNDAMENTAL_ID  ORDER BY YEAR) AS SEQ_EVENT
      ,CAST (0 AS INT ) AS           SEQ_YEAR_ASC      
      ,CAST (0 AS INT ) AS           SEQ_YEAR_DESC
      
    FROM  PUBLIC.fundamentals_historic T1
    WHERE 1= 1
    --AND T1.TICKER = 'LOGI'
    AND T1.YEAR >= EXTRACT(YEAR FROM CURRENT_DATE) -5
);


-- INSERT RELEVANT GROWTH RATES 

INSERT INTO DF_FUNDAMENTALS_S1
SELECT 
    CASE WHEN   T1.FUNDAMENTAL_ID = 1 THEN 14
 		WHEN   T1.FUNDAMENTAL_ID = 5 THEN 15
		WHEN   T1.FUNDAMENTAL_ID = 9 THEN 16
		WHEN   T1.FUNDAMENTAL_ID = 6 THEN 17
		ELSE NULL END AS  FUNDAMENTAL_ID
		
		
        ,T1.TICKER
      	,case when (T1.VALUE = 0  or T2.VALUE = 0 ) then Null else  ( T1.VALUE-T2.VALUE)/ABS(T2.VALUE) end as VALUE
        ,T1.YEAR
        ,T1.LOAD_DATE
          ,CAST (0 AS INT ) AS   R1_BIGFIVE_FLAG
		 ,T1.SEQ_EVENT
FROM DF_FUNDAMENTALS_S1 AS T1
LEFT JOIN DF_FUNDAMENTALS_S1 AS T2 ON  T1.TICKER=T2.TICKER AND T1.FUNDAMENTAL_ID=T2.FUNDAMENTAL_ID  AND T1.SEQ_EVENT-1=T2.SEQ_EVENT
WHERE  T1.FUNDAMENTAL_ID IN (1,5,9,6)
ORDER BY   T1.FUNDAMENTAL_ID,T1.YEAR;


UPDATE DF_FUNDAMENTALS_S1 SET R1_BIGFIVE_FLAG = 1 WHERE FUNDAMENTAL_ID IN (SELECT FUNDAMENTAL_ID FROM PUBLIC.DF_FUNDAMENTAL_ID  WHERE R1_BIGFIVE_FLAG = 1 );

-- INSERT debt ratios 
-- Reference Value investing course - udimy - "4. Financial Statements 16. Useful Financial Ratios" 
---lt_debt_to_Equity_ratio
INSERT INTO DF_FUNDAMENTALS_S1
 SELECT 
    21 as fundamental_id
    ,t1.ticker 
    , case when (T1.VALUE = 0  or T2.VALUE = 0 ) then 0 else  t2.value / t1.value end as lt_debt_to_Equity_ratio  -- debt to equity ratio -  long term named lt_debt_to_Equity_ratio
    ,t1.year 
    ,T1.LOAD_DATE
      ,CAST (0 AS INT ) AS   R1_BIGFIVE_FLAG
     ,T1.SEQ_EVENT
    --,t1.value as totalstockholderequity
    --,t2.value as longtermdebt
FROM DF_FUNDAMENTALS_S1 AS T1
left join DF_FUNDAMENTALS_S1 AS T2 on T1.ticker=T2.ticker  and T1.year=T2.year    
where t1.FUNDAMENTAL_ID= '9'
and t2.FUNDAMENTAL_ID= '18';

--Current ratio -  short term debt 
--Short term debt , should be at list 1, but prefered to be 2 or huigher 
 INSERT INTO DF_FUNDAMENTALS_S1
 SELECT 
    22 as fundamental_id
    ,t1.ticker 
    , case when (T1.VALUE = 0  or T2.VALUE = 0 ) then 0 else  t2.value / t1.value end as st_debt_ratio  --Current ratio -  short term debt 
    ,t1.year 
    ,T1.LOAD_DATE
      ,CAST (0 AS INT ) AS   R1_BIGFIVE_FLAG
     ,T1.SEQ_EVENT
    --,t1.value as totalcurrentliabilities
    --,t2.value as totalcurrentassets
FROM DF_FUNDAMENTALS_S1 AS T1
left join DF_FUNDAMENTALS_S1 AS T2 on T1.ticker=T2.ticker  and T1.year=T2.year    
where t1.FUNDAMENTAL_ID= '20'
and t2.FUNDAMENTAL_ID= '19';




--SEQ_YEAR_DESC &   SEQ_YEAR_ASC
UPDATE DF_FUNDAMENTALS_S1 T1
SET
 	SEQ_YEAR_DESC	=	T2.SEQ_YEAR_DESC
,	SEQ_YEAR_ASC	=	T2.	SEQ_YEAR_ASC

FROM (
            SELECT 
            T1.TICKER
            ,T1.YEAR
            ,ROW_NUMBER() OVER(   PARTITION BY T1.TICKER ORDER BY YEAR DESC) AS SEQ_YEAR_DESC
            ,ROW_NUMBER() OVER(   PARTITION BY T1.TICKER ORDER BY YEAR ASC) AS SEQ_YEAR_ASC
            FROM (
                SELECT DISTINCT 
                TICKER
                ,YEAR
                FROM DF_FUNDAMENTALS_S1 T1A
                -- WHERE TICKER IN ( 'EXEL', 'V','LOGI')
                ) T1
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER  AND  T1.YEAR=T2.YEAR;
  
                        

-- DF_FUNDAMENTALS_S2A 
DROP TABLE IF EXISTS DF_FUNDAMENTALS_S2A;



CREATE TABLE DF_FUNDAMENTALS_S2A AS (


    SELECT 
    TICKER
    ,YEAR
    ,SEQ_YEAR_DESC
    ,SEQ_YEAR_ASC
    , sum (case when   fundamental_id = 	1	then   VALUE else  0 end)   as 	totalrevenue
    , sum (case when   fundamental_id = 	2	then   VALUE else  0 end)   as 	operatingincome
    , sum (case when   fundamental_id = 	3	then   VALUE else  0 end)   as 	incomebeforetax
    , sum (case when   fundamental_id = 	4	then   VALUE else  0 end)   as 	incometaxexpense
    , sum (case when   fundamental_id = 	5	then   VALUE else  0 end)   as 	earnings
    , sum (case when   fundamental_id = 	6	then   VALUE else  0 end)   as 	freecashflow
    , sum (case when   fundamental_id = 	7	then   VALUE else  0 end)   as 	cashandcashequivalents
    , sum (case when   fundamental_id = 	8	then   VALUE else  0 end)   as 	totalliab
    , sum (case when   fundamental_id = 	9	then   VALUE else  0 end)   as 	totalstockholderequity
    , sum (case when   fundamental_id = 	10	then   VALUE else  0 end)   as 	investedcapital
    , sum (case when   fundamental_id = 	11	then   VALUE else  0 end)   as 	taxrate
    , sum (case when   fundamental_id = 	12	then   VALUE else  0 end)   as 	notpad
    , sum (case when   fundamental_id = 	13	then   VALUE else  0 end)   as 	roic
    , sum (case when   fundamental_id = 	14	then   VALUE else  0 end)   as 	revenue_gw
    , sum (case when   fundamental_id = 	15	then   VALUE else  0 end)   as 	earnings_gw
    , sum (case when   fundamental_id = 	16	then   VALUE else  0 end)   as 	equity_gw
    , sum (case when   fundamental_id = 	17	then   VALUE else  0 end)   as 	freecashflow_gw
    , sum (case when   fundamental_id = 	18	then   VALUE else  0 end)   as 	longtermdebt
    , sum (case when   fundamental_id = 	19	then   VALUE else  0 end)   as 	totalcurrentassets
    , sum (case when   fundamental_id = 	20	then   VALUE else  0 end)   as 	totalcurrentliabilities
    , sum (case when   fundamental_id = 	21	then   VALUE else  0 end)   as 	lt_debt_to_Equity_ratio
    , sum (case when   fundamental_id = 	22	then   VALUE else  0 end)   as 	st_debt_ratio


    FROM DF_FUNDAMENTALS_S1
   

     group by 
     TICKER
    ,YEAR
    ,SEQ_YEAR_DESC
    ,SEQ_YEAR_ASC	

);


CREATE  INDEX DF_FUNDAMENTALS_S1_IX_1 ON DF_FUNDAMENTALS_S1  (TICKER);  
CREATE  INDEX DF_FUNDAMENTALS_S1_IX_2 ON DF_FUNDAMENTALS_S1  (TICKER,YEAR);  
CREATE  INDEX DF_FUNDAMENTALS_S1_IX_3 ON DF_FUNDAMENTALS_S1  (fundamental_id,YEAR    );  
CREATE  INDEX DF_FUNDAMENTALS_S1_IX_4 ON DF_FUNDAMENTALS_S1  (year desc );  
CREATE  INDEX DF_FUNDAMENTALS_S1_IX_5 ON DF_FUNDAMENTALS_S1  (TICKER,fundamental_id ,YEAR);  

END
$procedure$
