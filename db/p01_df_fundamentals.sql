
CREATE OR REPLACE PROCEDURE p01_df_fundamentals( MAX_YEAR IN INT DEFAULT DATE_PART('YEAR', CURRENT_DATE) )
LANGUAGE 'plpgsql'
AS $PROCEDURE$
BEGIN


-- DROTP TABLE ONLY 
DROP TABLE IF EXISTS DF_FUNDAMENTALS;

CREATE TABLE DF_FUNDAMENTALS AS (


    SELECT 
    T1.TICKER
    ,T1.YEAR
    ,T1.SEQ_YEAR_DESC
    ,T1.SEQ_YEAR_ASC

    ,'VALUE' AS TYPE
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	1	THEN   VALUE ELSE  0 END)   AS 	TOTALREVENUE
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	2	THEN   VALUE ELSE  0 END)   AS 	OPERATINGINCOME
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	3	THEN   VALUE ELSE  0 END)   AS 	INCOMEBEFORETAX
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	4	THEN   VALUE ELSE  0 END)   AS 	INCOMETAXEXPENSE
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	5	THEN   VALUE ELSE  0 END)   AS 	EARNINGS
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	6	THEN   VALUE ELSE  0 END)   AS 	FREECASHFLOW
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	7	THEN   VALUE ELSE  0 END)   AS 	CASHANDCASHEQUIVALENTS
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	8	THEN   VALUE ELSE  0 END)   AS 	TOTALLIAB
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	9	THEN   VALUE ELSE  0 END)   AS 	TOTALSTOCKHOLDEREQUITY
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	10	THEN   VALUE ELSE  0 END)   AS 	INVESTEDCAPITAL
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	11	THEN   VALUE ELSE  0 END)   AS 	TAXRATE
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	12	THEN   VALUE ELSE  0 END)   AS 	NOTPAD
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	13	THEN   VALUE ELSE  0 END)   AS 	ROIC
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	14	THEN   VALUE ELSE  0 END)   AS 	REVENUE_GW
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	15	THEN   VALUE ELSE  0 END)   AS 	EARNINGS_GW
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	16	THEN   VALUE ELSE  0 END)   AS 	EQUITY_GW
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	17	THEN   VALUE ELSE  0 END)   AS 	FREECASHFLOW_GW
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	18	THEN   VALUE ELSE  0 END)   AS 	LONGTERMDEBT
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	19	THEN   VALUE ELSE  0 END)   AS 	TOTALCURRENTASSETS
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	20	THEN   VALUE ELSE  0 END)   AS 	TOTALCURRENTLIABILITIES
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	21	THEN   VALUE ELSE  0 END)   AS 	DEBT_LT_RATIO
    , SUM (CASE WHEN   T1.FUNDAMENTAL_ID = 	22	THEN   VALUE ELSE  0 END)   AS 	DEBT_ST_RATIO


    FROM DF_FUNDAMENTALS_S1 T1
   
 WHERE  T1.YEAR  <= (SELECT MAX (T1A.YEAR)
                     FROM  DF_FUNDAMENTALS_S1 T1A
                     WHERE T1.TICKER=T1A.TICKER
                   AND T1A.YEAR <= MAX_YEAR
                   )
     
    -- AND  TICKER = 'EXEL'
     group by 
     T1.TICKER
    ,T1.YEAR
    ,T1.SEQ_YEAR_DESC
    ,T1.SEQ_YEAR_ASC	

);


-- Index created

CREATE  INDEX DF_FUNDAMENTALS_IX_1 ON DF_FUNDAMENTALS  (YEAR);  
CREATE  INDEX DF_FUNDAMENTALS_IX_2 ON DF_FUNDAMENTALS  (TICKER);  

--GET JUST LAST 5 YEAR 

DELETE FROM DF_FUNDAMENTALS WHERE SEQ_YEAR_DESC > 5;
DELETE FROM DF_FUNDAMENTALS WHERE (TICKER,YEAR) in  (

SELECT T1.TICKER,T2.YEAR
FROM DF_FUNDAMENTALS T1
LEFT JOIN DF_FUNDAMENTALS T2 ON T1.TICKER = T2.TICKER
WHERE T1.SEQ_YEAR_DESC = 1
AND T2.SEQ_YEAR_DESC > 1
and (T1.year- T2.year) > 4
);


--SEQ_YEAR_DESC &   SEQ_YEAR_ASC
UPDATE DF_FUNDAMENTALS T1
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
                FROM DF_FUNDAMENTALS T1A
                -- WHERE TICKER IN ( 'EXEL', 'V','LOGI')
                ) T1
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER  AND  T1.YEAR=T2.YEAR;



/*
---- INSERTING AVERAGE  RECORDS
*/
INSERT INTO  DF_FUNDAMENTALS
SELECT 
    T1.TICKER
    ,0 AS YEAR
    ,0 AS  SEQ_YEAR_DESC
    ,0 AS SEQ_YEAR_ASC
    ,'AVERAGE' AS TYPE
,AVG(	TOTALREVENUE	)	AS 	TOTALREVENUE
,AVG(	OPERATINGINCOME	)	AS 	OPERATINGINCOME
,AVG(	INCOMEBEFORETAX	)	AS 	INCOMEBEFORETAX
,AVG(	INCOMETAXEXPENSE	)	AS 	INCOMETAXEXPENSE
,AVG(	EARNINGS	)	AS 	EARNINGS
,AVG(	FREECASHFLOW	)	AS 	FREECASHFLOW
,AVG(	CASHANDCASHEQUIVALENTS	)	AS 	CASHANDCASHEQUIVALENTS
,AVG(	TOTALLIAB	)	AS 	TOTALLIAB
,AVG(	TOTALSTOCKHOLDEREQUITY	)	AS 	TOTALSTOCKHOLDEREQUITY
,AVG(	INVESTEDCAPITAL	)	AS 	INVESTEDCAPITAL
,AVG(	TAXRATE	)	AS 	TAXRATE
,AVG(	NOTPAD	)	AS 	NOTPAD
,AVG(	ROIC	)	AS 	ROIC
,AVG(	REVENUE_GW	)	AS 	REVENUE_GW
,AVG(	EARNINGS_GW	)	AS 	EARNINGS_GW
,AVG(	EQUITY_GW	)	AS 	EQUITY_GW
,AVG(	FREECASHFLOW_GW	)	AS 	FREECASHFLOW_GW
,AVG(	LONGTERMDEBT	)	AS 	LONGTERMDEBT
,AVG(	TOTALCURRENTASSETS	)	AS 	TOTALCURRENTASSETS
,AVG(	TOTALCURRENTLIABILITIES	)	AS 	TOTALCURRENTLIABILITIES
,AVG(	DEBT_LT_RATIO	)	AS 	DEBT_LT_RATIO
,AVG(	DEBT_ST_RATIO	)	AS 	DEBT_ST_RATIO

FROM  DF_FUNDAMENTALS T1

GROUP BY 
    T1.TICKER;
/* 
 --FIX ON  GROWTH RATES 
 */
UPDATE DF_FUNDAMENTALS  T1
SET
 	REVENUE_GW	=	T2.REVENUE_GW
,	EARNINGS_GW	=	T2.	EARNINGS_GW
,EQUITY_GW= T2.EQUITY_GW
,FREECASHFLOW_GW= T2.FREECASHFLOW_GW

,LONGTERMDEBT= T2.LONGTERMDEBT
,TOTALCURRENTASSETS= T2.TOTALCURRENTASSETS
,TOTALCURRENTLIABILITIES= T2.TOTALCURRENTLIABILITIES
,DEBT_LT_RATIO= T2.DEBT_LT_RATIO
,DEBT_ST_RATIO= T2.DEBT_ST_RATIO


FROM ( 
 SELECT 
    T1.TICKER
    ,0 AS YEAR
    ,0 AS  SEQ_YEAR_DESC
    ,0 AS SEQ_YEAR_ASC
    ,'AVERAGE' AS TYPE

,AVG(	REVENUE_GW	)	AS 	REVENUE_GW
,AVG(	EARNINGS_GW	)	AS 	EARNINGS_GW
,AVG(	EQUITY_GW	)	AS 	EQUITY_GW
,AVG(	FREECASHFLOW_GW	)	AS 	FREECASHFLOW_GW
,AVG(	LONGTERMDEBT	)	AS 	LONGTERMDEBT
,AVG(	TOTALCURRENTASSETS	)	AS 	TOTALCURRENTASSETS
,AVG(	TOTALCURRENTLIABILITIES	)	AS 	TOTALCURRENTLIABILITIES
,AVG(	DEBT_LT_RATIO	)	AS 	DEBT_LT_RATIO
,AVG(	DEBT_ST_RATIO	)	AS 	DEBT_ST_RATIO

FROM  DF_FUNDAMENTALS T1

WHERE  SEQ_YEAR_ASC >1

GROUP BY 
    T1.TICKER
    
     
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER and t1.TYPE =  'AVERAGE';
  

 
ALTER TABLE DF_FUNDAMENTALS  
ADD BIG5_TOTAL	INT 
, ADD BIG5_FAIL VARCHAR (200)
, ADD  YEARS_CNT INT
, ADD LASTEST_YEAR_FLAG  INT  DEFAULT 0 
,add BIG5_FAIL_VALUE  DECIMAL (14,6) DEFAULT 0 
,add BIG5_FAIL_NAME VARCHAR (20)
,add KPI_FLAG INT  DEFAULT 0 

;
 
 
UPDATE DF_FUNDAMENTALS  T1 SET LASTEST_YEAR_FLAG= 1  WHERE  SEQ_YEAR_DESC = 1 ;

/*
--BIG5_TOTAL &   BIG5_FAIL
*/
UPDATE DF_FUNDAMENTALS  T1
SET
 	BIG5_TOTAL	=	T2.BIG5_TOTAL
,	BIG5_FAIL	=	T2.	BIG5_FAIL
,BIG5_FAIL_VALUE= T2.BIG5_FAIL_VALUE
,BIG5_FAIL_NAME= T2.BIG5_FAIL_NAME

FROM (
  
SELECT 
        TICKER
        , CASE WHEN ROIC > .095 THEN  1 ELSE 0 END 
        + CASE WHEN FREECASHFLOW_GW > .095 THEN  1 ELSE 0 END 
        + CASE WHEN EQUITY_GW > .095 THEN  1 ELSE 0 END 
        + CASE WHEN REVENUE_GW > .095 THEN  1 ELSE 0 END 
        + CASE WHEN EARNINGS_GW > .095 THEN  1 ELSE 0 END   AS BIG5_TOTAL
        
        

        , CASE WHEN ROIC < .095 THEN  '1. ROIC: ' ||  CAST ( ROUND( ROIC::numeric*100,2)  AS VARCHAR (10)) || '% ' ELSE '' END 
        || CASE WHEN FREECASHFLOW_GW < .095 THEN  '2. FREECASHFLOW_GW: ' || CAST ( ROUND( FREECASHFLOW_GW::numeric*100,2) AS VARCHAR (10))  || '% ' ELSE '' END 
         || CASE WHEN EQUITY_GW < .095 THEN  '3. EQUITY_GW: ' || CAST ( ROUND( EQUITY_GW::numeric*100,2) AS VARCHAR (10)) || '% ' ELSE '' END 
        || CASE WHEN REVENUE_GW < .095 THEN  '4. REVENUE_GW: ' || CAST ( ROUND( REVENUE_GW::numeric*100,2) AS VARCHAR (10))  || '% ' ELSE '' END 
        || CASE WHEN EARNINGS_GW < .095 THEN  '5. EARNINGS_GW: ' || CAST ( ROUND( EARNINGS_GW::numeric*100,2) AS VARCHAR (10)) || '% ' ELSE '' END 
       
        AS BIG5_FAIL
       
        ,CASE WHEN ROIC < .095 THEN  ROIC  
            WHEN FREECASHFLOW_GW < .095 THEN  FREECASHFLOW_GW 
            WHEN EQUITY_GW < .095 THEN  EQUITY_GW
            WHEN REVENUE_GW < .095 THEN  REVENUE_GW 
            WHEN EARNINGS_GW < .095 THEN  EARNINGS_GW 
                           ELSE 0 END  as  BIG5_FAIL_VALUE

                           
                 ,CASE WHEN ROIC < .095 THEN  'ROIC'  
            WHEN FREECASHFLOW_GW < .095 THEN  'FREECASHFLOW_GW' 
            WHEN EQUITY_GW < .095 THEN  'EQUITY_GW'
            WHEN REVENUE_GW < .095 THEN  'REVENUE_GW' 
            WHEN EARNINGS_GW < .095 THEN  'EARNINGS_GW' 
                           ELSE 'NONE' END  as  BIG5_FAIL_NAME



    FROM DF_FUNDAMENTALS
    WHERE UPPER (TYPE) = 'AVERAGE'

        --SET  BIG 5 NUM 
 
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER ;
  

/*
--YEARS_CNT
*/
UPDATE DF_FUNDAMENTALS  T1
SET	YEARS_CNT	=	T2.YEARS_CNT
FROM (
    SELECT  
     TICKER
     ,COUNT (*) AS YEARS_CNT
     FROM DF_FUNDAMENTALS
     WHERE YEAR> 0 
     GROUP BY TICKER
     
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER ;
  
--KPI_FLAG  , which ones will be taken for next stage 
--all BIG5_TOTAL >3

UPDATE DF_FUNDAMENTALS  T1
set KPI_FLAG =1
where  BIG5_TOTAL >3;

-- in porfolio tickers  
UPDATE DF_FUNDAMENTALS  T1
set KPI_FLAG =2
where  TICKER in ( SELECT  T1A.TICKER FROM DF_PORFOLIO_VIEW T1A);

  END
$PROCEDURE$