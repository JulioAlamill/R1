--Y_RANK
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
Y_RANK = T2.	Y_RANK
, Y_RANK_NUM =T2.	Y_RANK_NUM 
, Y_RANK_ANALYST_NUM = T2.	Y_RANK_ANALYST_NUM 
, PROFIT_MARGINS = T2.	PROFIT_MARGINS 


FROM (

  SELECT DISTINCT 
   T1. TICKER
	,T2.Y_RANK
	,T2.Y_RANK_NUM
	,T2.Y_RANK_ANALYST_NUM
	,T2.PROFIT_MARGINS
    ,T1.LOAD_DATE
    FROM  DF_H_YAHOO_RECOMMENDATIONS T1
	LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
    WHERE    T1.Y_RANK IS  NULL  -- HERE CHANGE 
	AND  T2.Y_RANK IS NOT NULL  -- HERE CHANGE 
    AND T2.LOAD_DATE = (
                        SELECT MIN (T2A.LOAD_DATE )  -- EARLIES AFTER 
                        FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A  
                        WHERE  T2A.Y_RANK IS NOT NULL  -- HERE CHANGE 
						AND T2A.TICKER=T2.TICKER
                        AND  T2A.LOAD_DATE >= T1.LOAD_DATE -- EARLIES AFTER 
                        )
					
                        
	
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ; 

--Y_PRICE_1Y_EST
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
Y_PRICE_1Y_EST	=	T2.	Y_PRICE_1Y_EST



FROM (
        SELECT 
        T1.TICKER
        ,T2.	Y_PRICE_1Y_EST
        ,T1.LOAD_DATE
          FROM  DF_H_YAHOO_RECOMMENDATIONS T1
        LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
        WHERE 1=1
        AND  T1.Y_PRICE_1Y_EST IS  NULL  -- HERE CHANGE 
        AND  T2.Y_PRICE_1Y_EST IS  NOT  NULL  -- HERE CHANGE 
        AND T2.LOAD_DATE = (SELECT MIN (T2A.LOAD_DATE )  -- EARLIES AFTER 
                                 FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
                                WHERE  T2A.Y_PRICE_1Y_EST IS NOT NULL 
                              AND T2A.TICKER=T2.TICKER
                                AND  T2A.LOAD_DATE >= T1.LOAD_DATE -- EARLIES AFTER 
                                )
	
      )  AS T2 

WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ; 


--UPDATES FROM  Y_REVENUE_GROWTH_EST   &  Y_EARNINGS_GROWTH_EST  1ST
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
Y_REVENUE_GROWTH_EST	=	T2.	Y_REVENUE_GROWTH_EST
,Y_EARNINGS_GROWTH_EST	=	T2.	Y_EARNINGS_GROWTH_EST
FROM (
        SELECT 
        T1.TICKER
        ,T2.Y_REVENUE_GROWTH_EST
        ,T2.Y_EARNINGS_GROWTH_EST
        ,T1.LOAD_DATE
        FROM  DF_H_YAHOO_RECOMMENDATIONS T1
        LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
        WHERE 1=1 
        AND      T1.Y_REVENUE_GROWTH_EST IS  NULL   -- HERE TO CHANGE 
        AND T2.Y_REVENUE_GROWTH_EST IS NOT  NULL   -- HERE TO CHANGE 
        AND T2.LOAD_DATE = (SELECT MIN (T2A.LOAD_DATE )  -- EARLIES AFTER 
                                 FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
                                WHERE  T2A.Y_REVENUE_GROWTH_EST  IS NOT NULL  
                              AND T2A.TICKER=T2.TICKER
                                AND  T2A.LOAD_DATE >= T1.LOAD_DATE -- EARLIES AFTER 
                                )
	
      )  AS T2 
WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ;



--UPDATES FROM  GROWTH_EST_NEXT_YEAR  & GROWTH_EST_NEXT_5_YEARS 1ST
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
GROWTH_EST_NEXT_YEAR	=	T2.	GROWTH_EST_NEXT_YEAR
,GROWTH_EST_NEXT_5_YEARS	=	T2.	GROWTH_EST_NEXT_5_YEARS

FROM (
		SELECT 
		T1.TICKER
		,T2.GROWTH_EST_NEXT_YEAR
		,T2.GROWTH_EST_NEXT_5_YEARS
       , T1.LOAD_DATE
	    FROM  DF_H_YAHOO_RECOMMENDATIONS T1
	LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
		WHERE 1=1
  AND  T2 .GROWTH_EST_NEXT_YEAR IS NOT  NULL       
AND  T2.GROWTH_EST_NEXT_YEAR IS  NULL 
AND     T2.LOAD_DATE = (SELECT MIN (T2A.LOAD_DATE )  -- EARLIES AFTER 
								FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
								WHERE  T2A.GROWTH_EST_NEXT_YEAR  IS NOT NULL   -- HERE TO CHANGE 
								AND T2A.TICKER=T2.TICKER
								AND  T2A.LOAD_DATE >= T1.LOAD_DATE -- EARLIES AFTER 
								)

      )  AS T2 
WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ;

ETL03_QUOTE_H_UPDATE

--UPDATES FROM  MARKET_CAP  1ST
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
MARKET_CAP	=	T2.	MARKET_CAP



FROM (
		SELECT 
		T1.TICKER
		,T2.MARKET_CAP
        ,T1.LOAD_DATE
	    FROM  DF_H_YAHOO_RECOMMENDATIONS T1
	LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
		WHERE 1=1
          AND T1.MARKET_CAP IS  NULL 
                AND T2.MARKET_CAP IS  NOT NULL 
        AND T2.LOAD_DATE = (SELECT MIN (T2A.LOAD_DATE )  -- EARLIES AFTER 
								FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
												WHERE  T2A.MARKET_CAP  IS NOT NULL   -- HERE TO CHANGE 
								AND T2A.TICKER=T2.TICKER
								AND  T2A.LOAD_DATE >= T1.LOAD_DATE -- EARLIES AFTER 
								)

      )  AS T2 WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ;




--UPDATES FROM  AVG_VOLUME  1ST
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
AVG_VOLUME	=	T2.	AVG_VOLUME

FROM (
		SELECT 
		T1.TICKER
		,T2.AVG_VOLUME
        ,T1.LOAD_DATE
	    FROM  DF_H_YAHOO_RECOMMENDATIONS T1
LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
		WHERE 1=1
          AND T1.AVG_VOLUME IS  NULL 
                AND T2.AVG_VOLUME IS  NOT NULL 
        AND T2.LOAD_DATE = (SELECT MAX (T2A.LOAD_DATE )  -- EARLIES AFTER 
								FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
												WHERE  T2A.AVG_VOLUME   IS NOT NULL   -- HERE TO CHANGE 
								AND T2A.TICKER=T2.TICKER
								AND  T2A.LOAD_DATE <= T1.LOAD_DATE -- EARLIES AFTER 
								)

      )  AS T2 WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ;
      
    
      
      
      

--UPDATES FROM  EPS &  PE_RATIO 
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
EPS	=	T2.	EPS
PE_RATIO	=	T2.	PE_RATIO
FROM (
		SELECT 
		T1.TICKER
		,T2.EPS
        ,T2.PE_RATIO

        ,T1.LOAD_DATE
	    FROM  DF_H_YAHOO_RECOMMENDATIONS T1
LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
		WHERE 1=1
          AND T1.EPS IS  NULL 
                AND T2.EPS IS  NOT NULL 
        AND T2.LOAD_DATE = (SELECT MAX (T2A.LOAD_DATE ) -- MAX BEFORE 
								FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
												WHERE  T2A.EPS   IS NOT NULL   -- HERE TO CHANGE 
								AND T2A.TICKER=T2.TICKER
								AND  T2A.LOAD_DATE <= T1.LOAD_DATE -- MAX BEFORE 
								)

      )  AS T2 WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ;
  

--UPDATES FROM  SS_SHARES_OUTSTANDING  1ST
UPDATE DF_H_YAHOO_RECOMMENDATIONS  T1
SET
SS_SHARES_OUTSTANDING	=	T2.	SS_SHARES_OUTSTANDING

FROM (
		SELECT 
		T1.TICKER
		,T2.SS_SHARES_OUTSTANDING
        ,T1.LOAD_DATE
	    FROM  DF_H_YAHOO_RECOMMENDATIONS T1
LEFT JOIN  DF_H_YAHOO_RECOMMENDATIONS T2 ON T1.TICKER= T2.TICKER
		WHERE 1=1
          AND T1.SS_SHARES_OUTSTANDING IS  NULL 
                AND T2.SS_SHARES_OUTSTANDING IS  NOT NULL 
        AND T2.LOAD_DATE = (SELECT MIN (T2A.LOAD_DATE )  -- EARLIES AFTER 
								FROM DF_H_YAHOO_RECOMMENDATIONS  AS  T2A
												WHERE  T2A.SS_SHARES_OUTSTANDING   IS NOT NULL   -- HERE TO CHANGE 
								AND T2A.TICKER=T2.TICKER
								AND  T2A.LOAD_DATE >= T1.LOAD_DATE -- EARLIES AFTER 
								)

      )  AS T2 WHERE  T1.TICKER=T2.TICKER AND T1.LOAD_DATE= T2.LOAD_DATE   ;
      
      
      
    