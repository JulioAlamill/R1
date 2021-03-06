CREATE OR REPLACE PROCEDURE etl04_other_tables_update ()
LANGUAGE SQL
as $$


/* UPDATED ZACKS HISTORY  */

DELETE FROM DF_H_ZACKS_INDICATORS T1
WHERE   (T1.TICKER , T1.PERIOD )  IN (  SELECT T1A.TICKER , T1A.PERIOD   FROM DF_ZACKS_INDICATORS T1A);
INSERT INTO DF_H_ZACKS_INDICATORS 
SELECT 
	ticker
,	rank
,	sector_class
,	industry_rank
,	cast (exp_growth as decimal (10,2)) as exp_growth
,	period
,	cast (LOAD_DATE as date ) as load_date

FROM DF_ZACKS_INDICATORS;

--CREATE  INDEX DF_H_ZACKS_INDICATORS_IX_1  ON DF_H_ZACKS_INDICATORS  (TICKER); 


/* UPDATED NAMES HISTORY  */
/*
DELETE FROM DF_H_TICKER_NAME T1
WHERE  T1.TICKER IN (  SELECT T1.TICKER   FROM DF_TICKER_NAME T1A);
INSERT INTO DF_H_TICKER_NAME SELECT 

ticker
,name
,exch
,type
,exchdisp
,typedisp
,period
,load_date

 FROM DF_TICKER_NAME;
-- Index created 
--CREATE  INDEX DF_H_TICKER_NAME_IX_1 ON DF_H_TICKER_NAME  (TICKER);  

*/

 $$;