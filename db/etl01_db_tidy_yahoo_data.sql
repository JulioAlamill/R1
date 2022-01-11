CREATE OR REPLACE PROCEDURE etl01_db_tidy_yahoo_data ()
LANGUAGE SQL
as $$
/* TABLES TO BE CREATE
select * from public.df_income_statement
select * from public.df_balance
select * from public.df_cash_flow_statement
select * from public.df_earnings_q_results
select * from public.df_earnings_q_revenue
select * from public.df_earnings_y_revenue
select * from public.df_growth_est
select * from public.df_revenue_est
select * from public.df_earnings_est
select * from public.df_earnings_hist
select * from public.df_eps_trend
select * from public.df_quote_table
select * from public.df_stats

*/

--DF_INCOME_STATEMENT

-- DROP TABLE IF EXISTS DF_INCOME_STATEMENT;
-- CREATE TABLE DF_INCOME_STATEMENT
 -- (
	-- ticker	Varchar(15)
-- ,	report_Period	int
-- ,	researchdevelopment	Decimal (18,2) Default 0
-- ,	effectofaccountingcharges	Decimal (18,2) Default 0
-- ,	incomebeforetax	Decimal (18,2) Default 0
-- ,	minorityinterest	Decimal (18,2) Default 0
-- ,	netincome	Decimal (18,2) Default 0
-- ,	sellinggeneraladministrative	Decimal (18,2) Default 0
-- ,	grossprofit	Decimal (18,2) Default 0
-- ,	ebit	Decimal (18,2) Default 0
-- ,	operatingincome	Decimal (18,2) Default 0
-- ,	otheroperatingexpenses	Decimal (18,2) Default 0
-- ,	interestexpense	Decimal (18,2) Default 0
-- ,	extraordinaryitems	Decimal (18,2) Default 0
-- ,	nonrecurring	Decimal (18,2) Default 0
-- ,	otheritems	Decimal (18,2) Default 0
-- ,	incometaxexpense	Decimal (18,2) Default 0
-- ,	totalrevenue	Decimal (18,2) Default 0
-- ,	totaloperatingexpenses	Decimal (18,2) Default 0
-- ,	costofrevenue	Decimal (18,2) Default 0
-- ,	totalotherincomeexpensenet	Decimal (18,2) Default 0
-- ,	discontinuedoperations	Decimal (18,2) Default 0
-- ,	netincomefromcontinuingops	Decimal (18,2) Default 0
-- ,	netincomeapplicabletocommonshares	Decimal (18,2) Default 0
-- , LOAD_DATE DATE	
-- );

DELETE FROM DF_INCOME_STATEMENT;
INSERT INTO DF_INCOME_STATEMENT
select 
 ticker
,	enddate
,coalesce(cast ( researchdevelopment  as decimal (18,2))/1000000.00 , 0) as researchdevelopment
,coalesce(cast ( effectofaccountingcharges as decimal (18,2))/1000000.00 , 0) as effectofaccountingcharges
,coalesce(cast ( incomebeforetax as decimal (18,2))/1000000.00 , 0) as incomebeforetax
,coalesce(cast ( minorityinterest as decimal (18,2))/1000000.00 , 0) as minorityinterest
,coalesce(cast ( netincome as decimal (18,2))/1000000.00 , 0) as netincome
,coalesce(cast ( sellinggeneraladministrative as decimal (18,2))/1000000.00 , 0) as sellinggeneraladministrative
,coalesce(cast ( grossprofit as decimal (18,2))/1000000.00 , 0) as grossprofit
,coalesce(cast ( ebit as decimal (18,2))/1000000.00 , 0) as grossprofit
,coalesce(cast ( operatingincome as decimal (18,2))/1000000.00 , 0) as operatingincome
,coalesce(cast ( otheroperatingexpenses as decimal (18,2))/1000000.00 , 0) as otheroperatingexpenses
,coalesce(cast ( interestexpense as decimal (18,2))/1000000.00 , 0) as interestexpense
,coalesce(cast ( extraordinaryitems as decimal (18,2))/1000000.00 , 0) as extraordinaryitems
,coalesce(cast ( nonrecurring as decimal (18,2))/1000000.00 , 0) as nonrecurring
,coalesce(cast ( otheritems as decimal (18,2))/1000000.00 , 0) as otheritems
,coalesce(cast ( incometaxexpense as decimal (18,2))/1000000.00 , 0) as incometaxexpense
,coalesce(cast ( totalrevenue as decimal (18,2))/1000000.00 , 0) as totalrevenue
,coalesce(cast ( totaloperatingexpenses as decimal (18,2))/1000000.00 , 0) as totaloperatingexpenses
,coalesce(cast ( costofrevenue as decimal (18,2))/1000000.00 , 0) as costofrevenue
,coalesce(cast ( totalotherincomeexpensenet as decimal (18,2))/1000000.00 , 0) as totalotherincomeexpensenet
,coalesce(cast ( discontinuedoperations as decimal (18,2))/1000000.00 , 0) as discontinuedoperations
,coalesce(cast ( netincomefromcontinuingops as decimal (18,2))/1000000.00 , 0) as netincomefromcontinuingops
,coalesce(cast ( netincomeapplicabletocommonshares as decimal (18,2))/1000000.00 , 0) as netincomeapplicabletocommonshares
, CURRENT_DATE as LOAD_DATE
	from public.df_income_statement_s;




--df_balance


-- DROP TABLE IF EXISTS DF_BALANCE
-- ;
-- CREATE TABLE DF_BALANCE

 -- (
	-- ticker	Varchar(15)
-- ,	report_Period 	int
-- ,	commonStock	Decimal (18,2) Default 0
-- ,	deferredLongTermAssetCharges	Decimal (18,2) Default 0
-- ,	deferredLongTermLiab	Decimal (18,2) Default 0
-- ,	goodWill	Decimal (18,2) Default 0
-- ,	intangibleAssets	Decimal (18,2) Default 0
-- ,	inventory	Decimal (18,2) Default 0
-- ,	longTermDebt	Decimal (18,2) Default 0
-- ,	longTermInvestments	Decimal (18,2) Default 0
-- ,	minorityInterest	Decimal (18,2) Default 0
-- ,	netReceivables	Decimal (18,2) Default 0
-- ,	netTangibleAssets	Decimal (18,2) Default 0
-- ,	otherAssets	Decimal (18,2) Default 0
-- ,	otherCurrentAssets	Decimal (18,2) Default 0
-- ,	otherCurrentLiab	Decimal (18,2) Default 0
-- ,	otherLiab	Decimal (18,2) Default 0
-- ,	otherStockholderEquity	Decimal (18,2) Default 0
-- ,	propertyPlantEquipment	Decimal (18,2) Default 0
-- ,	retainedEarnings	Decimal (18,2) Default 0
-- ,	shortLongTermDebt	Decimal (18,2) Default 0
-- ,	shortTermInvestments	Decimal (18,2) Default 0
-- ,	totalAssets	Decimal (18,2) Default 0
-- ,	totalCurrentAssets	Decimal (18,2) Default 0
-- ,	totalCurrentLiabilities	Decimal (18,2) Default 0
-- ,	totalLiab	Decimal (18,2) Default 0
-- ,	totalStockholderEquity	Decimal (18,2) Default 0
-- ,	treasuryStock	Decimal (18,2) Default 0
-- ,	accountsPayable	Decimal (18,2) Default 1
-- ,	capitalSurplus	Decimal (18,2) Default 2
-- ,	cash	Decimal (18,2) Default 3
-- , LOAD_DATE DATE	
-- );

DELETE FROM DF_BALANCE;
INSERT INTO DF_BALANCE
SELECT 
	ticker 
,	enddate 
,coalesce(cast (  	commonStock	 as decimal (18,2))/1000000.00 , 0) as 	commonStock
,coalesce(cast (  	deferredLongTermAssetCharges	 as decimal (18,2))/1000000.00 , 0) as 	deferredLongTermAssetCharges
,coalesce(cast (  	deferredLongTermLiab	 as decimal (18,2))/1000000.00 , 0) as 	deferredLongTermLiab
,coalesce(cast (  	goodWill	 as decimal (18,2))/1000000.00 , 0) as 	goodWill
,coalesce(cast (  	intangibleAssets	 as decimal (18,2))/1000000.00 , 0) as 	intangibleAssets
,coalesce(cast (  	inventory	 as decimal (18,2))/1000000.00 , 0) as 	inventory
,coalesce(cast (  	longTermDebt	 as decimal (18,2))/1000000.00 , 0) as 	longTermDebt
,coalesce(cast (  	longTermInvestments	 as decimal (18,2))/1000000.00 , 0) as 	longTermInvestments
,coalesce(cast (  	minorityInterest	 as decimal (18,2))/1000000.00 , 0) as 	minorityInterest
,coalesce(cast (  	netReceivables	 as decimal (18,2))/1000000.00 , 0) as 	netReceivables
,coalesce(cast (  	netTangibleAssets	 as decimal (18,2))/1000000.00 , 0) as 	netTangibleAssets
,coalesce(cast (  	otherAssets	 as decimal (18,2))/1000000.00 , 0) as 	otherAssets
,coalesce(cast (  	otherCurrentAssets	 as decimal (18,2))/1000000.00 , 0) as 	otherCurrentAssets
,coalesce(cast (  	otherCurrentLiab	 as decimal (18,2))/1000000.00 , 0) as 	otherCurrentLiab
,coalesce(cast (  	otherLiab	 as decimal (18,2))/1000000.00 , 0) as 	otherLiab
,coalesce(cast (  	otherStockholderEquity	 as decimal (18,2))/1000000.00 , 0) as 	otherStockholderEquity
,coalesce(cast (  	propertyPlantEquipment	 as decimal (18,2))/1000000.00 , 0) as 	propertyPlantEquipment
,coalesce(cast (  	retainedEarnings	 as decimal (18,2))/1000000.00 , 0) as 	retainedEarnings
,coalesce(cast (  	shortLongTermDebt	 as decimal (18,2))/1000000.00 , 0) as 	shortLongTermDebt
,coalesce(cast (  	shortTermInvestments	 as decimal (18,2))/1000000.00 , 0) as 	shortTermInvestments
,coalesce(cast (  	totalAssets	 as decimal (18,2))/1000000.00 , 0) as 	totalAssets
,coalesce(cast (  	totalCurrentAssets	 as decimal (18,2))/1000000.00 , 0) as 	totalCurrentAssets
,coalesce(cast (  	totalCurrentLiabilities	 as decimal (18,2))/1000000.00 , 0) as 	totalCurrentLiabilities
,coalesce(cast (  	totalLiab	 as decimal (18,2))/1000000.00 , 0) as 	totalLiab
,coalesce(cast (  	totalStockholderEquity	 as decimal (18,2))/1000000.00 , 0) as 	totalStockholderEquity
,coalesce(cast (  	treasuryStock	 as decimal (18,2))/1000000.00 , 0) as 	treasuryStock
,coalesce(cast (  	accountsPayable	 as decimal (18,2))/1000000.00 , 0) as 	accountsPayable
,coalesce(cast (  	capitalSurplus	 as decimal (18,2))/1000000.00 , 0) as 	capitalSurplus
,coalesce(cast (  	cash	 as decimal (18,2))/1000000.00 , 0) as 	cash


, CURRENT_DATE as LOAD_DATE
FROM  DF_BALANCE_S;

--df_cash_flow_statement



-- DROP TABLE IF EXISTS DF_CASH_FLOW_STATEMENT;
-- CREATE TABLE DF_CASH_FLOW_STATEMENT

 -- (
	-- ticker	Varchar(15)
-- ,	report_Period	int
-- ,	capitalExpenditures	Decimal (18,2) Default 0
-- ,	changeInCash	Decimal (18,2) Default 0
-- ,	changeToAccountReceivables	Decimal (18,2) Default 0
-- ,	changeToInventory	Decimal (18,2) Default 0
-- ,	changeToLiabilities	Decimal (18,2) Default 0
-- ,	changeToNetincome	Decimal (18,2) Default 0
-- ,	changeToOperatingActivities	Decimal (18,2) Default 0
-- ,	depreciation	Decimal (18,2) Default 0
-- ,	dividendsPaid	Decimal (18,2) Default 0
-- ,	effectOfExchangeRate	Decimal (18,2) Default 0
-- ,	investments	Decimal (18,2) Default 0
-- ,	issuanceOfStock	Decimal (18,2) Default 0
-- ,	netBorrowings	Decimal (18,2) Default 0
-- ,	netIncome	Decimal (18,2) Default 0
-- ,	otherCashflowsFromFinancingActivities	Decimal (18,2) Default 0
-- ,	otherCashflowsFromInvestingActivities	Decimal (18,2) Default 0
-- ,	repurchaseOfStock	Decimal (18,2) Default 0
-- ,	totalCashFromFinancingActivities	Decimal (18,2) Default 0
-- ,	totalCashFromOperatingActivities	Decimal (18,2) Default 0
-- ,	totalCashflowsFromInvestingActivities	Decimal (18,2) Default 0
-- , load_date date	
-- );

DELETE FROM DF_CASH_FLOW_STATEMENT;
INSERT INTO DF_CASH_FLOW_STATEMENT
SELECT 
	  	
	ticker
,	enddate
,coalesce(cast ( 	capitalExpenditures	 as decimal (18,2))/1000000.00 , 0) as 	capitalExpenditures
,coalesce(cast ( 	changeInCash	 as decimal (18,2))/1000000.00 , 0) as 	changeInCash
,coalesce(cast ( 	changeToAccountReceivables	 as decimal (18,2))/1000000.00 , 0) as 	changeToAccountReceivables
,coalesce(cast ( 	changeToInventory	 as decimal (18,2))/1000000.00 , 0) as 	changeToInventory
,coalesce(cast ( 	changeToLiabilities	 as decimal (18,2))/1000000.00 , 0) as 	changeToLiabilities
,coalesce(cast ( 	changeToNetincome	 as decimal (18,2))/1000000.00 , 0) as 	changeToNetincome
,coalesce(cast ( 	changeToOperatingActivities	 as decimal (18,2))/1000000.00 , 0) as 	changeToOperatingActivities
,coalesce(cast ( 	depreciation	 as decimal (18,2))/1000000.00 , 0) as 	depreciation
,coalesce(cast ( 	dividendsPaid	 as decimal (18,2))/1000000.00 , 0) as 	dividendsPaid
,coalesce(cast ( 	effectOfExchangeRate	 as decimal (18,2))/1000000.00 , 0) as 	effectOfExchangeRate
,coalesce(cast ( 	investments	 as decimal (18,2))/1000000.00 , 0) as 	investments
,coalesce(cast ( 	issuanceOfStock	 as decimal (18,2))/1000000.00 , 0) as 	issuanceOfStock
,coalesce(cast ( 	netBorrowings	 as decimal (18,2))/1000000.00 , 0) as 	netBorrowings
,coalesce(cast ( 	netIncome	 as decimal (18,2))/1000000.00 , 0) as 	netIncome
,coalesce(cast ( 	otherCashflowsFromFinancingActivities	 as decimal (18,2))/1000000.00 , 0) as 	otherCashflowsFromFinancingActivities
,coalesce(cast ( 	otherCashflowsFromInvestingActivities	 as decimal (18,2))/1000000.00 , 0) as 	otherCashflowsFromInvestingActivities
,coalesce(cast ( 	repurchaseOfStock	 as decimal (18,2))/1000000.00 , 0) as 	repurchaseOfStock
,coalesce(cast ( 	totalCashFromFinancingActivities	 as decimal (18,2))/1000000.00 , 0) as 	totalCashFromFinancingActivities
,coalesce(cast ( 	totalCashFromOperatingActivities	 as decimal (18,2))/1000000.00 , 0) as 	totalCashFromOperatingActivities
,coalesce(cast ( 	totalCashflowsFromInvestingActivities	 as decimal (18,2))/1000000.00 , 0) as 	totalCashflowsFromInvestingActivities


, CURRENT_DATE as LOAD_DATE
FROM DF_CASH_FLOW_STATEMENT_S;



-- DF_QUOTE_TABLE

-- DROP TABLE IF EXISTS DF_QUOTE_TABLE;
-- CREATE TABLE DF_QUOTE_TABLE (

	 -- TICKER	Varchar(15)
-- ,	 TARGET_EST_1Y	VARCHAR(50)
-- ,	 WEEK_RANGE_52	VARCHAR(50)
-- ,	 ASK	VARCHAR(50)
-- ,	 AVG_VOLUME	DECIMAL (18,2) Default 0
-- ,	 BETA	DECIMAL (18,2) Default 0
-- ,	 BID	VARCHAR(50)
-- ,	 DAYS_RANGE	VARCHAR(50)
-- ,	 EPS	DECIMAL (18,2) Default 0
-- ,	 EARNINGS_DATE	VARCHAR(50)
-- ,	 EXDIVIDEND_DATE	VARCHAR(50)
-- ,	 FORWARD_DIVIDEND_YIELD	VARCHAR(50)
-- ,	 MARKET_CAP	VARCHAR(50)
-- ,	 OPEN	DECIMAL (18,2) Default 0
-- ,	 PE_RATIO	DECIMAL (18,2) Default 0
-- ,	 PREVIOUS_CLOSE	DECIMAL (18,2) Default 0
-- ,	 QUOTE_PRICE	DECIMAL (18,2) Default 0
-- ,	 VOLUME	DECIMAL (18,2) Default 0
-- , LOAD_DATE DATE	
-- );


DELETE FROM DF_QUOTE_TABLE;
INSERT INTO DF_QUOTE_TABLE

SELECT 
	 ticker
,	 "1y_target_est"
,	 "52_week_range"
,	 ask
,	 avg_volume
,	 beta
,	 bid
,	 days_range
,	 eps
,	 earnings_date
,	 exdividend_date
,	 forward_dividend_yield
,	 market_cap
,	 open

,	pe_ratio
,	 previous_close
,	 quote_price
,	 volume

, CURRENT_DATE AS LOAD_DATE
FROM DF_QUOTE_TABLE_S;

--DF_EARNINGS_Q_RESULTS


-- DROP TABLE IF EXISTS DF_EARNINGS_Q_RESULTS;
-- CREATE TABLE DF_EARNINGS_Q_RESULTS
 -- (
	-- TICKER	VARCHAR(15)
-- ,	REPORT_PERIOD	VARCHAR(15)
-- ,	ACTUAL	DECIMAL (18,2) Default 0
-- ,	ESTIMATE	DECIMAL (18,2) Default 0
-- , LOAD_DATE DATE	
-- );


DELETE FROM DF_EARNINGS_Q_RESULTS;
INSERT INTO DF_EARNINGS_Q_RESULTS
SELECT 
TICKER
,lower (DATE) as REPORT_PERIOD
,ACTUAL
,ESTIMATE
,CURRENT_DATE AS LOAD_DATE
FROM DF_EARNINGS_Q_RESULTS_S;


--df_earnings_q_revenue
-- DROP TABLE IF EXISTS DF_EARNINGS_Q_REVENUE;
-- CREATE TABLE DF_EARNINGS_Q_REVENUE
 -- (
-- TICKER	VARCHAR(15)
-- ,REPORT_PERIOD	VARCHAR(15)
-- ,REVENUE	DECIMAL (18,2) Default 0
-- ,EARNINGS	DECIMAL (18,2) Default 0
-- ,LOAD_DATE DATE
-- );

DELETE FROM DF_EARNINGS_Q_REVENUE;
INSERT INTO DF_EARNINGS_Q_REVENUE
SELECT 
	 TICKER
,DATE
,coalesce(cast (   REVENUE  as decimal (18,2))/1000000.00 , 0) as REVENUE
,coalesce(cast (  EARNINGS  as decimal (18,2))/1000000.00 , 0) as  EARNINGS
, CURRENT_DATE AS LOAD_DATE
FROM DF_EARNINGS_Q_REVENUE_S;



--df_earnings_y_revenue
-- DROP TABLE IF EXISTS df_earnings_y_revenue;
-- CREATE TABLE df_earnings_y_revenue
 -- (
	 -- TICKER VARCHAR(15)
-- ,	REPORT_PERIOD	int
-- ,	 REVENUE	DECIMAL (18,2) Default 0
-- ,	 EARNINGS	DECIMAL (18,2) Default 0
-- , LOAD_DATE DATE
-- );


DELETE FROM DF_EARNINGS_Y_REVENUE;
INSERT INTO DF_EARNINGS_Y_REVENUE
SELECT 
TICKER
,DATE
,COALESCE(   cast (REVENUE  as decimal (18,2)) , 0)  /1000000.00  AS REVENUE
,COALESCE(   cast (EARNINGS  as decimal (18,2)) , 0) /1000000.00  as  EARNINGS
, CURRENT_DATE AS LOAD_DATE
FROM DF_EARNINGS_Y_REVENUE_S;

--df_growth_est


-- DROP TABLE IF EXISTS DF_GROWTH_EST;
-- CREATE TABLE DF_GROWTH_EST
 -- (
 -- TICKER	VARCHAR(15)	
-- , SECTOR	VARCHAR(15)
-- , CURRENT_QTR	DECIMAL (12,6) DEFAULT 0
-- , NEXT_QTR	DECIMAL (12,6) DEFAULT 0
-- , CURRENT_YEAR	DECIMAL (12,6) DEFAULT 0
-- , NEXT_YEAR	DECIMAL (12,6) DEFAULT 0
-- , NEXT_5_YEARS	DECIMAL (12,6) DEFAULT 0
-- , PAST_5_YEARS	DECIMAL (12,6) DEFAULT 0
-- , LOAD_DATE DATE
-- );


DELETE FROM df_growth_est;
INSERT INTO df_growth_est
SELECT 
	 ticker
	 ,lower(index) as SECTOR
,cast ( translate(current_qtr, '%,,', '')  as decimal(12,6))/ 100  as current_qtr
,cast ( translate(next_qtr, '%,,', '')  as decimal(12,6))/ 100 as next_qtr
,cast ( translate( current_year, '%,,', '')  as decimal(12,6))/ 100 as   current_year 
,cast ( translate( next_year, '%,,', '')  as decimal(12,6))/ 100  as next_year
,cast ( translate(  next_5_years, '%,,', '')  as decimal(12,6))/ 100  as next_5_years
,cast ( translate(past_5_years, '%,,', '')  as decimal(12,6))/ 100 as past_5_years

, CURRENT_DATE AS LOAD_DATE
from df_growth_est_s;

--df_revenue_est

-- DROP TABLE IF EXISTS df_revenue_est;
-- CREATE TABLE df_revenue_est
 -- (
	 -- ticker	Varchar(15)
-- ,	 report_Period	Varchar(50)
-- ,	 no_of_analysts	int
-- ,	 avg_estimate	Varchar(20)
-- ,	 low_estimate	Varchar(20)
-- ,	 high_estimate	Varchar(20)
-- ,	 year_ago_sales	Varchar(20)
-- ,	 sales_growth	Decimal (12,6) Default 0
-- , LOAD_DATE DATE
-- );

DELETE FROM df_revenue_est;
INSERT INTO df_revenue_est
SELECT 
	 ticker
,	lower( index ) as report_Period
,	 	 cast ( cast (no_of_analysts as float) as int )
,	 avg_estimate
,	 low_estimate
,	 high_estimate
,	 year_ago_sales
,cast ( translate(sales_growth, '%,,', '')  as decimal(10,2))/ 100 as sales_growth
, CURRENT_DATE AS LOAD_DATE
from df_revenue_est_S;


--df_earnings_est

-- DROP TABLE IF EXISTS df_earnings_est;
-- CREATE TABLE df_earnings_est(
-- TICKER	VARCHAR(15)
-- ,REPORT_PERIOD	VARCHAR(50)
-- ,NO_OF_ANALYSTS	DECIMAL (18,2) Default 0
-- ,AVG_ESTIMATE	DECIMAL (18,2) Default 0
-- ,LOW_ESTIMATE	DECIMAL (18,2) Default 0
-- ,HIGH_ESTIMATE	DECIMAL (18,2) Default 0
-- ,YEAR_AGO_EPS	DECIMAL (18,2) Default 0
-- ,LOAD_DATE DATE

-- );


DELETE FROM DF_EARNINGS_EST;
INSERT INTO DF_EARNINGS_EST
SELECT 
    TICKER
,	lower( index ) as report_Period
    ,NO_OF_ANALYSTS
    ,AVG_ESTIMATE
    ,LOW_ESTIMATE
    ,HIGH_ESTIMATE
    ,YEAR_AGO_EPS
    ,CURRENT_DATE AS LOAD_DATE
FROM DF_EARNINGS_EST_S;


--df_earnings_hist
-- DROP TABLE IF EXISTS df_earnings_hist;
-- CREATE TABLE df_earnings_hist
-- (

	 -- ticker	Varchar(15)
-- ,	report_Period	date
-- ,	 eps_est	Decimal (18,2) Default 0
-- ,	 eps_actual	Decimal (18,2) Default 0
-- ,	 difference	Decimal (18,2) Default 0
-- ,	 surprise	Decimal (12,6) Default 0
-- , LOAD_DATE DATE

-- );

DELETE FROM df_earnings_hist;
INSERT INTO df_earnings_hist
SELECT 
	 ticker 
,	case when  INDEX  like '%Invalid%' then CURRENT_DATE  else TO_TIMESTAMP(INDEX,  'MM-D-YYYY') end as report_Period
,	 eps_est
,	 eps_actual
,	 difference
,cast ( translate(surprise, '%,,', '')  as decimal (12,6) )/ 100 as surprise
,CURRENT_DATE AS LOAD_DATE
from df_earnings_hist_s;



--df_eps_trend
-- DROP TABLE IF EXISTS df_eps_trend;
-- CREATE TABLE df_eps_trend

-- (
 -- ticker	Varchar(15)
-- ,	 eps_trend	Varchar(20)
-- ,	 current_qtr	Decimal (18,2) Default 0
-- ,	 next_qtr	Decimal (18,2) Default 0
-- ,	 current_year	Decimal (18,2) Default 0
-- ,	 next_year	Decimal (18,2) Default 0
-- , LOAD_DATE DATE
-- );

DELETE FROM df_eps_trend;
INSERT INTO df_eps_trend
SELECT 
	 ticker
,	 lower (eps_trend) as eps_trend
, 	 cast (  current_qtr  as decimal(12,6)) as current_qtr
,	cast (  next_qtr  as decimal(12,6)) as next_qtr
,	cast (  current_year as decimal(12,6)) as current_year
,	 cast (  next_year as decimal(12,6)) as next_year
,CURRENT_DATE AS LOAD_DATE
from df_eps_trend_s;


--df_stats

-- DROP TABLE IF EXISTS df_stats;
-- CREATE TABLE df_stats

-- (
	-- ticker	varchar(20)
-- ,	sph_beta_5y_monthly	decimal(18,2)
-- ,	sph_52_week_change	decimal(12,6)
-- ,	sph_sp500_52_week_change	decimal(12,6)
-- ,	sph_52_week_high	decimal(18,2)
-- ,	sph_52_week_low	decimal(18,2)
-- ,	sph_50_day_moving_average	decimal(18,2)
-- ,	sph_200_day_moving_average	decimal(18,2)
-- ,	ss_avg_vol_3month	decimal(18,2)
-- ,	ss_avg_vol_10day	decimal(18,2)
-- ,	ss_shares_outstanding	decimal(18,2)
-- ,	ss_implied_shares_outstanding	decimal(18,2)
-- ,	ss_shares_float	decimal(18,2)
-- ,	ss_perc_held_by_insiders	decimal(12,6)
-- ,	ss_perc_held_by_institutions	decimal(12,6)
-- ,	ss_shares_short_prev_month	decimal(18,2)
-- ,	ss_short_ratio_prev_month	decimal(12,6)
-- ,	ss_short_perc_float_prev_month	decimal(12,6)
-- ,	ss_short_perc_shares_outst_prev_month	decimal(12,6)
-- ,	ss_shares_short_prior_prev_month	decimal(18,2)
-- ,	ds_forward_annual_div_rate	decimal(12,6)
-- ,	ds_forward_annual_div_yield	decimal(12,6)
-- ,	ds_trailing_annual_div_rate	decimal(12,6)
-- ,	ds_trailing_annual_div_yield	decimal(12,6)
-- ,	ds_5_year_average_div_yield	decimal(18,2)
-- ,	ds_payout_ratio	decimal(12,6)
-- ,	ds_div_date	date
-- ,	ds_ex_div_date	date
-- ,	ds_last_split_factor	varchar(20)
-- ,	ds_last_split_date	date
-- ,	fiscal_year_ends	date
-- ,	most_recent_quarter	date
-- ,	p_profit_margin	decimal(12,6)
-- ,	p_operating_margin_ttm	decimal(12,6)
-- ,	me_return_on_assets_ttm	decimal(12,6)
-- ,	me_return_on_equity_ttm	decimal(12,6)
-- ,	is_revenue_ttm	decimal(18,2)
-- ,	is_revenue_per_share_ttm	decimal(18,2)
-- ,	is_quarterly_revenue_growth_yoy	decimal(12,6)
-- ,	is_gross_profit_ttm	decimal(18,2)
-- ,	is_ebitda	decimal(18,2)
-- ,	is_net_income_avi_to_common_ttm	decimal(18,2)
-- ,	is_diluted_eps_ttm	decimal(18,2)
-- ,	is_quarterly_earnings_growth_yoy	decimal(12,6)
-- ,	bs_total_cash_mrq	decimal(18,2)
-- ,	bs_total_cash_per_share_mrq	decimal(18,2)
-- ,	bs_total_debt_mrq	decimal(18,2)
-- ,	bs_total_debtequity_mrq	decimal(18,2)
-- ,	bs_current_ratio_mrq	decimal(18,2)
-- ,	bs_book_value_per_share_mrq	decimal(18,2)
-- ,	cfs_operating_cash_flow_ttm	decimal(18,2)
-- ,	cfs_levered_free_cash_flow_ttm	decimal(18,2)

-- , load_date DATE

-- );


DELETE FROM df_stats;
INSERT INTO df_stats
SELECT 
 ticker  	ticker
,	cast ( 	sph_beta_5y_monthly	as 	decimal(20,2)	)	as 	sph_beta_5y_monthly
,	cast ( 	replace(replace(cast (sph_52_week_change as varchar(100)),'%',''), ',','')	as 	decimal(16,6)	) /100	as 	sph_52_week_change
,	cast ( 	replace(replace(cast (sph_sp500_52_week_change as varchar(100) ),'%',''), ',','')	as 	decimal(16,6)	) /100	as 	sph_sp500_52_week_change
,	cast ( 	sph_52_week_high	as 	decimal(20,2)	)	as 	sph_52_week_high
,	cast ( 	sph_52_week_low	as 	decimal(20,2)	)	as 	sph_52_week_low
,	cast ( 	sph_50_day_moving_average	as 	decimal(20,2)	)	as 	sph_50_day_moving_average
,	cast ( 	sph_200_day_moving_average	as 	decimal(20,2)	)	as 	sph_200_day_moving_average
, case when lower (ss_avg_vol_3month) like '%k%'  then   cast (replace( lower (ss_avg_vol_3month), 'k', '' )as  decimal (20,2)) *1000
 when lower (ss_avg_vol_3month)like '%m%'  then   cast (replace(lower (ss_avg_vol_3month), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (ss_avg_vol_3month) like '%b%'  then   cast (replace( lower (ss_avg_vol_3month), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_avg_vol_3month
, case when lower (ss_avg_vol_10day) like '%k%'  then   cast (replace( lower (ss_avg_vol_10day), 'k', '' )as  decimal (20,2)) *1000
 when lower (ss_avg_vol_10day)like '%m%'  then   cast (replace(lower (ss_avg_vol_10day), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (ss_avg_vol_10day) like '%b%'  then   cast (replace( lower (ss_avg_vol_10day), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_avg_vol_10day
, case when lower (ss_shares_outstanding) like '%k%'  then   cast (replace( lower (ss_shares_outstanding), 'k', '' )as  decimal (20,2)) *1000
 when lower (ss_shares_outstanding)like '%m%'  then   cast (replace(lower (ss_shares_outstanding), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (ss_shares_outstanding) like '%b%'  then   cast (replace( lower (ss_shares_outstanding), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_shares_outstanding
, case when lower (cast (ss_implied_shares_outstanding as text )) like '%k%'  then   cast (replace( lower (cast (ss_implied_shares_outstanding as text )), 'k', '' )as  decimal (20,2)) *1000
 when lower (cast (ss_implied_shares_outstanding as text ))like '%m%'  then   cast (replace(lower (cast (ss_implied_shares_outstanding as text )), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (cast (ss_implied_shares_outstanding as text )) like '%b%'  then   cast (replace( lower (cast (ss_implied_shares_outstanding as text )), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_implied_shares_outstanding
, case when lower (ss_shares_float) like '%k%'  then   cast (replace( lower (ss_shares_float), 'k', '' )as  decimal (20,2)) *1000
 when lower (ss_shares_float)like '%m%'  then   cast (replace(lower (ss_shares_float), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (ss_shares_float) like '%b%'  then   cast (replace( lower (ss_shares_float), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_shares_float
,	cast ( 	replace(replace(ss_perc_held_by_insiders,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	ss_perc_held_by_insiders
,	cast ( 	replace(replace(ss_perc_held_by_institutions,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	ss_perc_held_by_institutions
, case when lower (ss_shares_short_prev_month) like '%k%'  then   cast (replace( lower (ss_shares_short_prev_month), 'k', '' )as  decimal (20,2)) *1000
 when lower (ss_shares_short_prev_month)like '%m%'  then   cast (replace(lower (ss_shares_short_prev_month), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (ss_shares_short_prev_month) like '%b%'  then   cast (replace( lower (ss_shares_short_prev_month), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_shares_short_prev_month
,	cast ( 	ss_short_ratio_prev_month	as 	decimal(16,6)	)	as 	ss_short_ratio_prev_month
,	cast ( 	replace(replace(ss_short_perc_float_prev_month,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	ss_short_perc_float_prev_month
,	cast ( 	replace(ss_short_perc_shares_outst_prev_month ,'%','')	as 	decimal(16,6)	)	as 	ss_short_perc_shares_outst_prev_month
, case when lower (ss_shares_short_prior_prev_month) like '%k%'  then   cast (replace( lower (ss_shares_short_prior_prev_month), 'k', '' )as  decimal (20,2)) *1000
 when lower (ss_shares_short_prior_prev_month)like '%m%'  then   cast (replace(lower (ss_shares_short_prior_prev_month), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (ss_shares_short_prior_prev_month) like '%b%'  then   cast (replace( lower (ss_shares_short_prior_prev_month), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	ss_shares_short_prior_prev_month
,	cast ( 	ds_forward_annual_div_rate	as 	decimal(16,6)	)	as 	ds_forward_annual_div_rate
,	cast ( 	replace(replace(ds_forward_annual_div_yield,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	ds_forward_annual_div_yield
,	cast ( 	ds_trailing_annual_div_rate	as 	decimal(16,6)	)	as 	ds_trailing_annual_div_rate
,	cast ( 	replace(replace(ds_trailing_annual_div_yield,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	ds_trailing_annual_div_yield
,	cast ( 	ds_5_year_average_div_yield	as 	decimal(20,2)	)	as 	ds_5_year_average_div_yield
,	cast ( 	replace(replace(ds_payout_ratio,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	ds_payout_ratio
,	cast ( 	ds_div_date	as 	date	)	as 	ds_div_date
,	cast ( 	ds_ex_div_date	as 	date	)	as 	ds_ex_div_date
,	cast ( 	ds_last_split_factor	as 	varchar(20)	)	as 	ds_last_split_factor
,	cast ( 	ds_last_split_date	as 	date	)	as 	ds_last_split_date
,	cast ( 	fiscal_year_ends	as 	date	)	as 	fiscal_year_ends
,	cast ( 	most_recent_quarter	as 	date	)	as 	most_recent_quarter
,	cast ( 	replace(replace(p_profit_margin,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	p_profit_margin
,	cast ( 	replace(replace(p_operating_margin_ttm,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	p_operating_margin_ttm
,	cast ( 	replace(replace(me_return_on_assets_ttm,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	me_return_on_assets_ttm
,	cast ( 	replace(replace(me_return_on_equity_ttm,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	me_return_on_equity_ttm
, case when lower (is_revenue_ttm) like '%k%'  then   cast (replace( lower (is_revenue_ttm), 'k', '' )as  decimal (20,2)) *1000
 when lower (is_revenue_ttm)like '%m%'  then   cast (replace(lower (is_revenue_ttm), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (is_revenue_ttm) like '%b%'  then   cast (replace( lower (is_revenue_ttm), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	is_revenue_ttm
,	cast ( 	is_revenue_per_share_ttm	as 	decimal(20,2)	)	as 	is_revenue_per_share_ttm
,	cast ( 	replace(replace(is_quarterly_revenue_growth_yoy,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	is_quarterly_revenue_growth_yoy
, case when lower (is_gross_profit_ttm) like '%k%'  then   cast (replace( lower (is_gross_profit_ttm), 'k', '' )as  decimal (20,2)) *1000
 when lower (is_gross_profit_ttm)like '%m%'  then   cast (replace(lower (is_gross_profit_ttm), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (is_gross_profit_ttm) like '%b%'  then   cast (replace( lower (is_gross_profit_ttm), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	is_gross_profit_ttm
, case when lower (is_ebitda) like '%k%'  then   cast (replace( lower (is_ebitda), 'k', '' )as  decimal (20,2)) *1000
 when lower (is_ebitda)like '%m%'  then   cast (replace(lower (is_ebitda), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (is_ebitda) like '%b%'  then   cast (replace( lower (is_ebitda), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	is_ebitda
, case when lower (is_net_income_avi_to_common_ttm) like '%k%'  then   cast (replace( lower (is_net_income_avi_to_common_ttm), 'k', '' )as  decimal (20,2)) *1000
 when lower (is_net_income_avi_to_common_ttm)like '%m%'  then   cast (replace(lower (is_net_income_avi_to_common_ttm), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (is_net_income_avi_to_common_ttm) like '%b%'  then   cast (replace( lower (is_net_income_avi_to_common_ttm), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	is_net_income_avi_to_common_ttm
,	cast ( 	is_diluted_eps_ttm	as 	decimal(20,2)	)	as 	is_diluted_eps_ttm
,	cast ( 	replace(replace(is_quarterly_earnings_growth_yoy,'%',''), ',','')	as 	decimal(16,6)	) /100	as 	is_quarterly_earnings_growth_yoy
, case when lower (bs_total_cash_mrq) like '%k%'  then   cast (replace( lower (bs_total_cash_mrq), 'k', '' )as  decimal (20,2)) *1000
 when lower (bs_total_cash_mrq)like '%m%'  then   cast (replace(lower (bs_total_cash_mrq), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (bs_total_cash_mrq) like '%b%'  then   cast (replace( lower (bs_total_cash_mrq), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	bs_total_cash_mrq
,	cast ( 	bs_total_cash_per_share_mrq	as 	decimal(20,2)	)	as 	bs_total_cash_per_share_mrq
, case when lower (bs_total_debt_mrq) like '%k%'  then   cast (replace( lower (bs_total_debt_mrq), 'k', '' )as  decimal (20,2)) *1000
 when lower (bs_total_debt_mrq)like '%m%'  then   cast (replace(lower (bs_total_debt_mrq), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (bs_total_debt_mrq) like '%b%'  then   cast (replace( lower (bs_total_debt_mrq), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	bs_total_debt_mrq
,	cast ( 	bs_total_debtequity_mrq	as 	decimal(20,2)	)	as 	bs_total_debtequity_mrq
,	cast ( 	bs_current_ratio_mrq	as 	decimal(20,2)	)	as 	bs_current_ratio_mrq
,	cast ( 	bs_book_value_per_share_mrq	as 	decimal(20,2)	)	as 	bs_book_value_per_share_mrq
, case when lower (cfs_operating_cash_flow_ttm) like '%k%'  then   cast (replace( lower (cfs_operating_cash_flow_ttm), 'k', '' )as  decimal (20,2)) *1000
 when lower (cfs_operating_cash_flow_ttm)like '%m%'  then   cast (replace(lower (cfs_operating_cash_flow_ttm), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (cfs_operating_cash_flow_ttm) like '%b%'  then   cast (replace( lower (cfs_operating_cash_flow_ttm), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	cfs_operating_cash_flow_ttm
, case when lower (cfs_levered_free_cash_flow_ttm) like '%k%'  then   cast (replace( lower (cfs_levered_free_cash_flow_ttm), 'k', '' )as  decimal (20,2)) *1000
 when lower (cfs_levered_free_cash_flow_ttm)like '%m%'  then   cast (replace(lower (cfs_levered_free_cash_flow_ttm), 'm', '' )as  decimal (20,2)) *1000000 
 when lower (cfs_levered_free_cash_flow_ttm) like '%b%'  then   cast (replace( lower (cfs_levered_free_cash_flow_ttm), 'b', '' )as  decimal (20,2)) *1000000000 
 else '0' end  as 	cfs_levered_free_cash_flow_ttm


,CURRENT_DATE AS LOAD_DATE
from public.df_stats_s t1;

$$;

