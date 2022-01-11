
CREATE OR REPLACE PROCEDURE etl04_stats_h_update ()
LANGUAGE SQL
as $$



--DF_QUOTE_H
DELETE FROM DF_STATS_H WHERE LOAD_DATE IN (
SeLECT LOAD_DATE
FROM DF_STATS); --delet if already load data in the same day 

--insert new data 
 
INSERT INTO DF_STATS_H

 SELECT 
	ticker
,	sph_beta_5y_monthly
,	sph_52_week_change
,	sph_sp500_52_week_change
,	sph_52_week_high
,	sph_52_week_low
,	sph_50_day_moving_average
,	sph_200_day_moving_average
,	ss_avg_vol_3month
,	ss_avg_vol_10day
,	ss_shares_outstanding
,	ss_implied_shares_outstanding
,	ss_shares_float
,	ss_perc_held_by_insiders
,	ss_perc_held_by_institutions
,	ss_shares_short_prev_month
,	ss_short_ratio_prev_month
,	ss_short_perc_float_prev_month
,	ss_short_perc_shares_outst_prev_month
,	ss_shares_short_prior_prev_month
,	ds_forward_annual_div_rate
,	ds_forward_annual_div_yield
,	ds_trailing_annual_div_rate
,	ds_trailing_annual_div_yield
,	ds_5_year_average_div_yield
,	ds_payout_ratio
,	ds_div_date
,	ds_ex_div_date
,	ds_last_split_factor
,	ds_last_split_date
,	fiscal_year_ends
,	most_recent_quarter
,	p_profit_margin
,	p_operating_margin_ttm
,	me_return_on_assets_ttm
,	me_return_on_equity_ttm
,	is_revenue_ttm
,	is_revenue_per_share_ttm
,	is_quarterly_revenue_growth_yoy
,	is_gross_profit_ttm
,	is_ebitda
,	is_net_income_avi_to_common_ttm
,	is_diluted_eps_ttm
,	is_quarterly_earnings_growth_yoy
,	bs_total_cash_mrq
,	bs_total_cash_per_share_mrq
,	bs_total_debt_mrq
,	bs_total_debtequity_mrq
,	bs_current_ratio_mrq
,	bs_book_value_per_share_mrq
,	cfs_operating_cash_flow_ttm
,	cfs_levered_free_cash_flow_ttm
,	load_date

from DF_STATS;

-- Index created 
-- CREATE  INDEX DF_QUOTE_H_IX_1 ON DF_QUOTE_H  (LOAD_DATE);  
-- CREATE  INDEX DF_QUOTE_H_IX_2 ON DF_QUOTE_H  (TICKER);  
-- CREATE  INDEX DF_QUOTE_H_IX_3 ON DF_QUOTE_H  (TICKER , LOAD_DATE desc  );  
 $$;
                                                                            
