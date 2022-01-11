CREATE OR REPLACE PROCEDURE etl_r1_daily_update ()

LANGUAGE plpgsql
AS $procedure$
BEGIN


-- Fundamentals S2
call  ETL07_DF_FUNDAMENTALS_S2 ();

--KPI 
call p02_df_kpi ();

END
$procedure$
