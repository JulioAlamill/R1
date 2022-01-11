-- PROCEDURE: public.run_etl_fundamentals_update()

-- DROP PROCEDURE public.run_etl_fundamentals_update();

CREATE OR REPLACE PROCEDURE public.run_etl_fundamentals_update(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN

--20210630

--1.-etl01_db_tidy_yahoo_data.sql
call  etl01_db_tidy_yahoo_data ();
--2.-etl02_fundamental_H_update.sql
call  etl02_fundamental_H_update ();
--3.-etl03_kpi_H_update.sql
call  etl03_quote_h_update ();
--4 20210730
call  etl04_stats_h_update () ;

--5.-etl05_df_fundamentals_s1.sql
call  etl05_df_fundamentals_s1 ();

--7.- Control 
call ut_fundamentals_control(  );

END
$BODY$;
