CREATE OR REPLACE PROCEDURE run_proc_r1_KPI_versions (MAX_DATE DATE  DEFAULT CURRENT_DATE )

LANGUAGE plpgsql
AS $procedure$
BEGIN




-- Fundamentals S2
call  p01_df_fundamentals ( cast  (DATE_PART('YEAR', MAX_DATE) as INT )) ;

-- Price , Buy Sell, and Recomendations 
call p02_df_price_bs_rec (MAX_DATE);

--KPI 
call p03_df_kpi (MAX_DATE);

--  porfolios

call p04_porfolios_update ( );

END
$procedure$
                                                                     