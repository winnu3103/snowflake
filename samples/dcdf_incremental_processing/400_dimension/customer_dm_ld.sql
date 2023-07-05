--------------------------------------------------------------------
--  Purpose: data presentation
--      insert/overwrite pattern for part_dm
--
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_pl_db;
use schema   main;
use warehouse dev_webinar_wh;
CREATE TABLE customer_dm (
  dw_customer_shk  BINARY(20),
  c_custkey        INTEGER,
  c_name           STRING,
  c_address        STRING,
  c_nationkey      INTEGER,
  c_phone          STRING,
  c_acctbal        FLOAT,
  c_mktsegment     STRING,
  comment          STRING,
  dw_load_ts       TIMESTAMP,
  dw_update_ts     TIMESTAMP
);
CREATE OR REPLACE PROCEDURE dim_customer_data()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    
    INSERT OVERWRITE INTO customer_dm
    SELECT
        p.dw_customer_shk,
        p.c_custkey,
        p.c_name,
        p.c_address,
        p.c_nationkey,
        p.c_phone,
        p.c_acctbal,
        p.c_mktsegment,
        p.c_comment AS comment,
        p.dw_load_ts,
        p.dw_load_ts AS dw_update_ts
    FROM tasksample.public.customer_hist p;

    RETURN 'SUCCESS';
    
END;
$$;


select *
from dev_webinar_pl_db.main.customer_dm p
where c_custkey in (50459048);