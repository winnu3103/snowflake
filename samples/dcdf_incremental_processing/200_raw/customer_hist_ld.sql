--------------------------------------------------------------------
--  Purpose: insert new and modified values into history table
--      This shows an insert-only pattern where every version of a 
--      received row is kept.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_orders_rl_db;
use schema   tpch;
use warehouse dev_webinar_wh;

/* Validation Queries
select * from customer_hist limit 1000;

total rows and distinct hash diff should always be identical
distinct hash key count will always be less

select count(*), count( distinct c_custkey ) from customer_stg;

select count(*), count( distinct dw_customer_shk ), count( distinct dw_hash_diff ) from customer_hist;
*/
CREATE TABLE customer_hist (
  dw_customer_shk     BINARY(20),
  dw_hash_diff        BINARY(20),
  dw_version_ts       TIMESTAMP,
  change_date         DATE,
  c_custkey           INTEGER,
  c_name              STRING,
  c_address           STRING,
  c_nationkey         INTEGER,
  c_phone             STRING,
  c_acctbal           FLOAT,
  c_mktsegment        STRING,
  c_comment           STRING,
  dw_file_name        STRING,
  dw_file_row_no      INTEGER,
  dw_load_ts          TIMESTAMP
);

CREATE OR REPLACE PROCEDURE process_customer_data()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    l_start_dt DATE;
    l_end_dt DATE;

BEGIN

    FOR record IN (SELECT start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) ORDER BY 1) DO
        l_start_dt := record.start_dt;
        l_end_dt := record.end_dt;

        INSERT INTO customer_hist
        WITH l_stg AS (
            SELECT
                SHA1_BINARY(s.c_custkey || TO_CHAR(s.change_date, 'yyyymmdd')) AS dw_customer_shk,
                SHA1_BINARY(CONCAT(s.c_custkey || TO_CHAR(s.change_date, 'yyyymmdd'),
                    '|', COALESCE(TO_CHAR(s.change_date, 'yyyymmdd'), '~'),
                    '|', COALESCE(s.c_name, '~'),
                    '|', COALESCE(s.c_address, '~'),
                    '|', COALESCE(TO_CHAR(s.c_nationkey), '~'),
                    '|', COALESCE(s.c_phone, '~'),
                    '|', COALESCE(TO_CHAR(s.c_acctbal), '~'),
                    '|', COALESCE(s.c_mktsegment, '~'),
                    '|', COALESCE(s.c_comment, '~')
                )) AS dw_hash_diff,
                s.*
            FROM customer_stg s
            WHERE s.change_date >= l_start_dt
                AND s.change_date < l_end_dt
        ),
        l_deduped AS (
            SELECT *
            FROM l_stg
            QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_hash_diff ORDER BY change_date DESC, dw_file_row_no) = 1
        )
        SELECT
            s.dw_customer_shk,
            s.dw_hash_diff,
            s.dw_load_ts AS dw_version_ts,
            s.change_date,
            s.c_custkey,
            s.c_name,
            s.c_address,
            s.c_nationkey,
            s.c_phone,
            s.c_acctbal,
            s.c_mktsegment,
            s.c_comment,
            s.dw_file_name,
            s.dw_file_row_no,
            CURRENT_TIMESTAMP() AS dw_load_ts
        FROM l_deduped s
        WHERE s.dw_hash_diff NOT IN (
            SELECT dw_hash_diff FROM customer_hist
        )
        ORDER BY c_custkey;

    END FOR;

    RETURN 'SUCCESS';

END;
$$;

select *
from dev_webinar_orders_rl_db.tpch.customer_hist
where c_custkey in (50459048)
order by 5;
