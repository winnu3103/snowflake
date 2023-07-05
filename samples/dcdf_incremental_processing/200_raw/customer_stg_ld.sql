--------------------------------------------------------------------
--  Purpose: load into stage table
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
queries for reviewing loaded data

select * from customer_stg limit 1000;
select count(*), count( distinct o_orderkey ) from customer_stg;

select dw_file_name, count(*), sum( count(*) ) over() from customer_stg group by 1 order by 1;


list @~ pattern = 'customer.*';
list @~ pattern = 'customer/data.*\.csv\.gz';

put file:///users/<user_name>/test/data_bad.csv @~/customer 
*/

-- Truncate/Load Pattern
-- truncate stage prior to bulk load

CREATE TABLE customer_stg (
  c_custkey INT,
  change_date DATE,
  c_name STRING,
  c_address STRING,
  c_nationkey INT,
  c_phone STRING,
  c_acctbal FLOAT,
  c_mktsegment STRING,
  c_comment STRING,
  dw_file_name STRING,
  dw_file_row_no INT,
  dw_load_ts TIMESTAMP
);
truncate table customer_stg;

-- perform bulk load
CREATE OR REPLACE PROCEDURE stg_customer_data()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    EXECUTE IMMEDIATE '
copy into
    customer_stg
from
    (
    select
         s.$1                                            -- c_custkey
        ,s.$2                                            -- change_date
        ,s.$3                                            -- c_name
        ,s.$4                                            -- c_address
        ,s.$5                                            -- c_nationkey
        ,s.$6                                            -- c_phone
        ,s.$7                                            -- c_acctbal
        ,s.$8                                            -- c_mktsegment
        ,s.$9                                            -- c_comment
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @~ s
    )
purge         = true
pattern       = '.*customer/data.*\.csv\.gz'
file_format   = ( type=csv field_optionally_enclosed_by = '"' )
on_error      = skip_file
--validation_mode = return_all_errors
;';

    RETURN 'COPY INTO statement executed successfully.';
END;
$$;

--
-- review history of load errors
--
select 
    *
from 
    table(information_schema.copy_history(table_name=>'CUSTOMER_STG', start_time=> dateadd(hours, -1, current_timestamp())))
where
    status = 'Loaded'
order by
    last_load_time desc
;

select *
from dev_webinar_orders_rl_db.tpch.customer_stg
where c_custkey in (50459048);
