--------------------------------------------------------------------
--  Purpose: merge new and modified values into target table
--      This shows a merge pattern where only the most recent version
--      of a given primary keys record is kept.
--
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
truncate table partsupp;

select * from partsupp limit 1000;

all 3 counts should be identical

select count(*), count( distinct p_partsuppkey ) from partsupp_stg;

select count(*), count( distinct dw_partsupp_shk ), count( distinct dw_hash_diff ) from partsupp;
*/
CREATE or replace TABLE partsupp (
  dw_partsupp_shk    VARCHAR(64),
  dw_hash_diff       VARCHAR(999),
  dw_version_ts      TIMESTAMP,
  ps_partkey         INTEGER,
  ps_suppkey         INTEGER,
  ps_availqty        INTEGER,
  ps_supplycost      DECIMAL(15,2),
  ps_comment         VARCHAR(79),
  last_modified_dt   TIMESTAMP,
  dw_file_name       VARCHAR(255),
  dw_file_row_no     INTEGER,
  dw_load_ts         TIMESTAMP,
  dw_update_ts       TIMESTAMP
);

execute immediate $$

begin
    --
    -- Merge Pattern
    --
    -- 
    merge into partsupp tgt    
    using (
        with l_stg as (
            select
                HEX_ENCODE(sha1_binary(concat(s.ps_partkey, '|' , s.ps_suppkey))) as dw_partsupp_shk,
                HEX_ENCODE(sha1_binary(concat(s.ps_partkey, '|' , s.ps_suppkey, '|' , coalesce(s.ps_availqty, 0), '|' , coalesce(s.ps_supplycost, 0), '|' , coalesce(s.ps_comment, '~')))) as dw_hash_diff,
                s.*
            from partsupp_stg s
        ),
    --

        l_deduped as (
            select
                *
            from l_stg s
            qualify row_number() over(partition by dw_partsupp_shk order by s.last_modified_dt desc) = 1
        ),
        l_tgt as (
            select *
            from partsupp
        )
        select
            current_timestamp() as dw_version_ts,
            s.*
        from l_deduped s
        left join l_tgt t on t.dw_partsupp_shk = s.dw_partsupp_shk
        where t.dw_partsupp_shk is null
            or (t.last_modified_dt < s.last_modified_dt and t.dw_hash_diff != s.dw_hash_diff)
        order by s.ps_partkey
    ) src on tgt.dw_partsupp_shk = src.dw_partsupp_shk
    when matched then update set
        tgt.dw_hash_diff = src.dw_hash_diff,
        tgt.dw_version_ts = src.dw_version_ts,
        tgt.ps_availqty = src.ps_availqty,
        tgt.ps_supplycost = src.ps_supplycost,
        tgt.ps_comment = src.ps_comment,
        tgt.last_modified_dt = src.last_modified_dt,
        tgt.dw_file_name = src.dw_file_name,
        tgt.dw_file_row_no = src.dw_file_row_no,
        tgt.dw_update_ts = src.dw_version_ts
    when not matched then insert (
        dw_partsupp_shk,
        dw_hash_diff,
        dw_version_ts,
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        --ps_comment,
        last_modified_dt,
        dw_file_name,
        dw_file_row_no,
        dw_load_ts,
        dw_update_ts
    )
    values (
        src.dw_partsupp_shk,
        src.dw_hash_diff,
        src.dw_version_ts,
        src.ps_partkey,
        src.ps_suppkey,
        src.ps_availqty,
        src.ps_supplycost,
        -- src.ps_comment,
        src.last_modified_dt,
        src.dw_file_name,
        src.dw_file_row_no,
        src.dw_load_ts,
        src.dw_version_ts
    );
    return 'SUCCESS';
end;
$$
;

select * 
from dev_webinar_orders_rl_db.tpch.partsupp
where ps_partkey in ( 105237594, 128236374);