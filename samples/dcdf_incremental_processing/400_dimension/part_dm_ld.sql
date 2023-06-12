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
create or replace table part_dm (
    dw_part_shk        binary(20),
    p_partkey          binary(20),
    part_name          VARCHAR(100),
    mfgr               VARCHAR(100),
    brand              VARCHAR(100),
    type               VARCHAR(100),
    size               NUMBER(38, 0),
    container          VARCHAR(100),
    retail_price       FLOAT,
    comment            VARCHAR(1000),
    first_orderdate    DATE,
    last_modified_dt   TIMESTAMP,
    dw_load_ts         TIMESTAMP,
    dw_update_ts       TIMESTAMP
);
execute immediate $$

begin
    
   insert overwrite into part_dm
   select
       p.dw_part_shk
      ,sha1_binary(p.p_partkey) as p_partkey
      ,p.p_name as part_name
      ,p.p_mfgr as mfgr
      ,p.p_brand as brand
      ,p.p_type as type
      ,p.p_size as size
      ,p.p_container as container
      ,p.p_retailprice as retail_price
      ,p.p_comment as comment
      ,d.first_orderdate
      ,p.last_modified_dt
      ,p.dw_load_ts
      ,p.dw_update_ts
   from
       dev_webinar_orders_rl_db.tpch.part p
       left join dev_webinar_orders_rl_db.tpch.part_first_order_dt d
         on d.dw_part_shk = p.dw_part_shk;
  
  return 'SUCCESS';

end;
$$
;


select *
from dev_webinar_pl_db.main.part_dm p
where p_partkey in ( 105237594, 128236374);