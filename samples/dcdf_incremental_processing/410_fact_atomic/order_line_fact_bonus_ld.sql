--------------------------------------------------------------------
--  Purpose: delete/insert pattern
--      This shows an insert-only pattern 
--
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role      sysadmin;
use database  dev_webinar_pl_db;
use schema    main;
use warehouse dev_webinar_wh;
CREATE TABLE order_line_fact_bonus (
  dw_line_item_shk BINARY(20),
  o_orderdate DATE,
  dw_order_shk BINARY(20),
  dw_part_shk BINARY(20),
  dw_supplier_shk BINARY(20),
  dw_customer_shk BINARY(20),
  quantity DECIMAL(15, 2),
  extendedprice DECIMAL(15, 2),
  discount DECIMAL(15, 2),
  tax DECIMAL(15, 2),
  returnflag VARCHAR(1),
  linestatus VARCHAR(1),
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  margin_amt DECIMAL(15, 2),
  dw_load_ts TIMESTAMP
);
CREATE OR REPLACE PROCEDURE process_order_data()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  l_start_dt DATE;
  l_end_dt DATE;
  c1 CURSOR FOR SELECT start_dt, end_dt FROM TABLE(tasksample.public.dw_delta_date_range_f('week')) ORDER BY 1;
BEGIN
    
  FOR record IN c1 DO
    l_start_dt := record.start_dt;
    l_end_dt := record.end_dt;

    -- Delete the records using the logical partition
    DELETE FROM order_line_fact_bonus
    WHERE orderdate >= l_start_dt
      AND orderdate < l_end_dt;
      
    -- Insert the logical partitioned records into the table
    INSERT INTO order_line_fact_bonus
    WITH l_cust AS (
      SELECT
          dw_customer_shk,
          c_custkey,
          change_date AS active_date,
          NVL(LEAD(change_date) OVER (PARTITION BY c_custkey ORDER BY change_date), TO_DATE('1/1/2888', 'mm/dd/yyyy')) AS inactive_date
      FROM tasksample.public.customer_hist
    )
    SELECT
        li.dw_line_item_shk,
        o.o_orderdate,
        o.dw_order_shk,
        p.dw_part_shk,
        s.dw_partsupp_shk,
        c.dw_customer_shk,
        li.l_quantity AS quantity,
        li.l_extendedprice AS extendedprice,
        li.l_discount AS discount,
        li.l_tax AS tax,
        li.l_returnflag AS returnflag,
        li.l_linestatus AS linestatus,
        li.l_shipdate,
        li.l_commitdate,
        li.l_receiptdate,
        lim.margin_amt,
        CURRENT_TIMESTAMP() AS dw_load_ts
    FROM
        tasksample.public..line_item li
        JOIN tasksample.public.orders o ON o.o_orderkey = li.l_orderkey
        JOIN tasksample.public.line_item_margin lim ON lim.dw_line_item_shk = li.dw_line_item_shk
        LEFT OUTER JOIN tasksample.public.part p ON p.p_partkey = li.l_partkey
        LEFT OUTER JOIN tasksample.public.partsupp s ON s.ps_suppkey = li.l_suppkey
        LEFT OUTER JOIN l_cust c ON o.o_custkey = c.c_custkey AND o.o_orderdate >= c.active_date AND o.o_orderdate < c.inactive_date
    WHERE
        li.o_orderdate >= l_start_dt
        AND li.o_orderdate < l_end_dt
    ORDER BY o.o_orderdate;
  
  END FOR;
  
  RETURN 'SUCCESS';
  
END;
$$;


select 
     c.c_custkey
    ,c.c_name
    ,c.c_acctbal
    ,olf.*
from dev_webinar_pl_db.main.customer_dm c
   join dev_webinar_pl_db.main.order_line_fact_bonus olf
     on olf.dw_customer_shk = c.dw_customer_shk
where c.c_custkey in (50459048);
order by olf.orderdate;