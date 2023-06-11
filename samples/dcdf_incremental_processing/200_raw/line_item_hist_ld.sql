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
select * from line_item_hist limit 1000;

total rows and distinct hash diff should always be identical
distinct hash key count will always be less

select count(*), count( distinct o_orderkey ) from line_item_stg;

select count(*), count( distinct dw_line_item_shk ), count( distinct dw_hash_diff ) from line_item_hist;
*/

CREATE TABLE line_item_hist (
    dw_line_item_shk VARCHAR(40),
    dw_hash_diff VARCHAR(40),
    dw_load_ts TIMESTAMP,
    l_orderkey INTEGER,
    o_orderdate DATE,
    l_partkey INTEGER,
    l_suppkey INTEGER,
    l_linenumber INTEGER,
    l_quantity DECIMAL(10,2),
    l_extendedprice DECIMAL(10,2),
    l_discount DECIMAL(10,2),
    l_tax DECIMAL(10,2),
    l_returnflag CHAR(1),
    l_linestatus CHAR(1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR(25),
    l_shipmode VARCHAR(10),
    l_comment VARCHAR(100),
    last_modified_dt TIMESTAMP,
    dw_file_name VARCHAR(100),
    dw_file_row_no INTEGER,
    dw_load_ts_updated TIMESTAMP
);


EXECUTE IMMEDIATE '
DECLARE
  l_start_dt DATE;
  l_end_dt   DATE;
  c1 CURSOR FOR SELECT start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f(''week'')) ORDER BY 1;

BEGIN

  FOR record IN c1 DO
    l_start_dt := record.start_dt;
    l_end_dt   := record.end_dt;

    INSERT INTO line_item_hist
    WITH l_stg AS
    (
        SELECT
            HEX_ENCODE(SHA1_BINARY(CONCAT(s.l_orderkey, ''|'', s.l_linenumber))) AS dw_line_item_shk,
            HEX_ENCODE(SHA1_BINARY(CONCAT(
                s.l_orderkey,
                ''|'',
                COALESCE(TO_CHAR(s.o_orderdate, ''yyyymmdd''), ''~''),
                ''|'',
                s.l_linenumber,
                ''|'',
                COALESCE(TO_CHAR(s.l_partkey), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_suppkey), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_quantity), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_extendedprice), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_discount), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_tax), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_returnflag), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_linestatus), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_shipdate, ''yyyymmdd''), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_commitdate, ''yyyymmdd''), ''~''),
                ''|'',
                COALESCE(TO_CHAR(s.l_receiptdate, ''yyyymmdd''), ''~''),
                ''|'',
                COALESCE(s.l_shipinstruct, ''~''),
                ''|'',
                COALESCE(s.l_shipmode, ''~''),
                ''|'',
                COALESCE(s.l_comment, ''~'')
            ))) AS dw_hash_diff,
            s.*
        FROM
            line_item_stg s
        WHERE
            s.o_orderdate >= :l_start_dt
            AND s.o_orderdate < :l_end_dt
    ),
    l_deduped AS
    (
        SELECT
            *
        FROM
            l_stg
        QUALIFY
            ROW_NUMBER() OVER (PARTITION BY dw_hash_diff ORDER BY last_modified_dt DESC, dw_file_row_no) = 1
    )
    SELECT
        s.dw_line_item_shk,
        s.dw_hash_diff,
        s.dw_load_ts AS dw_version_ts,
        s.l_orderkey,
        s.o_orderdate,
        s.l_partkey,
        s.l_suppkey,
        s.l_linenumber,
        s.l_quantity,
        s.l_extendedprice,
        s.l_discount,
        s.l_tax,
        s.l_returnflag,
        s.l_linestatus,
        s.l_shipdate,
        s.l_commitdate,
        s.l_receiptdate,
        s.l_shipinstruct,
        s.l_shipmode,
        s.l_comment,
        s.last_modified_dt,
        s.dw_file_name,
        s.dw_file_row_no,
        CURRENT_TIMESTAMP AS dw_load_ts
    FROM
        l_deduped s
    WHERE
        TO_VARCHAR(s.dw_line_item_shk) NOT IN (
            SELECT TO_VARCHAR(dw_line_item_shk) FROM line_item_hist
            WHERE
                o_orderdate >= :l_start_dt
                AND o_orderdate < :l_end_dt
        )
    ORDER BY
        o_orderdate;

  END FOR;

  RETURN ''SUCCESS'';

END;';
