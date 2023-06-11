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
truncate table line_item;

select * from line_item limit 1000;

all 3 counts should be identical

select count(*), count( distinct l_orderkey ), min( o_orderdate ), max( o_orderdate ) from line_item_stg;

select count(*), count( distinct dw_line_item_shk ), count( distinct dw_hash_diff ) from line_item;
*/
CREATE TABLE line_item (
    dw_line_item_shk    VARCHAR(40),
    dw_hash_diff        VARCHAR(40),
    dw_version_ts       TIMESTAMP,
    l_orderkey          INTEGER,
    o_orderdate         DATE,
    l_partkey           INTEGER,
    l_suppkey           INTEGER,
    l_linenumber        INTEGER,
    l_quantity          DECIMAL(12, 2),
    l_extendedprice     DECIMAL(12, 2),
    l_discount          DECIMAL(12, 2),
    l_tax               DECIMAL(12, 2),
    l_returnflag        CHAR(1),
    l_linestatus        CHAR(1),
    l_shipdate          DATE,
    l_commitdate        DATE,
    l_receiptdate       DATE,
    l_shipinstruct      VARCHAR(25),
    l_shipmode          VARCHAR(10),
    l_comment           VARCHAR(44),
    last_modified_dt    TIMESTAMP,
    dw_file_name        VARCHAR(100),
    dw_file_row_no      INTEGER,
    dw_load_ts          TIMESTAMP,
    dw_update_ts        TIMESTAMP
);

-- Use Anonymous block SQL Scripting
EXECUTE IMMEDIATE '
DECLARE
  l_start_dt DATE;
  l_end_dt   DATE;
  c1 CURSOR FOR SELECT start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f(''week'')) ORDER BY 1;
BEGIN
  FOR record IN c1 LOOP
    l_start_dt := record.start_dt;
    l_end_dt   := record.end_dt;

    MERGE INTO line_item tgt
    USING (
      WITH l_stg AS (
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
        FROM line_item_stg s
        WHERE s.o_orderdate >= :l_start_dt
          AND s.o_orderdate < :l_end_dt
      ),
      l_deduped AS (
        SELECT *
        FROM l_stg
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_hash_diff ORDER BY last_modified_dt DESC, dw_file_row_no) = 1
      ),
      l_tgt AS (
        SELECT *
        FROM line_item
        WHERE o_orderdate >= :l_start_dt
          AND o_orderdate < :l_end_dt
      )
      SELECT
        current_timestamp() AS dw_version_ts,
        s.*
      FROM l_deduped s
      LEFT JOIN l_tgt t ON t.dw_line_item_shk = s.dw_line_item_shk
      WHERE t.dw_line_item_shk IS NULL
        OR (t.last_modified_dt < s.last_modified_dt AND t.dw_hash_diff != s.dw_hash_diff)
      ORDER BY s.o_orderdate
    ) src
    ON (
      tgt.dw_line_item_shk = src.dw_line_item_shk
      AND tgt.o_orderdate >= :l_start_dt
      AND tgt.o_orderdate < :l_end_dt
    )
    WHEN MATCHED THEN UPDATE SET
      tgt.dw_hash_diff      = src.dw_hash_diff,
      tgt.dw_version_ts     = src.dw_version_ts,
      tgt.l_orderkey        = src.l_orderkey,
      tgt.l_partkey         = src.l_partkey,
      tgt.l_suppkey         = src.l_suppkey,
      tgt.l_linenumber      = src.l_linenumber,
      tgt.l_quantity        = src.l_quantity,
      tgt.l_extendedprice   = src.l_extendedprice,
      tgt.l_discount        = src.l_discount,
      tgt.l_tax             = src.l_tax,
      tgt.l_returnflag      = src.l_returnflag,
      tgt.l_linestatus      = src.l_linestatus,
      tgt.l_shipdate        = src.l_shipdate,
      tgt.l_commitdate      = src.l_commitdate,
      tgt.l_receiptdate     = src.l_receiptdate,
      tgt.l_shipinstruct    = src.l_shipinstruct,
      tgt.l_shipmode        = src.l_shipmode,
      tgt.l_comment         = src.l_comment,
      tgt.last_modified_dt  = src.last_modified_dt,
      tgt.dw_file_name      = src.dw_file_name,
      tgt.dw_file_row_no    = src.dw_file_row_no,
      tgt.dw_update_ts      = src.dw_version_ts
    WHEN NOT MATCHED THEN INSERT (
      dw_line_item_shk,
      dw_hash_diff,
      dw_version_ts,
      l_orderkey,
      o_orderdate,
      l_partkey,
      l_suppkey,
      l_linenumber,
      l_quantity,
      l_extendedprice,
      l_discount,
      l_tax,
      l_returnflag,
      l_linestatus,
      l_shipdate,
      l_commitdate,
      l_receiptdate,
      l_shipinstruct,
      l_shipmode,
      l_comment,
      last_modified_dt,
      dw_file_name,
      dw_file_row_no,
      dw_load_ts,
      dw_update_ts
    ) VALUES (
      src.dw_line_item_shk,
      src.dw_hash_diff,
      src.dw_version_ts,
      src.l_orderkey,
      src.o_orderdate,
      src.l_partkey,
      src.l_suppkey,
      src.l_linenumber,
      src.l_quantity,
      src.l_extendedprice,
      src.l_discount,
      src.l_tax,
      src.l_returnflag,
      src.l_linestatus,
      src.l_shipdate,
      src.l_commitdate,
      src.l_receiptdate,
      src.l_shipinstruct,
      src.l_shipmode,
      src.l_comment,
      src.last_modified_dt,
      src.dw_file_name,
      src.dw_file_row_no,
      src.dw_load_ts,
      src.dw_version_ts
    );
  END LOOP;
  RETURN ''SUCCESS'';
END;';

