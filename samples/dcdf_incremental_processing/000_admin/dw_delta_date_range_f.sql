--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ---------------------------------create or replace function if not-
--  dd/mm/yy
--------------------------------------------------------------------
use database DEV_WEBINAR_COMMON_DB;
use schema   util;

CREATE OR REPLACE FUNCTION dw_delta_date_range_f (
    p_period_type_cd VARCHAR
)
RETURNS TABLE (start_dt DATE, end_dt DATE)
LANGUAGE SQL
AS
$$
    SELECT
        CASE LOWER(p_period_type_cd)
            WHEN 'all'     THEN CURRENT_DATE()
            WHEN 'day'     THEN DATE_TRUNC('day', event_dt)::DATE
            WHEN 'week'    THEN DATE_TRUNC('week', event_dt)::DATE
            WHEN 'month'   THEN DATE_TRUNC('month', event_dt)::DATE
            WHEN 'quarter' THEN DATE_TRUNC('quarter', event_dt)::DATE
            WHEN 'year'    THEN DATE_TRUNC('year', event_dt)::DATE
            ELSE CURRENT_DATE()
        END AS start_dt,
        MAX(event_dt) + INTERVAL '1 DAY' AS end_dt
    FROM
        dw_delta_date
    GROUP BY
        1
    ORDER BY
        1
$$
