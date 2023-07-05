CREATE OR REPLACE TASK acq_cust
WAREHOUSE = DEV_WEBINAR_WH
SCHEDULE = 'USING CRON 0 9-17 * * 1-5 UTC'
AS
CALL acq_customer_data();

CREATE TASK stg_cust
after acq_cust
AS
CALL stg_customer_data();

CREATE TASK process_cust
after stg_cust
AS
CALL process_customer_data();

CREATE TASK dim_cust
after process_cust
AS
CALL dim_customer_data();

CREATE TASK fact_cust
after process_cust
AS
CALL fact_order_data(); 