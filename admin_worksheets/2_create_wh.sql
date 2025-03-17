USE ROLE accountadmin;
CREATE WAREHOUSE IF NOT EXISTS accountadmin_wh
     COMMENT = 'this is accountadmin warehosue '
     WAREHOUSE_SIZE = 'X-SMALL'
     AUTO_RESUME = TRUE
     AUTO_SUSPEND = 60
     ENABLE_QUERY_ACCELERATION = FALSE
     WAREHOUSE_TYPE = 'STANDARD'
     MIN_CLUSTER_COUNT = 1
     MAX_CLUSTER_COUNT = 1
     SCALING_POLICY = 'STANDARD'
     INITIALLY_SUSPENDED = TRUE;