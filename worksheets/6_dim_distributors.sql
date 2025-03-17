USE ROLE DEV;
CREATE TABLE IF NOT EXISTS taskdb.dwh.dim_distributors
    DATA_RETENTION_TIME_IN_DAYS = 1
    (
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    distributor STRING(255) UNIQUE NOT NULL
    );
    
USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.dwh.stream_revenues_per_day4dim_distributors ON DYNAMIC TABLE TASKDB.STG.REVENUES_PER_DAY;

USE ROLE OPS;
CREATE OR REPLACE TASK taskdb.dwh.populate_dim_distributors
    WAREHOUSE = TRANSFORM_WH
    AFTER TASKDB.DWH.DWH_START
    AS 
MERGE INTO taskdb.dwh.dim_distributors dd USING (
    SELECT DISTINCT distributor, METADATA$ISUPDATE
    FROM TASKDB.DWH.STREAM_REVENUES_PER_DAY4DIM_DISTRIBUTORS
    WHERE distributor <> '-' AND METADATA$ACTION <> 'DELETE'
    ) s
ON dd.distributor=s.distributor
/* WHEN MATCHED AND s.METADATA$ISUPDATE = 'TRUE' THEN UPDATE
    SET dd.updated_date = current_timestamp() */
WHEN NOT MATCHED THEN INSERT (distributor)
    VALUES (s.distributor);

    
-- DELETION SECTION
/* 
USE ROLE DEV;
ALTER TASK TASKDB.DWH.POPULATE_DIM_DISTRIBUTORS SUSPEND;
DROP TASK IF EXISTS TASKDB.DWH.POPULATE_DIM_DISTRIBUTORS;
DROP STREAM IF EXISTS TASKDB.DWH.STREAM_REVENUES_PER_DAY4DIM_DISTRIBUTORS;
DROP TABLE IF EXISTS TASKDB.DWH.DIM_DISTRIBUTORS;
*/