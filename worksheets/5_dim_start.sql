USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.dwh.stream_revenues_per_day4dwh_start ON DYNAMIC TABLE TASKDB.STG.REVENUES_PER_DAY;
USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.dwh.stream_movies_details4dwh_start ON DYNAMIC TABLE TASKDB.STG.MOVIES_DETAILS;

USE ROLE OPS;
CREATE OR REPLACE TASK taskdb.dwh.dwh_start
    WAREHOUSE = TRANSFORM_WH
    WHEN SYSTEM$STREAM_HAS_DATA('TASKDB.DWH.STREAM_REVENUES_PER_DAY4DWH_START') AND SYSTEM$STREAM_HAS_DATA('TASKDB.DWH.STREAM_MOVIES_DETAILS4DWH_START')
    AS 
        BEGIN
            CREATE OR REPLACE TEMPORARY TABLE taskdb.dwh.start_temp AS 
                SELECT * FROM TASKDB.DWH.STREAM_REVENUES_PER_DAY4DWH_START LIMIT 1;
            CREATE OR REPLACE TEMPORARY TABLE taskdb.dwh.start_temp AS 
                SELECT * FROM TASKDB.DWH.STREAM_MOVIES_DETAILS4DWH_START LIMIT 1;
            DROP TABLE IF EXISTS taskdb.dwh.start_temp;
        END;

-- It should be WHEN ... OR ... BUT first run with only REVENUES_PER_DAY causes exhaustion of stream 4FACT_REVENUES
-- Applying AND causes that pipeline will be not triggered for updates of only revenues data without movies_details new data
-- Therefore there should be another pipeline for updates. Simplified - only with tasks for tables where revenues are source.
        
-- DELETION SECTION
/* 
USE ROLE OPS;  
ALTER TASK TASKDB.DWH.DWH_START SUSPEND;
DROP TASK TASKDB.DWH.DWH_START
DROP STREAM TASKDB.DWH.STREAM_REVENUES_PER_DAY4DWH_START;
DROP STREAM TASKDB.DWH.STREAM_MOVIES_DETAILS4DWH_START;
*/