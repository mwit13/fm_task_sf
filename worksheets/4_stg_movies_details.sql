USE ROLE DEV;
CREATE TABLE IF NOT EXISTS taskdb.stg.raw_movies_details (
    stg_created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    source_name STRING(200) NOT NULL,
    source_modified_date DATETIME NOT NULL,
    response VARIANT NOT NULL
)
MAX_DATA_EXTENSION_TIME_IN_DAYS = 1;

USE ROLE DEV;
CREATE OR REPLACE FILE FORMAT taskdb.stg.movies_details
    TYPE = JSON
    COMPRESSION = NONE;

USE ROLE DEV;
CREATE STAGE IF NOT EXISTS taskdb.stg.movies_details
    FILE_FORMAT = taskdb.stg.movies_details;

USE ROLE DEV;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.stg.movies_details
    TARGET_LAG = DOWNSTREAM
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = LOAD_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
    AS
        WITH ranked_rows AS (
            SELECT
                s.stg_created_date,
                s.source_name,
                s.source_modified_date,
                j.value:APICallTitle::string as title,
                j.value as response,
                ROW_NUMBER() OVER (PARTITION BY title ORDER BY source_modified_date DESC) AS rn
            FROM 
                taskdb.stg.raw_movies_details s,
                LATERAL FLATTEN(input => s.response) j
        )
        SELECT
            stg_created_date,
            source_name,
            source_modified_date,
            title,
            response,
        FROM ranked_rows
        WHERE rn = 1;

USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.stg.stream_revenues_per_day4fetch_movie_details_script ON DYNAMIC TABLE TASKDB.STG.REVENUES_PER_DAY;

USE ROLE OPS;
CREATE TASK IF NOT EXISTS taskdb.stg.put_raw_movies_details
    WAREHOUSE = LOAD_WH
    AS
        COPY INTO taskdb.stg.raw_movies_details (
            source_name,
            source_modified_date,
            response
        ) FROM (
        SELECT 
            METADATA$FILENAME,
            METADATA$FILE_LAST_MODIFIED,
            $1
        FROM @taskdb.stg.movies_details)
    ON_ERROR = CONTINUE; 

-- Giving the service role proper priviliges to be able to load data into stage
USE ROLE DEV;
GRANT READ, WRITE ON STAGE taskdb.stg.movies_details TO ROLE SER;
GRANT OPERATE ON TABLE taskdb.stg.movies_details TO ROLE SER;
GRANT SELECT ON TABLE taskdb.stg.movies_details TO ROLE SER;
GRANT OPERATE ON TABLE taskdb.stg.revenues_per_day TO ROLE SER;
GRANT SELECT ON TABLE taskdb.stg.revenues_per_day TO ROLE SER;
GRANT SELECT ON STREAM taskdb.stg.stream_revenues_per_day4fetch_movie_details_script TO ROLE SER;
USE ROLE OPS;
GRANT OPERATE ON TASK taskdb.stg.put_raw_movies_details TO ROLE SER;
USE ROLE DEV;

-- Since there is limitation that external access integration is not possible
-- Nor outbound from streamlit 
-- I decided to go with sns message to other AWS account to run lambda 
USE ROLE OPS;
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS notify_sns_topic_movies_details
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:eu-central-1:{...}:movies_details'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::{...}:role/role_snowflake_sns_topic_movies_details';

-- DESC NOTIFICATION INTEGRATION notify_sns_topic_movies_details;
-- Get SF_AWS_EXTERNAL_ID and SF_AWS_IAM_USER_ARN and add it to trust policy

USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.stg.stream_raw_revenues_per_day4notify_sns ON TABLE TASKDB.STG.RAW_REVENUES_PER_DAY;

USE ROLE OPS;
CREATE TASK IF NOT EXISTS taskdb.stg.notify_sns_to_run_fetch_movies_details
    WAREHOUSE = LOAD_WH
    WHEN SYSTEM$STREAM_HAS_DATA('TASKDB.STG.STREAM_RAW_REVENUES_PER_DAY4NOTIFY_SNS')
    AS 
        CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
            SNOWFLAKE.NOTIFICATION.APPLICATION_JSON('
                {
                    "delta_load": true,
                    "drop_file_from_stage": false,
                    "limit": null,
                    "revenues_name": "taskdb.stg.revenues_per_day",
                    "movies_details_name": "taskdb.stg.movies_details",
                    "join_column_name": "title",
                    "task_name": "taskdb.stg.put_raw_movies_details",
                    "revenues_stream_name": "taskdb.stg.stream_revenues_per_day4fetch_movie_details_script",
                    "movies_details_stage_name": "@taskdb.stg.movies_details",
                    "filename_prefix": "movies_details",
                    "faults_allowed": 5
                }
            '),
            SNOWFLAKE.NOTIFICATION.INTEGRATION('notify_sns_topic_movies_details')
        );

USE ROLE OPS;    
CREATE OR REPLACE TASK taskdb.stg.exhaust_stream_raw_revenues_per_day4notify_sns
    WAREHOUSE = LOAD_WH
    AFTER TASKDB.STG.NOTIFY_SNS_TO_RUN_FETCH_MOVIES_DETAILS
AS
    BEGIN
        CREATE OR REPLACE TEMPORARY TABLE taskdb.stg.stream_raw_revenues_per_day4notify_sns_temp AS 
        SELECT * FROM TASKDB.STG.STREAM_RAW_REVENUES_PER_DAY4NOTIFY_SNS LIMIT 1;
        DROP TABLE IF EXISTS taskdb.stg.stream_raw_revenues_per_day4notify_sns_temp;
    END;


USE ROLE OPS;
ALTER TASK TASKDB.STG.EXHAUST_STREAM_RAW_REVENUES_PER_DAY4NOTIFY_SNS RESUME;
ALTER TASK TASKDB.STG.NOTIFY_SNS_TO_RUN_FETCH_MOVIES_DETAILS RESUME;
USE ROLE DEV;
DESC NOTIFICATION INTEGRATION notify_sns_topic_movies_details;
-- Get SF_AWS_EXTERNAL_ID and SF_AWS_IAM_USER_ARN and add it to trust policy


-- DELETION SECTION
/* 
USE ROLE OPS;
ALTER TASK TASKDB.STG.NOTIFY_SNS_TO_RUN_FETCH_MOVIES_DETAILS SUSPEND;
ALTER TASK TASKDB.STG.EXHAUST_STREAM_RAW_REVENUES_PER_DAY4NOTIFY_SNS SUSPEND;
DROP TASK IF EXISTS TASKDB.STG.PUT_RAW_MOVIES_DETAILS;
DROP TASK IF EXISTS TASKDB.STG.NOTIFY_SNS_TO_RUN_FETCH_MOVIES_DETAILS;
DROP TASK IF EXISTS TASKDB.STG.EXHAUST_STREAM_RAW_REVENUES_PER_DAY4NOTIFY_SNS;
DROP STREAM IF EXISTS TASKDB.STG.STREAM_REVENUES_PER_DAY4FETCH_MOVIE_DETAILS_SCRIPT;
DROP STREAM IF EXISTS TASKDB.STG.STREAM_RAW_REVENUES_PER_DAY4NOTIFY_SNS;
DROP DYNAMIC TABLE IF EXISTS TASKDB.STG.MOVIES_DETAILS;
DROP TABLE IF EXISTS TASKDB.STG.RAW_MOVIES_DETAILS;
DROP STAGE IF EXISTS TASKDB.STG.MOVIES_DETAILS;
--
DROP FILE FORMAT IF EXISTS TASKDB.STG.MOVIES_DETAILS;
DROP NOTIFICATION INTEGRATION IF EXISTS NOTIFY_SNS_TOPIC_MOVIES_DETAILS;
*/ 
SELECT * FROM TASKDB.STG.MOVIES_DETAILS;
ALTER DYNAMIC TABLE TASKDB.STG.MOVIES_DETAILS REFRESH;