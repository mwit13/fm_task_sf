USE ROLE OPS;
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_revenues
      TYPE = EXTERNAL_STAGE
      STORAGE_PROVIDER = S3
      ENABLED = TRUE 
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{...}:role/SnowflakeMovieRevenuesReader'
      STORAGE_ALLOWED_LOCATIONS = ('s3://my-movies-storage-663/revenues/')
      COMMENT = 'DESC INTEGRATION S3_REVENUES; Get STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID and use it in AWS role.' ;

-- SHOW INTEGRATIONS;
-- DESC INTEGRATION s3_revenues;
-- Get STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID and use it in AWS role.

USE ROLE DEV;
CREATE OR REPLACE FILE FORMAT taskdb.stg.s3_revenues
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    DATE_FORMAT = 'YYYY-MM-DD'
    BINARY_FORMAT = UTF8
    NULL_IF = ('NULL', 'null', '')
    TRIM_SPACE = FALSE
    EMPTY_FIELD_AS_NULL = TRUE;

USE ROLE DEV;
CREATE STAGE IF NOT EXISTS taskdb.stg.s3_revenues
    STORAGE_INTEGRATION = S3_REVENUES
    URL = 's3://my-movies-storage-663/revenues/'
    FILE_FORMAT = TASKDB.STG.S3_REVENUES;

USE ROLE DEV;
CREATE TABLE IF NOT EXISTS taskdb.stg.raw_revenues_per_day (
    stg_created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    source_name STRING(200) NOT NULL,
    source_modified_date DATETIME NOT NULL,
    id STRING(36) NOT NULL,
    date DATE NOT NULL,
    title STRING(200) NOT NULL,
    revenue INTEGER NOT NULL,
    theaters INTEGER NOT NULL,
    distributor STRING(100) NOT NULL,
    PRIMARY KEY (date, title),
    CONSTRAINT uix_id_date UNIQUE (id, date)
)
MAX_DATA_EXTENSION_TIME_IN_DAYS = 1;

USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.stg.stream_raw_revenues_per_day4errors_raw_revenues_per_day ON TABLE TASKDB.STG.RAW_REVENUES_PER_DAY;

USE ROLE DEV;
CREATE TRANSIENT TABLE IF NOT EXISTS taskdb.stg.errors_raw_revenues_per_day (
    stg_created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    source_name STRING(200) NOT NULL,
    row_number INTEGER NOT NULL,
    column_name STRING NOT NULL,
    error STRING NOT NULL,
    rejected_record STRING NOT NULL
);

USE ROLE DEV;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.stg.revenues_per_day
    TARGET_LAG = DOWNSTREAM
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = LOAD_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    WITH ranked_rows AS (
        SELECT
            stg_created_date,
            source_name,
            source_modified_date,
            id,
            date,
            title,
            revenue,
            theaters,
            distributor,
            ROW_NUMBER() OVER (PARTITION BY id, date ORDER BY source_modified_date DESC) AS rn
        FROM TASKDB.STG.RAW_REVENUES_PER_DAY
    )
    SELECT
        stg_created_date,
        source_name,
        source_modified_date,
        id,
        date,
        title,
        revenue,
        theaters,
        distributor
    FROM ranked_rows
    WHERE rn = 1;

-- Only role which have rights to integration can create pipe, otherwise there will be silent error Insufficient privileges to operate on integration
USE ROLE OPS;
CREATE PIPE IF NOT EXISTS taskdb.stg.raw_revenues_per_day
    AUTO_INGEST = TRUE
    COMMENT = 'DESC PIPE TASKDB.STG.REVENUES_PER_DAY; Get notification_channel and add it to AWS S3 notification channel (SQS)'
    AS
        COPY INTO TASKDB.STG.RAW_REVENUES_PER_DAY (
            source_name,
            source_modified_date,
            id,
            date,
            title,
            revenue,
            theaters,
            distributor
        ) FROM (
        SELECT 
            METADATA$FILENAME,
            METADATA$FILE_LAST_MODIFIED,
            s.$1,
            s.$2, 
            s.$3, 
            s.$4,
            s.$5, 
            s.$6 
        FROM @TASKDB.STG.S3_REVENUES s)
        ON_ERROR = CONTINUE
        TRUNCATECOLUMNS = FALSE; 

USE ROLE OPS;
CREATE TASK IF NOT EXISTS taskdb.stg.populate_errors_raw_revenues_per_day
    WAREHOUSE = LOAD_WH
    WHEN SYSTEM$STREAM_HAS_DATA('TASKDB.STG.STREAM_RAW_REVENUES_PER_DAY4ERRORS_RAW_REVENUES_PER_DAY')
    AS 
INSERT INTO TASKDB.STG.ERRORS_RAW_REVENUES_PER_DAY (
    source_name,
    row_number,
    column_name,
    error,
    rejected_record
) SELECT 
    file as source_name,
    row_number,
    column_name,
    error,
    rejected_record
FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'TASKDB.STG.RAW_REVENUES_PER_DAY',
    START_TIME => DATEADD(MINUTE,-15,CURRENT_TIMESTAMP())));

USE ROLE OPS;    
CREATE OR REPLACE TASK taskdb.stg.exhaust_stream_raw_revenues_per_day4errors_raw_revenues_per_day
    WAREHOUSE = LOAD_WH
    AFTER TASKDB.STG.POPULATE_ERRORS_RAW_REVENUES_PER_DAY
AS
    BEGIN
        CREATE OR REPLACE TEMPORARY TABLE taskdb.stg.stream_raw_revenues_per_day4errors_raw_revenues_per_day_temp AS 
        SELECT * FROM TASKDB.STG.STREAM_RAW_REVENUES_PER_DAY4ERRORS_RAW_REVENUES_PER_DAY LIMIT 1;
        DROP TABLE IF EXISTS taskdb.stg.stream_raw_revenues_per_day4errors_raw_revenues_per_day_temp;
    END;


USE ROLE OPS;
ALTER TASK TASKDB.STG.EXHAUST_STREAM_RAW_REVENUES_PER_DAY4ERRORS_RAW_REVENUES_PER_DAY RESUME;
ALTER TASK TASKDB.STG.POPULATE_ERRORS_RAW_REVENUES_PER_DAY RESUME;
USE ROLE DEV;
DESC INTEGRATION s3_revenues;
--Get STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID and use it in AWS role.




-- DELETION SECTION
/* 
USE ROLE OPS;
ALTER TASK TASKDB.STG.POPULATE_ERRORS_RAW_REVENUES_PER_DAY SUSPEND;
ALTER TASK TASKDB.STG.EXHAUST_STREAM_RAW_REVENUES_PER_DAY4ERRORS_RAW_REVENUES_PER_DAY SUSPEND;
DROP TASK IF EXISTS TASKDB.STG.POPULATE_ERRORS_RAW_REVENUES_PER_DAY;
DROP TASK IF EXISTS TASKDB.STG.EXHAUST_STREAM_RAW_REVENUES_PER_DAY4ERRORS_RAW_REVENUES_PER_DAY;
DROP STREAM IF EXISTS TASKDB.STG.STREAM_RAW_REVENUES_PER_DAY4ERRORS_RAW_REVENUES_PER_DAY;
DROP PIPE IF EXISTS TASKDB.STG.RAW_REVENUES_PER_DAY;
DROP STAGE IF EXISTS TASKDB.STG.S3_REVENUES;
DROP TABLE IF EXISTS TASKDB.STG.ERRORS_RAW_REVENUES_PER_DAY;
DROP TABLE IF EXISTS TASKDB.STG.REVENUES_PER_DAY;
DROP TABLE IF EXISTS TASKDB.STG.RAW_REVENUES_PER_DAY;
--
DROP FILE FORMAT TASKDB.STG.S3_REVENUES;
DROP INTEGRATION S3_REVENUES;
*/

