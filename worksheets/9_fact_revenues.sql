USE ROLE DEV;
CREATE TABLE IF NOT EXISTS taskdb.dwh.fact_revenues (
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL,
    distributor_id INTEGER,
    date_id DATE NOT NULL,
    revenue INTEGER NOT NULL,
    theaters_number INTEGER NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES TASKDB.DWH.DIM_MOVIES(id),
    FOREIGN KEY (distributor_id) REFERENCES TASKDB.DWH.DIM_DISTRIBUTORS(id),
    FOREIGN KEY (date_id) REFERENCES TASKDB.DWH.DIM_DATES(date_value)
)
MAX_DATA_EXTENSION_TIME_IN_DAYS = 1;

USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.dwh.stream_revenues_per_day4fact_revenues ON DYNAMIC TABLE TASKDB.STG.REVENUES_PER_DAY;

USE ROLE OPS;
CREATE OR REPLACE TASK taskdb.dwh.populate_fact_revenues
    WAREHOUSE = TRANSFORM_WH
    AFTER TASKDB.DWH.POPULATE_DIM_REVIEWS_RESULTS, TASKDB.DWH.POPULATE_DIM_MOVIES, TASKDB.DWH.POPULATE_DIM_MOVIES_REVIEWERS, TASKDB.DWH.POPULATE_DIM_DATES, TASKDB.DWH.POPULATE_DIM_DISTRIBUTORS
    AS 
MERGE INTO taskdb.dwh.fact_revenues fr USING (
    SELECT 
        dm.id AS movie_id,
    	ddi.id AS distributor_id,
    	dda.date_value AS date_id,
    	r.revenue,
    	r.theaters AS theaters_number,
        r.METADATA$ISUPDATE
    FROM TASKDB.DWH.STREAM_REVENUES_PER_DAY4FACT_REVENUES r
    LEFT JOIN TASKDB.DWH.DIM_MOVIES dm ON dm.title = r.title
    LEFT JOIN TASKDB.DWH.DIM_DISTRIBUTORS ddi ON ddi.distributor = r.distributor
    LEFT JOIN TASKDB.DWH.DIM_DATES dda ON dda.date_value = TO_DATE(r.date)
    WHERE dm.id IS NOT NULL
    AND r.METADATA$ACTION <> 'DELETE') s
ON fr.movie_id=s.movie_id AND fr.date_id=s.date_id
WHEN MATCHED /* AND s.METADATA$ISUPDATE = 'TRUE' */ THEN UPDATE
    SET fr.updated_date = current_timestamp(),
        fr.movie_id = s.movie_id,
    	fr.distributor_id = s.distributor_id,
    	fr.date_id = s.date_id,
    	fr.revenue = s.revenue,
    	fr.theaters_number = s.theaters_number
WHEN NOT MATCHED THEN INSERT (movie_id, distributor_id, date_id, revenue, theaters_number)
    VALUES (s.movie_id, s.distributor_id, s.date_id, s.revenue, s.theaters_number);

-- DELETION SECTION
/* 
USE ROLE OPS;
ALTER TASK TASKDB.DWH.POPULATE_FACT_REVENUES SUSPEND;
DROP TASK IF EXISTS TASKDB.DWH.POPULATE_FACT_REVENUES;
DROP STREAM IF EXISTS TASKDB.DWH.STREAM_REVENUES_PER_DAY4FACT_REVENUES;
DROP TABLE IF EXISTS TASKDB.DWH.FACT_REVENUES;
*/     