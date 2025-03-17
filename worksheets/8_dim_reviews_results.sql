USE ROLE DEV;
CREATE TABLE IF NOT EXISTS taskdb.dwh.dim_reviews_results (
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL,
    reviewer_id INTEGER NOT NULL,
    score_percent INTEGER NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES TASKDB.DWH.DIM_MOVIES(id),
    FOREIGN KEY (reviewer_id) REFERENCES TASKDB.DWH.DIM_MOVIES_REVIEWERS(id)
)
MAX_DATA_EXTENSION_TIME_IN_DAYS = 1;    

USE ROLE DEV;
CREATE FUNCTION IF NOT EXISTS taskdb.dwh.score_to_percent(raw_score STRING)
RETURNS int
LANGUAGE python
RUNTIME_VERSION = '3.12'
HANDLER = 'score_to_percent'
AS
$$
def score_to_percent(raw_score: str) -> int:
    try:
        if '%' in raw_score:
            return int(raw_score.replace('%', ''))
        elif '/' in raw_score:
            return int(eval(raw_score)*100)
        else:
            return int(raw_score)
    except (ValueError, ZeroDivisionError):
        return -1
$$;

USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.dwh.stream_movies_details4dim_reviews_results ON DYNAMIC TABLE TASKDB.STG.MOVIES_DETAILS;

USE ROLE OPS;
CREATE OR REPLACE TASK taskdb.dwh.populate_dim_reviews_results
    WAREHOUSE = TRANSFORM_WH
    AFTER TASKDB.DWH.POPULATE_DIM_MOVIES, TASKDB.DWH.POPULATE_DIM_MOVIES_REVIEWERS
    AS 
    MERGE INTO taskdb.dwh.dim_reviews_results drr USING (
    WITH source AS (
    SELECT 
        title,
        value:Source::string AS reviewer,
        taskdb.dwh.score_to_percent(value:Value::string) AS score_percent,
        METADATA$ISUPDATE
    FROM TASKDB.DWH.STREAM_MOVIES_DETAILS4DIM_REVIEWS_RESULTS,
    LATERAL FLATTEN(input => response:Ratings)
    WHERE METADATA$ACTION <> 'DELETE'
    UNION ALL
    SELECT
        title,
        'IMDb' AS reviewer,
        (response:imdbRating::float * 10)::int  AS score_percent,
        METADATA$ISUPDATE
    FROM TASKDB.DWH.STREAM_MOVIES_DETAILS4DIM_REVIEWS_RESULTS WHERE METADATA$ACTION <> 'DELETE')
        SELECT 
            dm.id AS movie_id,
            dr.id AS reviewer_id,
            source.score_percent,
            source.METADATA$ISUPDATE
        FROM source
        LEFT JOIN TASKDB.DWH.DIM_MOVIES dm ON source.title = dm.title
        LEFT JOIN TASKDB.DWH.DIM_MOVIES_REVIEWERS dr ON source.reviewer = dr.reviewer ) s
    ON drr.movie_id = s.movie_id AND drr.reviewer_id = s.reviewer_id
    WHEN MATCHED AND s.METADATA$ISUPDATE = 'TRUE' THEN UPDATE
        SET drr.updated_date = current_timestamp(),
            drr.score_percent = s.score_percent
    WHEN NOT MATCHED THEN INSERT (movie_id, reviewer_id, score_percent)
        VALUES (s.movie_id, s.reviewer_id, s.score_percent);



-- DELETION SECTION
/* 
USE ROLE DEV;
ALTER TASK TASKDB.DWH.POPULATE_DIM_REVIEWS_RESULTS SUSPEND;
DROP TASK IF EXISTS TASKDB.DWH.POPULATE_DIM_REVIEWS_RESULTS;
DROP STREAM IF EXISTS TASKDB.DWH.STREAM_MOVIES_DETAILS4DIM_REVIEWS_RESULTS;
DROP FUNCTION IF EXISTS TASKDB.DWH.SCORE_TO_PERCENT;
DROP TABLE IF EXISTS TASKDB.DWH.DIM_REVIEWS_RESULTS;
*/ 