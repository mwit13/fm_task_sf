USE ROLE DEV;
CREATE TABLE IF NOT EXISTS taskdb.dwh.dim_movies (
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title STRING(200) NOT NULL,
    full_title STRING(200) NOT NULL,
    start_year_date_id DATE,
    end_year_date_id DATE,
    rated STRING(10),
    release_date_id DATE,
    length_min INTEGER,
    genre VARIANT,
    directors VARIANT,
    writers VARIANT,
    actors VARIANT,
    plot STRING,
    languages VARIANT,
    countries VARIANT,
    awards STRING,
    poster_url STRING,
    imdb_id STRING(50),
    is_series BOOLEAN,
    dvd_release_date_id DATE,
    boxoffice INTEGER,
    production STRING(200),
    FOREIGN KEY (start_year_date_id) REFERENCES taskdb.dwh.dim_dates(date_value),
    FOREIGN KEY (end_year_date_id) REFERENCES taskdb.dwh.dim_dates(date_value),
    FOREIGN KEY (dvd_release_date_id) REFERENCES taskdb.dwh.dim_dates(date_value)
)
MAX_DATA_EXTENSION_TIME_IN_DAYS = 1;

USE ROLE DEV;
CREATE STREAM IF NOT EXISTS taskdb.dwh.stream_movies_details4dim_movies ON DYNAMIC TABLE TASKDB.STG.MOVIES_DETAILS;

USE ROLE OPS;
CREATE OR REPLACE TASK taskdb.dwh.populate_dim_movies
    WAREHOUSE = TRANSFORM_WH
    AFTER TASKDB.DWH.POPULATE_DIM_DATES
    AS 
MERGE INTO taskdb.dwh.dim_movies dm USING (
    WITH json_unpack AS (
        SELECT 
            title,
            response:Title::string AS full_title,
            response:Year::string AS year,
            response:Rated::string AS rated,
            response:Released::string release_date_raw, 
            response:Runtime::string AS length,
            response:Genre::string AS genre_raw,
            response:Director::string AS directors_raw,
            response:Writer::string AS writers_raw,
            response:Actors::string AS actors_raw,
            response:Plot::string AS plot,
            response:Language::string AS languages_raw,
            response:Country::string AS countries_raw,
            response:Awards::string AS awards,
            response:Poster::string AS poster,
            response:imdbVotes::string AS imdb_votes,
            response:imdbID::string AS imdb_id,
            response:Type::string AS type,
            response:DVD::string AS dvd_release_date_raw,        
            response:BoxOffice::string AS boxoffice_raw,
            response:Production::string AS production_raw,
            METADATA$ISUPDATE
        FROM TASKDB.DWH.STREAM_MOVIES_DETAILS4DIM_MOVIES WHERE METADATA$ACTION <> 'DELETE'
        ),
    transformation AS (
    SELECT
            title,
            full_title,
            TO_DATE(SPLIT_PART(year, '-', 1), 'YYYY') AS start_year_date,
            CASE WHEN CONTAINS(year, '-')
                THEN
                    TO_DATE(SPLIT_PART(year, '-', 2), 'YYYY')
                ELSE
                    NULL
                END AS end_year_date,
            rated,
            TO_DATE(release_date_raw, 'DD MON YYYY') AS release_date, 
            CASE WHEN length <> 'N/A'
                THEN 
                    REPLACE(length, ' min', '')::integer
                ELSE
                    NULL
                END AS length_min,
            CASE WHEN genre_raw <> 'N/A'
                THEN 
                    TO_VARIANT(SPLIT(genre_raw, ', ')) 
                ELSE
                    NULL
                END AS genre,
            CASE WHEN directors_raw <> 'N/A'
                THEN 
                    TO_VARIANT(SPLIT(directors_raw, ', ')) 
                ELSE
                    NULL
                END AS directors,
            CASE WHEN writers_raw <> 'N/A'
                THEN 
                    TO_VARIANT(SPLIT(writers_raw, ', ')) 
                ELSE
                    NULL
                END AS writers,
            CASE WHEN actors_raw <> 'N/A'
                THEN 
                    TO_VARIANT(SPLIT(actors_raw, ', ')) 
                ELSE
                    NULL
                END AS actors,
            plot,
            CASE WHEN languages_raw <> 'N/A'
                THEN 
                    TO_VARIANT(SPLIT(languages_raw, ', ')) 
                ELSE
                    NULL
                END AS languages,
            CASE WHEN countries_raw <> 'N/A'
                THEN 
                    TO_VARIANT(SPLIT(countries_raw, ', ')) 
                ELSE
                    NULL
                END AS countries,
            awards,
            poster AS poster_url,
            CASE WHEN type <> 'series'
                THEN             
                    REPLACE(imdb_votes, ',', '')::integer
                ELSE
                    NULL
                END AS imdb_votes_number,
            imdb_id,
            CASE WHEN type <> 'series'
                THEN 
                    FALSE 
                ELSE
                    TRUE
                END AS is_series,
            CASE WHEN dvd_release_date_raw <> 'N/A'
                THEN 
                    TO_DATE(dvd_release_date_raw, 'DD MON YYYY')   
                ELSE
                    NULL
                END AS dvd_release_date,
            CASE WHEN boxoffice_raw <> 'N/A'
                THEN 
                    REPLACE(REPLACE(boxoffice_raw, ',', ''), '$', '')::integer  
                ELSE
                    NULL
                END AS boxoffice,
            CASE WHEN production_raw <> 'N/A'
                THEN 
                    production_raw 
                ELSE
                    NULL
                END AS production,
            METADATA$ISUPDATE
    FROM 
    json_unpack)
    SELECT 
        t.title,
        t.full_title,
        dates3.date_value AS start_year_date_id,
        dates4.date_value AS end_year_date_id,
        t.rated,
        dates1.date_value AS release_date_id,
        t.length_min,
        t.genre,
        t.directors,
        t.writers,
        t.actors,
        t.plot,
        t.languages,
        t.countries,
        t.awards,
        t.poster_url,
        t.imdb_id,
        t.is_series,
        dates2.date_value AS dvd_release_date_id,
        t.boxoffice,
        t.production,
        t.METADATA$ISUPDATE
    FROM transformation t
    LEFT JOIN TASKDB.DWH.DIM_DATES dates1 ON t.release_date = dates1.date_value
    LEFT JOIN TASKDB.DWH.DIM_DATES dates2 ON t.dvd_release_date = dates2.date_value
    LEFT JOIN TASKDB.DWH.DIM_DATES dates3 ON t.start_year_date = dates3.date_value
    LEFT JOIN TASKDB.DWH.DIM_DATES dates4 ON t.end_year_date = dates4.date_value
    ) s
ON dm.title=s.title
WHEN MATCHED AND s.METADATA$ISUPDATE = 'TRUE' THEN UPDATE
    SET dm.updated_date = current_timestamp(),
        dm.full_title = s.full_title,
        dm.start_year_date_id = s.start_year_date_id,
        dm.end_year_date_id = s.end_year_date_id,
        dm.rated = s.rated,
        dm.release_date_id = s.release_date_id,
        dm.length_min = s.length_min,
        dm.genre = s.genre,
        dm.directors = s.directors,
        dm.writers = s.writers,
        dm.actors = s.actors,
        dm.plot = s.plot,
        dm.languages = s.languages,
        dm.countries = s.countries,
        dm.awards = s.countries,
        dm.poster_url = s.poster_url,
        dm.imdb_id = s.imdb_id,
        dm.is_series = s.is_series,
        dm.dvd_release_date_id = s.dvd_release_date_id,
        dm.boxoffice = s.boxoffice,
        dm.production = s.production
WHEN NOT MATCHED THEN INSERT (
    title,
    full_title,
    start_year_date_id,
    end_year_date_id,
    rated,
    release_date_id,
    length_min,
    genre,
    directors,
    writers,
    actors,
    plot,
    languages,
    countries,
    awards,
    poster_url,
    imdb_id,
    is_series,
    dvd_release_date_id,
    boxoffice,
    production
    )
VALUES (
    s.title,
    s.full_title,
    s.start_year_date_id,
    s.end_year_date_id,
    s.rated,
    s.release_date_id,
    s.length_min,
    s.genre,
    s.directors,
    s.writers,
    s.actors,
    s.plot,
    s.languages,
    s.countries,
    s.awards,
    s.poster_url,
    s.imdb_id,
    s.is_series,
    s.dvd_release_date_id,
    s.boxoffice,
    s.production
    );



-- DELETION SECTION
/* 
USE ROLE DEV;
ALTER TASK TASKDB.DWH.POPULATE_DIM_MOVIES SUSPEND;
DROP TASK IF EXISTS TASKDB.DWH.POPULATE_DIM_MOVIES;
DROP STREAM IF EXISTS TASKDB.DWH.STREAM_MOVIES_DETAILS4DIM_MOVIES;
DROP TABLE IF EXISTS TASKDB.DWH.DIM_MOVIES;
*/ 