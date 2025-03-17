USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.actors_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    WITH actors_array AS (
        SELECT
            m.actors,
            r.revenue
        FROM TASKDB.DWH.DIM_MOVIES m
        JOIN TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
        ), 
    actor_data AS (
        SELECT
            a.value::STRING AS actor,
            actors_array.revenue
        FROM actors_array,
        LATERAL FLATTEN(INPUT => PARSE_JSON(actors_array.actors)) a
        )
    SELECT
        actor,
        SUM(revenue) AS total_revenue
    FROM actor_data
    GROUP BY actor;

CREATE OR REPLACE SECURE VIEW taskdb.public.actors_revenues AS SELECT * FROM TASKDB.OUT.ACTORS_REVENUES;

USE ROLE ANA;

CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.country_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    WITH countries_array AS (
        SELECT
            m.countries,
            r.revenue
        FROM TASKDB.DWH.DIM_MOVIES m
        JOIN TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
        ), 
    country_data AS (
        SELECT
            c.value::STRING AS country,
            countries_array.revenue
        FROM countries_array,
        LATERAL FLATTEN(INPUT => PARSE_JSON(countries_array.countries)) c
        )
    SELECT
        country,
        SUM(revenue) AS total_revenue
    FROM country_data
    GROUP BY country;

CREATE OR REPLACE SECURE VIEW taskdb.public.country_revenues AS SELECT * FROM TASKDB.OUT.COUNTRY_REVENUES;

USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.director_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    WITH directors_array AS (
        SELECT
            m.directors,
            r.revenue
        FROM TASKDB.DWH.DIM_MOVIES m
        JOIN TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
        ), 
    director_data AS (
        SELECT
            d.value::STRING AS director,
            directors_array.revenue
        FROM directors_array,
        LATERAL FLATTEN(INPUT => PARSE_JSON(directors_array.directors)) d
        )
    SELECT
        director,
        SUM(revenue) AS total_revenue
    FROM director_data
    GROUP BY director;

CREATE OR REPLACE SECURE VIEW taskdb.public.director_revenues AS SELECT * FROM TASKDB.OUT.DIRECTOR_REVENUES;

USE ROLE ANA;

CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.genre_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    WITH genres_array AS (
        SELECT
            m.genre,
            r.revenue
        FROM TASKDB.DWH.DIM_MOVIES m
        JOIN TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
        ), 
    genre_data AS (
        SELECT
            g.value::STRING AS genre,
            genres_array.revenue
        FROM genres_array,
        LATERAL FLATTEN(INPUT => PARSE_JSON(genres_array.genre)) g
        )
    SELECT
        genre,
        SUM(revenue) AS total_revenue
    FROM genre_data
    GROUP BY genre;

CREATE OR REPLACE SECURE VIEW taskdb.public.genre_revenues AS SELECT * FROM TASKDB.OUT.GENRE_REVENUES;

USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.writer_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    WITH writers_array AS (
        SELECT
            m.writers,
            r.revenue
        FROM TASKDB.DWH.DIM_MOVIES m
        JOIN TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
        ), 
    writer_data AS (
        SELECT
            w.value::STRING AS writer,
            writers_array.revenue
        FROM writers_array,
        LATERAL FLATTEN(INPUT => PARSE_JSON(writers_array.writers)) w
        )
    SELECT
        writer,
        SUM(revenue) AS total_revenue
    FROM writer_data
    GROUP BY writer;

CREATE OR REPLACE SECURE VIEW taskdb.public.writer_revenues AS SELECT * FROM TASKDB.OUT.WRITER_REVENUES;

USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.movies_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    SELECT
        m.title,
        SUM(r.revenue) AS total_revenue
    FROM
        TASKDB.DWH.DIM_MOVIES m
    JOIN
        TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
    GROUP BY
        m.title;
CREATE OR REPLACE SECURE VIEW taskdb.public.movies_revenues AS SELECT * FROM TASKDB.OUT.MOVIES_REVENUES;

USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.per_month_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    SELECT
        MONTHNAME(d.date_value) AS release_month,
        AVG(r.revenue)::integer AS total_revenue
    FROM
        TASKDB.DWH.DIM_MOVIES m
    JOIN
        TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
    JOIN 
        TASKDB.DWH.DIM_DATES d ON m.release_date_id = d.date_value
    GROUP BY
        release_month;
CREATE OR REPLACE SECURE VIEW taskdb.public.per_month_revenues AS SELECT * FROM TASKDB.OUT.PER_MONTH_REVENUES;

USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.per_year_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    SELECT
        YEAR(d.date_value) AS release_year,
        AVG(r.revenue)::integer AS total_revenue
    FROM
        TASKDB.DWH.DIM_MOVIES m
    JOIN
        TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
    JOIN 
        TASKDB.DWH.DIM_DATES d ON m.release_date_id = d.date_value
    GROUP BY
        release_year;
CREATE OR REPLACE SECURE VIEW taskdb.public.per_year_revenues AS SELECT * FROM TASKDB.OUT.PER_YEAR_REVENUES;

USE ROLE ANA;
CREATE DYNAMIC TABLE IF NOT EXISTS taskdb.out.per_rated_revenues
    TARGET_LAG = '1 HOUR'
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    WAREHOUSE = OUT_WH
    DATA_RETENTION_TIME_IN_DAYS = 1
AS
    SELECT
        m.rated,
        SUM(r.revenue) AS total_revenue
    FROM
        TASKDB.DWH.DIM_MOVIES m
    JOIN
        TASKDB.DWH.FACT_REVENUES r ON m.id = r.movie_id
    GROUP BY
        m.rated;
CREATE OR REPLACE SECURE VIEW taskdb.public.per_rated_revenues AS SELECT * FROM TASKDB.OUT.PER_RATED_REVENUES;

-- DELETION SECTION
/*
DROP SECURE VIEW TASKDB.PUBLIC.PER_RATED_REVENUES;
DROP TABLE TASKDB.OUT.PER_RATED_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.PER_YEAR_REVENUES;
DROP TABLE TASKDB.OUT.PER_YEAR_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.PER_MONTH_REVENUES;
DROP TABLE TASKDB.OUT.PER_MONTH_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.MOVIES_REVENUES;
DROP TABLE TASKDB.OUT.MOVIES_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.WRITER_REVENUES;
DROP TABLE TASKDB.OUT.WRITER_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.GENRE_REVENUES;
DROP TABLE TASKDB.OUT.GENRE_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.DIRECTOR_REVENUES;
DROP TABLE TASKDB.OUT.DIRECTOR_REVENUES;
DROP VIEW TASKDB.PUBLIC.COUNTRY_REVENUES;
DROP TABLE TASKDB.OUT.COUNTRY_REVENUES;
DROP SECURE VIEW TASKDB.PUBLIC.ACTORS_REVENUES;
DROP TABLE TASKDB.OUT.ACTORS_REVENUES;
*/