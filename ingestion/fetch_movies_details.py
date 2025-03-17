import json
import logging
import os
import time
from typing import Iterator
import requests
from requests import Response
from datetime import datetime

from furl import furl
from tqdm import tqdm
from snowflake.snowpark import Session, Table
from snowflake.snowpark.functions import col


CONNECTION_DETAILS: dict = dict(user='robot',
                                password=os.environ.get("ROBOT_PWD"),
                                account="wn65628.eu-central-1",
                                role='ser',
                                warehouse="LOAD_WH")
API_KEY = os.environ.get("OMDB_API_KEY")
OMDB_ADDRESS = 'http://www.omdbapi.com'

"""
...Parameters...
In the future needs to be moved to separate config 
with proper safety measures.
"""
DELTA_LOAD = True
DROP_FILE_FROM_STAGE = False
LIMIT: int | None = None

REVENUES_NAME = "taskdb.stg.revenues_per_day"
MOVIES_DETAILS_NAME = "taskdb.stg.movies_details"
JOIN_COLUMN_NAME = "title"
TASK_NAME = "taskdb.stg.put_raw_movies_details"
REVENUES_STREAM_NAME = "taskdb.stg.stream_revenues_per_day4fetch_movie_details_script"
MOVIES_DETAILS_STAGE_NAME = "@taskdb.stg.movies_details"

FILENAME_PREFIX = 'movies_details'
FAULTS_ALLOWED = 5
"""
...Parameters...
"""

# Module to omit the warning, SNOW-1641112
import warnings
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module='snowflake.connector'
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_movies_details")


class Title(str):
    pass


class Filename(str):
    pass


def _get_omdb_data(omdb_address: str, api_key: str, movie_title: Title) -> Response | None:
    url = furl(omdb_address).add(dict(apikey=api_key, t=movie_title)).url
    try:
        return requests.get(url)
    except Exception as e:
        logger.error(e)
        return None


def get_movie_titles(revenues_fqn: str,
                     movie_details_fqn: str,
                     column_name: str,
                     limit: int | None,
                     session: Session) -> Iterator[Title]:
    column_name: str = column_name.upper()
    revenues_fqn: str = revenues_fqn.upper()
    movie_details_fqn: str = movie_details_fqn.upper()

    logger.info(f"Getting titles from {revenues_fqn}")
    revenues_df: Table = session.table(revenues_fqn).select(col(column_name))

    logger.info(f"Getting titles from {movie_details_fqn}")
    movies_df: Table = session.table(movie_details_fqn).select(col(column_name))

    if not limit:
        result: Table = revenues_df.join(movies_df, revenues_df[column_name] == movies_df[column_name],
                                         "leftanti")
    else:
        result: Table = revenues_df.join(movies_df, revenues_df[column_name] == movies_df[column_name],
                                         "leftanti").limit(limit)
    result_i: Iterator[Title] = (row[column_name] for row in result.to_local_iterator(case_sensitive=False))
    return result_i


def _get_next_filename(prefix: str) -> Filename:
    date: str = datetime.now().strftime("%Y-%m-%d")
    count: int = 1

    while True:
        filename = f'{prefix}_{date}_{count}.json'
        if not os.path.exists(filename):
            return Filename(filename)
        count += 1


def get_save_movie_details(movie_titles: Iterator[Title],
                           prefix: str,
                           faults_allowed: int,
                           omdb_address: str,
                           api_key: str) -> Filename | None:
    filename: Filename = _get_next_filename(prefix)

    first: bool = True
    faults: int = 0
    logger.info(f"Start of API calls to '{omdb_address}'.")

    with open(filename, "w", encoding="utf-8") as f:
        f.write("[\n")
        for title in tqdm(movie_titles):
            if faults > faults_allowed:
                break
            details = get_movie_details(omdb_address, api_key, title)

            if details is not None:
                details["APICallTitle"] = title
                json_details = json.dumps(details, indent=4)
                if not first:
                    f.write(",\n")
                f.write(json_details)
                first = False
            else:
                faults += 1
        f.write("\n]")

    logger.info(f"End of API calls.")

    if first:
        os.remove(filename)
        logger.warning(f"File is not created.")
        return None

    logger.info(f"File created '{filename}'.")
    return filename


def get_movie_details(omdb_address, api_key, title) -> dict | None:
    try:
        omdb_response: Response | None = _get_omdb_data(omdb_address, api_key, title)
        if isinstance(omdb_response, Response):
            response_json = omdb_response.json()
            if not eval(response_json.get("Response", "None")):
                logger.warning(f"Faulty API response for {title}. {response_json.get("Error", "Error not known")}")
                return None
            return omdb_response.json()
        else:
            logger.warning(f"Faulty API response for {title}.")
            return None

    except Exception as e:
        logger.error(f"Error processing title {title}: {e}")
        return None


def is_file(filename: Filename | None) -> bool:
    if not filename:
        logger.info(f"No file generated. Therefore omitting upload and running the task.")
        return False
    return True


def upload_file(filename: Filename,
                stage_fqn: str,
                session: Session) -> None:

    stage_fqn = stage_fqn.upper()

    logger.info(f"Attempting to send '{filename}' to stage '{stage_fqn}'.")
    session.file.put(filename, stage_fqn, auto_compress=False, overwrite=True)
    logger.info(f"File '{filename}' sent to stage '{stage_fqn}'.")
    return None


def run_copy_task(task_fqn: str, session: Session) -> None:
    task_fqn = task_fqn.upper()
    logger.info(f"Attempting to run task '{task_fqn}'.")
    session.sql(f"EXECUTE TASK {task_fqn};").collect()
    logger.info(f"Task '{task_fqn}' executed.")


def exhaust_stream(revenues_stream_fqn, session: Session) -> None:
    temp_table_name = f"{revenues_stream_fqn}_temp"
    logger.info(f"Attempting to create temporary table '{temp_table_name}' to exhaust '{revenues_stream_fqn}' stream.")
    query = f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM {revenues_stream_fqn} LIMIT 1"
    session.sql(query).collect()
    logger.info(f"Created successfully. Now it will be dropped.")
    query = f"DROP TABLE IF EXISTS {temp_table_name}"
    session.sql(query).collect()


def delete_file_from_stage(filename: Filename, stage_fqn: str, session: Session) -> None:
    file_path = f"{stage_fqn.upper()}/{filename}"
    logger.info(f"Attempting to delete file from {file_path}.")
    session.sql(f"REMOVE {file_path};").collect()
    logger.info(f"File deleted.")


def refresh_dynamic_table(table_name: str, session: Session) -> None:
    table_name = table_name.upper()
    logger.info(f"Refreshing '{table_name};")
    session.sql(f"ALTER DYNAMIC TABLE {table_name} REFRESH;").collect()


def lambda_handler(event, context):
    session: Session = Session.builder.configs(CONNECTION_DETAILS).create()

    try:
        refresh_dynamic_table(table_name=event['revenues_name'], session=session)
        refresh_dynamic_table(table_name=event['movies_details_name'], session=session)

        if DELTA_LOAD:
            md: Iterator[Title] = get_movie_titles(revenues_fqn=event['revenues_stream_name'],
                                                   movie_details_fqn=event['movies_details_name'],
                                                   column_name=event['join_column_name'],
                                                   limit=event['limit'],
                                                   session=session)
        else:
            md: Iterator[Title] = get_movie_titles(revenues_fqn=event['revenues_name'],
                                                   movie_details_fqn=event['movies_details_name'],
                                                   column_name=event['join_column_name'],
                                                   limit=event['limit'],
                                                   session=session)

        proper_filename: Filename = get_save_movie_details(movie_titles=md,
                                                           prefix=event['filename_prefix'],
                                                           faults_allowed=event['faults_allowed'],
                                                           omdb_address=OMDB_ADDRESS,
                                                           api_key=API_KEY)
        is_f = is_file(filename=proper_filename)
        if is_f:
            upload_file(filename=proper_filename,
                        stage_fqn=event['movies_details_stage_name'],
                        session=session)

            run_copy_task(task_fqn=event['task_name'],
                          session=session)

        if DELTA_LOAD:
            exhaust_stream(revenues_stream_fqn=event['revenues_stream_name'], session=session)

        if event['drop_file_from_stage'] and proper_filename:
            delete_file_from_stage(filename=proper_filename, stage_fqn=event['movies_details_stage_name'], session=session)

        if is_f:
            time.sleep(10)
            refresh_dynamic_table(table_name=event['movies_details_name'], session=session)

        logger.info("Whole script finished successfully.")

    finally:
        session.close()


if __name__ == "__main__":
    context = None
    event = dict(
        delta_load=DELTA_LOAD,
        drop_file_from_stage=DROP_FILE_FROM_STAGE,
        limit=LIMIT,
        revenues_name=REVENUES_NAME,
        movies_details_name=MOVIES_DETAILS_NAME,
        join_column_name=JOIN_COLUMN_NAME,
        task_name=TASK_NAME,
        revenues_stream_name=REVENUES_STREAM_NAME,
        movies_details_stage_name=MOVIES_DETAILS_STAGE_NAME,
        filename_prefix=FILENAME_PREFIX,
        faults_allowed=FAULTS_ALLOWED
    )
    event = {
        "delta_load": True,
        "drop_file_from_stage": False,
        "limit": None,
        "revenues_name": "taskdb.stg.revenues_per_day",
        "movies_details_name": "taskdb.stg.movies_details",
        "join_column_name": "title",
        "task_name": "taskdb.stg.put_raw_movies_details",
        "revenues_stream_name": "taskdb.stg.stream_revenues_per_day4fetch_movie_details_script",
        "movies_details_stage_name": "@taskdb.stg.movies_details",
        "filename_prefix": "movies_details",
        "faults_allowed": 5
    }
    lambda_handler(event, context)
