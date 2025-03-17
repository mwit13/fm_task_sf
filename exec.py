import json
import os
import snowflake.connector as sf
from dataclasses import dataclass, asdict
from snowflake.connector.cursor import SnowflakeCursor, DictCursor


class SFQuery(str):
    pass


@dataclass
class SFQueryResult:
    q: SFQuery
    r: dict | tuple | None

    def to_json(self):
        return json.dumps(asdict(self))

    def __repr__(self):
        return str(self.to_json())


def get_queries(filename: str, directory: str = ".") -> list[SFQuery]:
    try:
        with (open(f'{directory}/{filename}', 'r') as file):
            sql_queries = file.read()
            sql_queries = sql_queries \
                .replace("\t", " ") \
                .replace("\n", " ") \
                .replace("    ", "") \
                .replace("  ", " ")
            sql_queries = sql_queries.split(";")
            sql_queries = sql_queries[:-1] if sql_queries[-1] == "" else sql_queries
            sql_queries = [SFQuery(q.lstrip() + ";") for q in sql_queries]  # not necessary to put ; in the end
    except FileNotFoundError:
        raise
    return sql_queries


def execute_file(cursor: SnowflakeCursor,
                 filename: str,
                 directory: str = ".",
                 close: bool = True) -> list[SFQueryResult]:
    queries = get_queries(filename, directory)
    output: list[SFQueryResult] = []
    try:
        for q in queries:
            cursor.execute(q)
            output.append(SFQueryResult(q=q, r=cursor.fetchone()))
    finally:
        if close:
            cursor.close()
    return output


# Module to omit the warning, SNOW-1641112
import warnings
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module='snowflake.connector'
)

if __name__ == "__main__":
    conn = sf.connect(
        user='mw',
        password=os.getenv('SNOWFLAKE_CONNECTIONS_MW_PASSWORD'),
        account='wn65628.eu-central-1',
        warehouse='COMPUTE_WH',
    )
    cursor = conn.cursor(cursor_class=DictCursor)

    print(
        execute_file(cursor=cursor, filename="esql.sql")
    )
    cursor.close()