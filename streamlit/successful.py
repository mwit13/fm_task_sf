import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from dataclasses import dataclass


@dataclass
class MostSuccessfulPlotDetails:
    name: str
    x: str
    y: str
    labels: dict
    title: str
    has_limit: bool


definitions = dict(
    per_month=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.PER_MONTH_REVENUES",
        x='RELEASE_MONTH',
        y='TOTAL_REVENUE',
        labels={'RELEASE_MONTH': 'Release Month', 'TOTAL_REVENUE': 'Average Revenue [$]'},
        title='Most fruitful months',
        has_limit=False
    ),
    genres=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.GENRE_REVENUES",
        x='GENRE',
        y='TOTAL_REVENUE',
        labels={'GENRE': 'Genre', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most fruitful genres',
        has_limit=True
    ),
    actors=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.ACTORS_REVENUES",
        x='ACTOR',
        y='TOTAL_REVENUE',
        labels={'ACTOR': 'Actor', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most successful actors',
        has_limit=True
    ),
    countries=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.COUNTRY_REVENUES",
        x='COUNTRY',
        y='TOTAL_REVENUE',
        labels={'COUNTRY': 'Country', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most successful countries',
        has_limit=True
    ),
    directors=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.DIRECTOR_REVENUES",
        x='DIRECTOR',
        y='TOTAL_REVENUE',
        labels={'DIRECTOR': 'Director', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most successful directors',
        has_limit=True
    ),
    movies=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.MOVIES_REVENUES",
        x='TITLE',
        y='TOTAL_REVENUE',
        labels={'TITLE': 'Movie title', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most fruitful movies',
        has_limit=True
    ),
    rating=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.PER_RATED_REVENUES",
        x='RATED',
        y='TOTAL_REVENUE',
        labels={'RATED': 'Age rating type', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most fruitful age ratings',
        has_limit=False
    ),
    per_year=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.PER_YEAR_REVENUES",
        x='RELEASE_YEAR',
        y='TOTAL_REVENUE',
        labels={'RELEASE_YEAR': 'Release year', 'TOTAL_REVENUE': 'Average Revenue [$]'},
        title='Most fruitful years',
        has_limit=False
    ),
    writers=MostSuccessfulPlotDetails(
        name="TASKDB.PUBLIC.WRITER_REVENUES",
        x='WRITER',
        y='TOTAL_REVENUE',
        labels={'WRITER': 'Writer', 'TOTAL_REVENUE': 'Total Revenue [$]'},
        title='Most successful writers',
        has_limit=True
    ),
)

dv_tv_map = dict((
    ('Month', 'per_month'),
    ('Genres', 'genres'),
    ('Actors', 'actors'),
    ('Countries', 'countries'),
    ('Directors', 'directors'),
    ('Titles', 'movies'),
    ('Age Rating', 'rating'),
    ('Year', 'per_year'),
    ('Writers', 'writers'),
))

st.title("Most successful factors :film_frames:")
st.write("Graphs depict total/average movies revenues for choosen factor :heavy_dollar_sign:.")

dv_choice = st.selectbox('Select factor', dv_tv_map.keys())
tv_choice = dv_tv_map[dv_choice]
d = definitions[tv_choice]

session = get_active_session()

df = (session.table(d.name).select(col(d.x), col(d.y))
      .order_by(d.y, ascending=False))
if d.has_limit:
    l = st.number_input("Entities on graph", min_value=2, max_value=50, value=25, step=1)
    df = df.limit(l)
df = df.to_pandas()

chart = alt.Chart(df).mark_bar().encode(
    x=alt.X(f"{d.x}:N", title=d.labels[d.x], sort="-y"),
    y=alt.Y(f"{d.y}:Q", title=d.labels[d.y]),
    tooltip=[alt.Tooltip(d.x, title=d.labels[d.x]),
             alt.Tooltip(d.y, title=d.labels[d.y], format=",.0f")]
).properties(
    width=800,
    height=500,
    title=d.title
)

st.altair_chart(chart, use_container_width=True)