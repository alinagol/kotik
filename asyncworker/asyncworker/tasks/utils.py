import logging
import time

import neo4j.exceptions
import pandas as pd
from neo4j import GraphDatabase

log = logging.getLogger(__name__)


NUM_TOPICS = 500
TEXT_KEYS = ["plot", "description", "synopsis", "consensus"]


def run_query(query: str, session: neo4j.Session) -> neo4j.Result:
    retries = 0
    while retries <= 3:
        try:
            return session.run(query)
        except neo4j.exceptions.TransientError:
            wait = retries * 5
            time.sleep(wait)
            retries += 1
    raise RuntimeError


def cypher_escape(string: str) -> str:
    try:
        return string.replace('"', "'")
    except AttributeError:
        return string


def calculate_correlations(neo4j_client: GraphDatabase) -> pd.DataFrame:
    log.info("Calculating correlations...")

    with neo4j_client.session() as session:
        query = """MATCH (m:Movie)
        OPTIONAL MATCH (g:Genre)-[:HAS_MOVIE]->(m)
        OPTIONAL MATCH (c:Category)-[:HAS_MOVIE]->(m)
        WITH m, collect(distinct g.name) as genres, collect(distinct c.name) as categories
        RETURN m.slug as slug,
               m.imdb_id as id,
               m.sadness as sadness,
               m.anger as anger,
               m.joy as joy,
               m.fear as fear, 
               m.disgust as disgust, 
               m.imdb_rating as rating, 
               m.critics_score as critics_score,
               m.audience_score as audience_score,
               m.critics_rating as critics_rating,
               genres,
               categories;
        """
        movies = run_query(query, session).data()

    dataframe = pd.DataFrame(movies)

    genres = set(gen for gen in dataframe["genres"].values for gen in gen)
    categories = set(
        cat for cat in dataframe["categories"].values for cat in cat
    )

    genre = None
    for genre in genres:
        dataframe[genre] = dataframe.apply(
            lambda x: 1 if genre in x["genres"] else 0, axis=1
        )
    category = None
    for category in categories:
        dataframe[category] = dataframe.apply(
            lambda x: 1 if category in x["categories"] else 0, axis=1
        )

    dataframe.drop("genres", axis=1, inplace=True)
    dataframe.drop("categories", axis=1, inplace=True)

    corr = dataframe.select_dtypes(["number"]).T.corr("spearman")  # 'kendall'

    log.info("Correlations calculated.")

    return corr
