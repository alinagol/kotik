from celery import Celery
import json
import logging
import os

from neo4j import GraphDatabase
from neo4j.exceptions import CypherError
import textrazor

from datasource import Ororo, IMDB, IBMWatson


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
log = logging.getLogger(__name__)

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379"),)
CELERY_RESULT_BACKEND = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)


def cypher_escape(s):
    try:
        return s.replace('"', "'")
    except AttributeError:
        return s


@celery.task(name="tasks.update_database")
def update_database():
    log.info("Updating movie database...")

    with open("config.json") as f:
        config = json.load(f)

    # Neo4j
    neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

    try:
        with neo.session() as session:
            session.run("CREATE INDEX movie_slug FOR (m:Movie) ON (m.slug)")
    except CypherError:
        pass

    # Data sources
    ororo = Ororo(
        url=config["ororo"]["url"],
        username=config["ororo"]["username"],
        password=config["ororo"]["password"],
    )
    imdb = IMDB(url=config["imdb"]["url"], api_key=config["imdb"]["apikey"])
    ibm = IBMWatson(url=config["ibm"]["url"], api_key=config["ibm"]["apikey"])
    textrazor.api_key = config["textrazor"]["apikey"]
    textrazorclient = textrazor.TextRazor(extractors=["topics"], do_encryption=False)

    # Get movies and series from Ororo
    movies = ororo.get(path="movies")["movies"]
    shows = ororo.get(path="shows")["shows"]
    log.info(f"Found {len(movies)} movies and {len(shows)} in Ororo.")

    library = {"movies": movies, "shows": shows}

    for media_type, items in library.items():
        for item in items:

            item_clean = {key: cypher_escape(value) for key, value in item.items()}
            imdb_id = f'tt{item_clean["imdb_id"]}'

            log.info(f'Processing {item["name"]} {imdb_id}...')

            # Find whether the movie is already in Neo4j
            with neo.session() as session:
                q = (
                    """MATCH (m: Movie {imdb_id: "%s"}) 
                RETURN m.name, m.imdb_data, m.textrazor_data, m.ibm_data, m.plot;
                """
                    % imdb_id
                )
                media = session.run(q).data()
                if media:
                    if not media[0]["m.imdb_data"]:
                        add_imdb_data(imdb, imdb_id, neo)
                    if not media[0]["m.textrazor_data"]:
                        add_textrazor_data(textrazorclient, imdb_id, neo)
                    if not media[0]["m.ibm_data"]:
                        add_ibm_data(ibm, imdb_id, neo)
                    log.info(f'Skipping {media[0]["m.name"]}, already in Neo4j')
                    continue

            # If movie not yet in Neo4j
            queries = []

            if not item_clean["imdb_rating"]:
                item_clean["imdb_rating"] = 0
            if not item_clean["year"]:
                item_clean["year"] = 0
            if not item_clean["length"]:
                item_clean["length"] = 0

            m = """MERGE (m: Movie {name: "%s", 
            slug: "%s",
            type: "%s",
            year: %i,
            imdb_rating: %f,
            imdb_id: "%s",
            description: "%s",
            length: %i,
            ororo_link: "%s",
            imdb_data: false,
            ibm_data: false,
            textrazor_data: false});
            """ % (
                item_clean["name"],
                item_clean["slug"],
                media_type,
                int(item_clean["year"]),
                float(item_clean["imdb_rating"]),
                f'tt{item_clean["imdb_id"]}',
                item_clean["desc"],
                int(item_clean["length"]),
                str(f'https://ororo.tv/en/{media_type}/{item_clean["slug"]}'),
            )
            queries.append(m)

            if type(item_clean["array_genres"]) != list:
                genres = [item_clean["array_genres"]]
            else:
                genres = item_clean["array_genres"]

            for genre in genres:
                g = """MATCH (m: Movie {imdb_id: "%s"})
                MERGE (g: Genre {name: "%s"})
                WITH g, m
                MERGE (g)-[:HAS_MOVIE]->(m);
                """ % (
                    f'tt{item_clean["imdb_id"]}',
                    genre.lower().strip(),
                )
                queries.append(g)

            for countries in item_clean["array_countries"]:
                if type(countries) != list:
                    countries = [countries]
                for country in countries:
                    c = """MATCH (m: Movie {imdb_id: "%s"})
                        MERGE (c: Country {name: "%s"})
                        WITH c, m
                        MERGE (c)-[:HAS_MOVIE]->(m);
                        """ % (
                        f'tt{item_clean["imdb_id"]}',
                        country.strip(),
                    )
                    queries.append(c)

            with neo.session() as session:
                log.info(f"Uploading {imdb_id} data to Neo4j")
                for q in queries:
                    session.run(q)

            # Extra information
            add_imdb_data(imdb, imdb_id, neo)
            add_ibm_data(ibm, imdb_id, neo)
            add_textrazor_data(textrazorclient, imdb_id, neo)


def add_textrazor_data(textrazor_client, imdb_id, neo4jclient):
    with neo4jclient.session() as session:
        q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
        RETURN m.textrazor_data, m.plot, m.description;
        """
            % imdb_id
        )
        data_flag = session.run(q).data()
        if data_flag[0]["m.textrazor_data"]:
            return
        else:
            try:
                text = data_flag[0]["m.plot"] + data_flag[0]["m.description"]
            except TypeError:
                text = data_flag[0]["m.description"]
    queries = []
    try:
        log.info(f"Getting Textrazor data for {imdb_id}")
        topics = textrazor_client.analyze(text).topics()
        for t in topics:
            if t.score > 0.5:
                q = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.textrazor_data = true
                MERGE (t: Topic {name: "%s"})
                WITH m, t
                MERGE (t)-[r:HAS_MOVIE]->(m)
                SET r.score = %f;
                """ % (
                    imdb_id,
                    t.label.lower().strip(),
                    t.score,
                )
                queries.append(q)
        with neo4jclient.session() as session:
            for q in queries:
                session.run(q)
    except textrazor.TextRazorAnalysisException:
        log.warning(f"Cannot get textrazor info for {imdb_id}")


def add_ibm_data(ibm_client, imdb_id, neo4jclient):
    with neo4jclient.session() as session:
        q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
        RETURN m.ibm_data, m.plot, m.description;
        """
            % imdb_id
        )
        data_flag = session.run(q).data()
        if data_flag[0]["m.ibm_data"]:
            return
        else:
            try:
                text = data_flag[0]["m.plot"] + data_flag[0]["m.description"]
            except TypeError:
                text = data_flag[0]["m.description"]
    queries = []
    try:
        log.info(f"Getting IBM data for {imdb_id}")
        ibm_data = ibm_client.get(
            path="/v1/analyze",
            params={
                "version": "2019-07-12",
                "features": "emotion,categories",
                "text": text,
            },
        )
        for t in ibm_data["categories"]:
            if t["score"] > 0.5:
                q = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.ibm_data = true
                MERGE (c: Category {name: "%s"})
                WITH m, c
                MERGE (c)-[r:HAS_MOVIE]->(m)
                SET r.score = %f;
                """ % (
                    imdb_id,
                    t["label"].lower().strip(),
                    t["score"],
                )
                queries.append(q)
        for emotion, score in ibm_data["emotion"]["document"]["emotion"].items():
            q = """MATCH (m: Movie {imdb_id: "%s"})
            SET m.ibm_data = true
            SET m.%s = %f
            """ % (
                imdb_id,
                emotion.lower().strip(),
                score,
            )
            queries.append(q)

        with neo4jclient.session() as session:
            for q in queries:
                session.run(q)
    except Exception as ex:
        log.warning(f"Cannot get IBM info for {imdb_id}: {ex}")


def add_imdb_data(imdb_client, imdb_id, neo4jclient):
    with neo4jclient.session() as session:
        q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
        RETURN m.imdb_data;
        """
            % imdb_id
        )
        data_flag = session.run(q).data()
        if data_flag[0]["m.imdb_data"]:
            return
    queries = []
    try:
        log.info(f"Getting IMDB data for {imdb_id}")
        imdb_data = imdb_client.get(params={"i": f"{imdb_id}"})
        q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
            SET m.imdb_data = true;
            """
            % imdb_id
        )
        queries.append(q)
        if "Plot" in imdb_data.keys():
            q = """MATCH (m: Movie {imdb_id: "%s"})
            SET m.plot = "%s";
            """ % (
                imdb_id,
                cypher_escape(imdb_data["Plot"]),
            )
            queries.append(q)
        if "Genre" in imdb_data.keys():
            for genre in imdb_data["Genre"].split(","):
                g = """MATCH (m: Movie {imdb_id: "%s"})
                MERGE (g: Genre {name: "%s"})
                WITH g, m
                MERGE (g)-[:HAS_MOVIE]->(m);
                """ % (
                    imdb_id,
                    genre.lower().strip(),
                )
                queries.append(g)
        if "Actors" in imdb_data.keys():
            for actor in imdb_data["Actors"].split(","):
                g = """MATCH (m: Movie {imdb_id: "%s"})
                MERGE (p: Person {name: "%s"})
                WITH p, m
                MERGE (p)-[:ACTED_IN]->(m);
                """ % (
                    imdb_id,
                    actor.lower().strip(),
                )
                queries.append(g)
        if "Director" in imdb_data.keys():
            for director in imdb_data["Director"].split(","):
                g = """MATCH (m: Movie {imdb_id: "%s"})
                MERGE (p: Person {name: "%s"})
                WITH p, m
                MERGE (p)-[:DIRECTED]->(m);
                """ % (
                    imdb_id,
                    director.lower().strip(),
                )
                queries.append(g)
        with neo4jclient.session() as session:
            for q in queries:
                session.run(q)
    except Exception as ex:
        log.warning(f"Cannot get IMDB info for {imdb_id}: {ex}")
