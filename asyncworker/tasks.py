from celery import Celery
import json
import logging
import os

from neo4j import GraphDatabase
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

    config = json.load(open("config.json"))

    # Neo4j
    neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

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
    series = ororo.get(path="shows")["shows"]

    library = movies + series

    log.info(f"Found {len(library)} movies in Ororo.")

    for item in library:

        item_clean = {key: cypher_escape(value) for key, value in item.items()}
        text = item_clean["desc"]
        imdb_id = f'tt{item_clean["imdb_id"]}'

        log.info(f'Processing {item["name"]} {imdb_id}...')

        # Find whether the movie is already in Neo4j
        with neo.session() as session:
            q = (
                'MATCH (m: Movie {imdb_id: "%s"}) RETURN m.name, m.imdb_data, m.textrazor_data, m.ibm_data, m.plot'
                % imdb_id
            )
            media = session.run(q).data()

            if media:
                if media[0]["m.imdb_data"]:
                    text = text + media[0]["m.plot"]
                if (
                    media[0]["m.imdb_data"]
                    and media[0]["m.textrazor_data"]
                    and media[0]["m.ibm_data"]
                ):
                    log.info(f'Skipping {media[0]["m.name"]}, already in Neo4j')
                    continue

        # If movie not yet in Neo4j or some data is missing
        queries = []

        if not item_clean["imdb_rating"]:
            item_clean["imdb_rating"] = 0
        if not item_clean["year"]:
            item_clean["year"] = 0
        if not item_clean["length"]:
            item_clean["length"] = 0

        m = """
        MERGE (m: Movie {name: "%s", 
                         year: %i, 
                         imdb_rating: %f, 
                         imdb_id: "%s", 
                         description: "%s", 
                         length: %i});
        """ % (
            item_clean["name"],
            int(item_clean["year"]),
            float(item_clean["imdb_rating"]),
            f'tt{item_clean["imdb_id"]}',
            item_clean["desc"],
            int(item_clean["length"]),
        )
        queries.append(m)

        # If movie has extra information
        if media:
            data_flag = (
                'MATCH (m: Movie {imdb_id: "%s"}) \n' % f'tt{item_clean["imdb_id"]}'
            )
            if not media[0]["m.imdb_data"]:
                data_flag = data_flag + "SET m.imdb_data = false \n"

            if not media[0]["m.textrazor_data"]:
                data_flag = data_flag + "SET m.textrazor_data = false \n"

            if not media[0]["m.ibm_data"]:
                data_flag = data_flag + "SET m.ibm_data = false \n"
        else:
            data_flag = (
                """MATCH (m: Movie  {imdb_id: "%s"}) 
                   SET m.imdb_data = false, 
                    m.textrazor_data = false, 
                    m.ibm_data = false"""
                % f'tt{item_clean["imdb_id"]}'
            )
        queries.append(data_flag)

        if type(item_clean["array_genres"]) != list:
            genres = [item_clean["array_genres"]]
        else:
            genres = item_clean["array_genres"]

        for genre in genres:
            g = """
            MATCH (m: Movie {imdb_id: "%s"})
            MERGE (g: Genre {name: "%s"})
            WITH g, m
            MERGE (g)-[:HAS_MOVIE]->(m)
            """ % (
                f'tt{item_clean["imdb_id"]}',
                genre.lower().strip(),
            )
            queries.append(g)

        for countries in item_clean["array_countries"]:
            if type(countries) != list:
                countries = [countries]
            for country in countries:
                c = """
                MATCH (m: Movie {imdb_id: "%s"})
                MERGE (c: Country {name: "%s"})
                WITH c, m
                MERGE (c)-[:HAS_MOVIE]->(m)
                """ % (
                    f'tt{item_clean["imdb_id"]}',
                    country.strip(),
                )
                queries.append(c)

        with neo.session() as session:
            log.info(f"Uploading {imdb_id} data to Neo4j")
            for q in queries:
                session.run(q)
            q = ('MATCH (m: Movie {imdb_id: "%s"}) RETURN m.name, m.imdb_data, m.textrazor_data, m.ibm_data' % imdb_id)
            updated_media = session.run(q).data()[0]

        extra_data_queries = []

        if not updated_media["m.imdb_data"]:
            try:
                log.info(f'Gettin IMDB info for {item["name"]}...')
                imdb_data = imdb.get(params={"i": f"{imdb_id}"})
                q = """
                MATCH (m: Movie {imdb_id: "%s"})
                SET m.imdb_data = true
                """ % (
                    imdb_id
                )
                text = text + imdb_data["Plot"]
                extra_data_queries.append(q)

                if "Plot" in imdb_data.keys():
                    q = """
                    MATCH (m: Movie {imdb_id: "%s"})
                    SET m.plot = "%s"
                    """ % (
                        imdb_id,
                        cypher_escape(imdb_data["Plot"]),
                    )
                    extra_data_queries.append(q)

                if "Genre" in imdb_data.keys():
                    for genre in imdb_data["Genre"].split(","):
                        g = """
                        MATCH (m: Movie {imdb_id: "%s"})
                        MERGE (g: Genre {name: "%s"})
                        WITH g, m
                        MERGE (g)-[:HAS_MOVIE]->(m)
                        """ % (
                            imdb_id,
                            genre.lower().strip(),
                        )

                        extra_data_queries.append(g)
                log.info(f"Received IMDB data for {imdb_id}")
            except Exception as ex:
                log.warning(f"Cannot get IMDB info for {imdb_id}: {ex}")

        if not updated_media["m.ibm_data"]:
            try:
                log.info(f'Gettin IBM Watson info for {item["name"]}...')
                ibm_data = ibm.get(
                    path="/v1/analyze",
                    params={
                        "version": "2019-07-12",
                        "features": "emotion,categories",
                        "text": text,
                    },
                )
                for t in ibm_data["categories"]:
                    if t["score"] > 0.5:
                        q = """
                        MATCH (m: Movie {imdb_id: "%s"})
                        SET m.ibm_data = true
                        MERGE (c: Category {name: "%s"})
                        WITH m, c
                        MERGE (c)-[r:HAS_MOVIE]->(m)
                        SET r.score = %f
                        """ % (
                            imdb_id,
                            t["label"].lower().strip(),
                            t["score"],
                        )
                        extra_data_queries.append(q)

                for emotion, score in ibm_data["emotion"]["document"][
                    "emotion"
                ].items():
                    q = """
                    MATCH (m: Movie {imdb_id: "%s"})
                    SET m.ibm_data = true
                    MERGE (e: Emotion {name: "%s"})
                    WITH m, e
                    MERGE (e)-[r:HAS_MOVIE]->(m)
                    SET r.score = %f
                    """ % (
                        imdb_id,
                        emotion.lower().strip(),
                        score,
                    )
                    extra_data_queries.append(q)

                log.info(f"Received IBM data for {imdb_id}")
            except Exception as ex:
                log.warning(f"Cannot get IBM info for {imdb_id}: {ex}")

        if not updated_media["m.textrazor_data"]:
            try:
                log.info(f'Getting Textrazor info for {item["name"]}...')
                topics = textrazorclient.analyze(text).topics()
                for t in topics:
                    if t.score > 0.5:
                        q = """
                        MATCH (m: Movie {imdb_id: "%s"})
                        SET m.textrazor_data = true
                        MERGE (t: Topic {name: "%s"})
                        WITH m, t
                        MERGE (t)-[r:HAS_MOVIE]->(m)
                        SET r.score = %f
                        """ % (
                            imdb_id,
                            t.label.lower().strip(),
                            t.score,
                        )
                        extra_data_queries.append(q)
                log.info(f"Received textrazor data for {imdb_id}")
            except textrazor.TextRazorAnalysisException:
                log.warning(f"Cannot get textrazor info for {imdb_id}")

        with neo.session() as session:
            log.info(f"Uploading {imdb_id} data to Neo4j")
            for q in extra_data_queries:
                session.run(q)
