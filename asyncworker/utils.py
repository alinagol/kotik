import logging

import pandas as pd
import requests.exceptions
from params import ibm_client, imdb_client, neo, rotten_tomatoes_client

log = logging.getLogger(__name__)


def cypher_escape(string: str) -> str:
    try:
        return string.replace('"', "'")
    except AttributeError:
        return string


def calculate_correlations() -> pd.DataFrame:
    log.info("Calculating correlations...")

    with neo.session() as session:
        q = """MATCH (m:Movie)
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
               categories

        """
        movies = session.run(q).data()

    df = pd.DataFrame(movies)

    genres = set([g for g in df["genres"].values for g in g])
    categories = set([g for g in df["categories"].values for g in g])

    for genre in genres:
        df[genre] = df.apply(
            lambda x: 1 if genre in x["genres"] else 0, axis=1
        )
    for category in categories:
        df[category] = df.apply(
            lambda x: 1 if category in x["categories"] else 0, axis=1
        )

    df.drop("genres", axis=1, inplace=True)
    df.drop("categories", axis=1, inplace=True)

    corr = df.select_dtypes(["number"]).T.corr("spearman")  # 'kendall'

    log.info("Correlations calculated.")

    return corr


def add_ibm_data(imdb_id: str) -> None:
    with neo.session() as session:
        q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
            RETURN m.ibm_data, m.plot, m.description, m.synopsis, m.reviews, m.consensus;
            """
            % imdb_id
        )
        data_flag = session.run(q).data()

        if data_flag[0]["m.ibm_data"]:
            return
        else:
            text_list = []
            for item in data_flag[0]:
                if data_flag[0][item] and type(data_flag[0][item]) == str:
                    text_list.append(data_flag[0][item])
            text = ". ".join(text_list)
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
            if t["score"] > 0.75:
                categories = t["label"].split("/")
                categories = [c.lower().strip() for c in categories if c]
                if categories:
                    for i, category in enumerate(categories[1:]):
                        if i == 0:
                            q = (
                                'MERGE (c:Category {name: "%s"})'
                                % cypher_escape(category.lower().strip())
                            )
                        else:
                            q = """MATCH (n:Category {name: "%s"})
                            MERGE (c:Category {name: "%s"})
                            WITH c, n
                            MERGE (n)-[:HAS_SUBCATEGORY]->(c)
                            """ % (
                                cypher_escape(categories[i - 1]),
                                cypher_escape(category),
                            )
                        queries.append(q)
                    for category in categories[1:]:
                        q = """
                        MATCH (m:Movie {imdb_id: "%s"})
                        SET m.ibm_data = true
                        MERGE (c:Category {name: "%s"})
                        WITH m, c
                        MERGE (c)-[r:HAS_MOVIE]->(m)
                        SET r.score = %f;
                        """ % (
                            imdb_id,
                            cypher_escape(category),
                            t["score"],
                        )
                        queries.append(q)
        for emotion, score in ibm_data["emotion"]["document"][
            "emotion"
        ].items():
            q = """MATCH (m: Movie {imdb_id: "%s"})
            SET m.ibm_data = true
            SET m.%s = %f
            """ % (
                imdb_id,
                emotion.lower().strip(),
                score,
            )
            queries.append(q)

        with neo.session() as session:
            for q in queries:
                session.run(q)
    except (requests.exceptions.RequestException, KeyError) as err:
        log.warning("Cannot get IBM info for %s: %s.", imdb_id, repr(err))


def add_rotten_tomatoes_data(imdb_id: str) -> None:
    with neo.session() as session:
        q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
        RETURN m.rotten_tomatoes_data, m.slug, m.name;
        """
            % imdb_id
        )
        data_flag = session.run(q).data()
        if data_flag[0]["m.rotten_tomatoes_data"]:
            return
        slug = data_flag[0]["m.slug"].replace("-", "_").replace("the_", "")
        title = data_flag[0]["m.name"]
    queries = []
    try:
        log.info(f"Getting Rotten Tomatoes data for {imdb_id}")
        rt_data = rotten_tomatoes_client.get(
            path=slug, params={"title": title}
        )

        if rt_data:
            q = (
                """MATCH (m: Movie {imdb_id: "%s"}) 
            SET m.rotten_tomatoes_data = true;
            """
                % imdb_id
            )
            queries.append(q)

            if "ratingSummary" in rt_data.keys():
                q = """MATCH (m: Movie {imdb_id: "%s"})
                    SET m.consensus = "%s"
                    """ % (
                    imdb_id,
                    cypher_escape(rt_data["ratingSummary"]["consensus"]),
                )
                queries.append(q)

                try:
                    if (
                        rt_data["ratingSummary"]["topCritics"]["averageRating"]
                        != -1
                    ):
                        q = """MATCH (m: Movie {imdb_id: "%s"})
                        SET m.critics_rating = %f
                        """ % (
                            imdb_id,
                            float(
                                rt_data["ratingSummary"]["topCritics"][
                                    "averageRating"
                                ]
                            ),
                        )
                        queries.append(q)
                except KeyError:
                    pass

            if "ratings" in rt_data.keys():
                q = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.critics_score = %f
                SET m.audience_score = %f;
                """ % (
                    imdb_id,
                    float(rt_data["ratings"]["critics_score"]),
                    float(rt_data["ratings"]["audience_score"]),
                )
                queries.append(q)

            if "synopsis" in rt_data.keys():
                q = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.synopsis = "%s";
                """ % (
                    imdb_id,
                    cypher_escape(rt_data["synopsis"]),
                )
                queries.append(q)

            if "reviews" in rt_data.keys():
                reviews_list = []
                for review in rt_data["reviews"]["reviews"]:
                    try:
                        reviews_list.append(review["quote"])
                    except KeyError:
                        pass
                reviews = ". ".join(reviews_list)
                q = """MATCH (m: Movie {imdb_id: "%s"})
                    SET m.reviews = "%s";
                    """ % (
                    imdb_id,
                    cypher_escape(reviews),
                )
                queries.append(q)

            with neo.session() as session:
                for q in queries:
                    session.run(q)
    except (requests.exceptions.RequestException, IndexError) as err:
        log.warning(
            "Cannot get Rotten Tomatoes info for %s: %s.", imdb_id, repr(err)
        )


def add_imdb_data(imdb_id: str) -> None:
    with neo.session() as session:
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
                if actor != "n/a":
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
                if director != "n/a":
                    g = """MATCH (m: Movie {imdb_id: "%s"})
                    MERGE (p: Person {name: "%s"})
                    WITH p, m
                    MERGE (p)-[:DIRECTED]->(m);
                    """ % (
                        imdb_id,
                        director.lower().strip(),
                    )
                    queries.append(g)
        with neo.session() as session:
            for q in queries:
                session.run(q)
    except requests.exceptions.RequestException as err:
        log.warning("Cannot get IMDB info for %s: %s.", imdb_id, repr(err))
