from celery import Celery
import json
import logging
import os

import gensim
from neo4j import GraphDatabase
from neo4j.exceptions import CypherError
import nltk
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import RegexpTokenizer
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from datasource import Ororo, IMDB, IBMWatson, RottenTomatoes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
log = logging.getLogger(__name__)

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379"),)
CELERY_RESULT_BACKEND = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

NUM_TOPICS = 500
TEXT_KEYS = ["plot", "description", "synopsis", "consensus"]


def cypher_escape(s):
    try:
        return s.replace('"', "'")
    except AttributeError:
        return s


@celery.task(name="tasks.find_similarities")
def find_similarities():
    log.info("Finding similarities...")

    with open("config.json") as f:
        config = json.load(f)

    neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

    # Clean up old similarities
    with neo.session() as session:
        q = "MATCH (:Movie)-[r:SIMILAR]-(:Movie) DETACH DELETE r"
        session.run(q)

    with neo.session() as session:
        q = "MATCH (m:Movie) RETURN m as movie"
        movies = session.run(q).data()

    tokenizer = RegexpTokenizer(r'\w+')
    stemmer = SnowballStemmer("english")
    stop_list = stopwords.words('english')

    dictionary = gensim.corpora.Dictionary()

    for movie in movies:
        text_list = []

        for key in TEXT_KEYS:
            if movie["movie"][key] and type(movie["movie"][key]) == str:
                text_list.append(movie["movie"][key].lower())

        text = ". ".join(text_list)
        text_tokenized = tokenizer.tokenize(text)
        text_tokenized_stemmed = [stemmer.stem(word) for word in text_tokenized]
        dictionary.add_documents([text_tokenized_stemmed])

    stop_ids = [dictionary.token2id[stopword] for stopword in stop_list if stopword in dictionary.token2id]
    once_ids = [tokenid for tokenid, docfreq in dictionary.dfs.items() if docfreq == 1]
    dictionary.filter_tokens(stop_ids + once_ids)
    dictionary.compactify()

    corpus = []
    gensim_id = 0
    for movie in movies:
        text_list = []
        for key in TEXT_KEYS:
            if movie["movie"][key] and type(movie["movie"][key]) == str:
                text_list.append(movie["movie"][key].lower())

        text = ". ".join(text_list)
        text_tokenized = tokenizer.tokenize(text)
        text_tokenized_stemmed = [stemmer.stem(word) for word in text_tokenized]
        corpus.append(dictionary.doc2bow(text_tokenized_stemmed))
        movie.update({'gensim_id': gensim_id})
        gensim_id += 1

    tfidf = gensim.models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]

    lsi = gensim.models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=NUM_TOPICS)

    index = gensim.similarities.MatrixSimilarity(lsi[corpus_tfidf])

    corr = calculate_correlations()
    corr_scaled = MinMaxScaler(feature_range=(-1,1)).fit_transform(corr)

    log.info("Updating neo4j with similarities")

    for i, similarities in enumerate(index):
        log.debug(f"Updating neo4j with similarities for {movies[i]['movie']['slug']}")
        assert i == movies[i]['gensim_id']
        correlations = corr_scaled[i]
        sim_corr = 0.75*similarities + 0.25*correlations
        neighbours = sorted(enumerate(sim_corr), key=lambda item: -item[1])[1:11]
        for j, similarity in neighbours:
            if similarity > 0.5:
                with neo.session() as session:
                    q = """MATCH (m:Movie {slug: "%s"})
                    MATCH (sm: Movie {slug: "%s"}) 
                    MERGE (m)-[r:SIMILAR]-(sm)
                    SET r.similarity = %f
                    """ % (movies[i]["movie"]["slug"], movies[j]["movie"]["slug"], similarity)
                    session.run(q)


def calculate_correlations():
    log.info("Calculating correlations...")

    with open("config.json") as f:
        config = json.load(f)

    # Neo4j
    neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

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

    genres = set([g for g in df['genres'].values for g in g])
    categories = set([g for g in df['categories'].values for g in g])

    for genre in genres:
        df[genre] = df.apply(lambda x: 1 if genre in x['genres'] else 0, axis=1)
    for category in categories:
        df[category] = df.apply(lambda x: 1 if category in x['categories'] else 0, axis=1)

    df.drop('genres', axis=1, inplace=True)
    df.drop('categories', axis=1, inplace=True)

    corr = df.select_dtypes(['number']).T.corr('spearman')  # 'kendall'

    log.info("Correlations calculated")

    return corr


@celery.task(name="tasks.update_database")
def update_database():
    log.info("Updating movie database...")

    with open("config.json") as f:
        config = json.load(f)

    # Neo4j
    neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

    index = [
        "CREATE INDEX movie_slug FOR (m:Movie) ON (m.slug, m.name)",
        "CREATE INDEX genre_name FOR (g:Genre) ON (g.name)",
        "CREATE INDEX category_name FOR (c:Category) ON (c.name)"
        "CREATE INDEX person_name FOR (p:Person) ON (p.name)",
    ]
    for i in index:
        try:
            with neo.session() as session:
                session.run(i)
        except CypherError:
            pass

    # Data sources
    ororo = Ororo(
        url=config["ororo"]["url"],
        username=config["ororo"]["username"],
        password=config["ororo"]["password"],
    )

    # Get movies and series from Ororo
    movies = ororo.get(path="movies")["movies"]
    shows = ororo.get(path="shows")["shows"]
    log.info(f"Found {len(movies)} movies and {len(shows)} shows in Ororo.")

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
                RETURN m.name, m.imdb_data, m.rotten_tomatoes_data, m.ibm_data, m.plot;
                """
                    % imdb_id
                )
                media = session.run(q).data()
                if media:
                    if not media[0]["m.imdb_data"]:
                        add_imdb_data(config["imdb"], imdb_id, neo)
                    if not media[0]["m.rotten_tomatoes_data"]:
                        add_rotten_tomatoes_data(config["rotten_tomatoes"], imdb_id, neo)
                    if not media[0]["m.ibm_data"]:
                        add_ibm_data(config["ibm"], imdb_id, neo)
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
            poster: "%s",
            imdb_data: false,
            ibm_data: false,
            rotten_tomatoes_data: false});
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
                item_clean["poster_thumb"]
            )
            queries.append(m)

            for genre in item_clean["array_genres"]:
                g = """MATCH (m: Movie {imdb_id: "%s"})
                MERGE (g: Genre {name: "%s"})
                WITH g, m
                MERGE (g)-[:HAS_MOVIE]->(m);
                """ % (
                    f'tt{item_clean["imdb_id"]}',
                    genre.lower().strip(),
                )
                queries.append(g)

            for country in item_clean["array_countries"]:
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
            add_imdb_data(config["imdb"], imdb_id, neo)
            add_rotten_tomatoes_data(config["rotten_tomatoes"], imdb_id, neo)
            add_ibm_data(config["ibm"], imdb_id, neo)


def add_ibm_data(config, imdb_id, neo4jclient):
    with neo4jclient.session() as session:
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
        ibm_client = IBMWatson(url=config["url"], api_key=config["apikey"])
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
                            q = 'MERGE (c:Category {name: "%s"})' % cypher_escape(
                                category.lower().strip()
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
    except Exception:
        log.warning(f"Cannot get IBM info for {imdb_id}", exc_info=True)


def add_rotten_tomatoes_data(config, imdb_id, neo4jclient):
    with neo4jclient.session() as session:
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
        rotten_tomatoes_client = RottenTomatoes(url=config["url"])
        log.info(f"Getting Rotten Tomatoes data for {imdb_id}")
        rt_data = rotten_tomatoes_client.get(path=slug, params={"title": title})

        if rt_data:
            q = (
            """MATCH (m: Movie {imdb_id: "%s"}) 
            SET m.rotten_tomatoes_data = true;
            """  % imdb_id
            )
            queries.append(q)

            if "ratingSummary" in rt_data.keys():
                q = """MATCH (m: Movie {imdb_id: "%s"})
                    SET m.consensus = "%s"
                    """ % (
                    imdb_id,
                    cypher_escape(rt_data["ratingSummary"]["consensus"])
                )
                queries.append(q)

                try:
                    if rt_data["ratingSummary"]["topCritics"]["averageRating"] != -1:
                        q = """MATCH (m: Movie {imdb_id: "%s"})
                        SET m.critics_rating = %f
                        """ % (
                            imdb_id,
                            float(rt_data["ratingSummary"]["topCritics"]["averageRating"])
                        )
                        queries.append(q)
                except Exception:
                    pass

            if "ratings" in rt_data.keys():
                q = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.critics_score = %f
                SET m.audience_score = %f;
                """ % (
                    imdb_id,
                    float(rt_data["ratings"]["critics_score"]),
                    float(rt_data["ratings"]["audience_score"])
                )
                queries.append(q)

            if "synopsis" in rt_data.keys():
                q = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.synopsis = "%s";
                """ % (
                    imdb_id,
                    cypher_escape(rt_data["synopsis"])
                )
                queries.append(q)

            if "reviews" in rt_data.keys():
                reviews_list = []
                for review in rt_data["reviews"]["reviews"]:
                    try:
                        reviews_list.append(review["quote"])
                    except KeyError:
                        pass
                reviews = '. '.join(reviews_list)
                q = """MATCH (m: Movie {imdb_id: "%s"})
                    SET m.reviews = "%s";
                    """ % (
                    imdb_id,
                    cypher_escape(reviews)
                )
                queries.append(q)

            with neo4jclient.session() as session:
                for q in queries:
                    session.run(q)
    except Exception:
        log.warning(f"Cannot get Rotten Tomatoes info for {imdb_id}", exc_info=True)


def add_imdb_data(config, imdb_id, neo4jclient):
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
        imdb_client = IMDB(url=config["url"], api_key=config["apikey"])
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
                if actor != 'n/a':
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
                if director != 'n/a':
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
    except Exception:
        log.warning(f"Cannot get IMDB info for {imdb_id}", exc_info=True)
