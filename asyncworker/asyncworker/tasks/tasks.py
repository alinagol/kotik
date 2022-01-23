import logging
from typing import Dict

import gensim
import requests.exceptions
from celery import Task, chain, group
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import RegexpTokenizer
from sklearn.preprocessing import MinMaxScaler

from asyncworker.celery import celery_app
from asyncworker.tasks import utils
from asyncworker.tasks.clients import (
    init_ibm_client,
    init_imdb_client,
    init_mubi_client,
    init_neo4j_client,
    init_ororo_client,
    init_rotten_tomatoes_client,
)

log = logging.getLogger(__name__)


# Task types
class TaskWithRetry(Task):  # pylint: disable=abstract-method
    retry_kwargs = {"max_retries": 3}
    autoretry_for = (
        requests.exceptions.ConnectionError,
        requests.exceptions.SSLError,
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
    )
    retry_backoff = True

    _neo4j_client = None
    _imdb_client = None
    _ibm_client = None
    _ororo_client = None
    _rotten_tomatoes_client = None
    _mubi_client = None

    @property
    def neo4j_client(self):
        if self._neo4j_client is None:
            self._neo4j_client = init_neo4j_client()
        return self._neo4j_client

    @property
    def imdb_client(self):
        if self._imdb_client is None:
            self._imdb_client = init_imdb_client()
        return self._imdb_client

    @property
    def mubi_client(self):
        if self._mubi_client is None:
            self._mubi_client = init_mubi_client()
        return self._mubi_client

    @property
    def ibm_client(self):
        if self._ibm_client is None:
            self._ibm_client = init_ibm_client()
        return self._ibm_client

    @property
    def ororo_client(self):
        if self._ororo_client is None:
            self._ororo_client = init_ororo_client()
        return self._ororo_client

    @property
    def rotten_tomatoes_client(self):
        if self._rotten_tomatoes_client is None:
            self._rotten_tomatoes_client = init_rotten_tomatoes_client()
        return self._rotten_tomatoes_client


@celery_app.task(name="tasks.find_similarities", base=TaskWithRetry)
def find_similarities() -> str:  # pylint: disable=too-many-locals
    log.info("Finding similarities...")

    tokenizer = RegexpTokenizer(r"\w+")
    stemmer = SnowballStemmer("english")
    stop_list = stopwords.words("english")

    dictionary = gensim.corpora.Dictionary()

    # Clean up old similarities
    with find_similarities.neo4j_client.session() as session:
        query = "MATCH (:Movie)-[r:SIMILAR]-(:Movie) DETACH DELETE r"
        utils.run_query(query, session)
        query = "MATCH (m:Movie) RETURN m as movie"
        movies = utils.run_query(query, session).data()

    for movie in movies:
        text_list = []

        for key in utils.TEXT_KEYS:
            if movie["movie"][key] and isinstance(movie["movie"][key], str):
                text_list.append(movie["movie"][key].lower())

        text = ". ".join(text_list)
        text_tokenized = tokenizer.tokenize(text)
        text_tokenized_stemmed = [
            stemmer.stem(word) for word in text_tokenized
        ]
        dictionary.add_documents([text_tokenized_stemmed])

    stop_ids = [
        dictionary.token2id[stopword]
        for stopword in stop_list
        if stopword in dictionary.token2id
    ]
    once_ids = [
        tokenid for tokenid, docfreq in dictionary.dfs.items() if docfreq == 1
    ]
    dictionary.filter_tokens(stop_ids + once_ids)
    dictionary.compactify()

    corpus = []
    gensim_id = 0
    for movie in movies:
        text_list = []
        for key in utils.TEXT_KEYS:
            if movie["movie"][key] and isinstance(movie["movie"][key], str):
                text_list.append(movie["movie"][key].lower())

        text = ". ".join(text_list)
        text_tokenized = tokenizer.tokenize(text)
        text_tokenized_stemmed = [
            stemmer.stem(word) for word in text_tokenized
        ]
        corpus.append(dictionary.doc2bow(text_tokenized_stemmed))
        movie.update({"gensim_id": gensim_id})
        gensim_id += 1

    tfidf = gensim.models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]

    lsi = gensim.models.LsiModel(
        corpus_tfidf, id2word=dictionary, num_topics=utils.NUM_TOPICS
    )

    index = gensim.similarities.MatrixSimilarity(lsi[corpus_tfidf])

    corr = utils.calculate_correlations(find_similarities.neo4j_client)
    corr_scaled = MinMaxScaler(feature_range=(-1, 1)).fit_transform(corr)

    log.info("Updating neo4j with similarities.")

    for i, similarities in enumerate(index):
        log.debug(
            "Updating neo4j with similarities for %s",
            {movies[i]["movie"]["slug"]},
        )
        assert i == movies[i]["gensim_id"]
        correlations = corr_scaled[i]
        sim_corr = 0.25 * similarities + 0.75 * correlations
        neighbours = sorted(enumerate(sim_corr), key=lambda item: -item[1])[
            1:11
        ]
        for j, similarity in neighbours:
            if similarity > 0.25:
                with find_similarities.neo4j_client.session() as session:
                    query = """MATCH (m:Movie {slug: "%s"})
                    MATCH (sm: Movie {slug: "%s"})
                    MERGE (m)-[r:SIMILAR]-(sm)
                    SET r.similarity = %f
                    """ % (
                        movies[i]["movie"]["slug"],
                        movies[j]["movie"]["slug"],
                        similarity,
                    )
                    utils.run_query(query, session)
    return "Similarities calculated."


@celery_app.task(name="tasks.add_media", base=TaskWithRetry)
def add_media(  # pylint: disable=too-many-branches, too-many-statements
    item: Dict[str, str], media_type: str, source: str
) -> str:
    item_clean = {
        key: utils.cypher_escape(value) for key, value in item.items()
    }

    with add_media.neo4j_client.session() as session:
        if source == "ororo":
            imdb_id = f'tt{item["imdb_id"]}'
            slug = item_clean["slug"]
        elif source == "mubi":
            try:
                imdb_id = add_media.imdb_client.get_id(
                    item_clean["title"], item_clean["year"]
                )
                slug = item_clean["canonical_url"].split("/")[-1]
            except KeyError:
                log.warning("Could not find imdb id for '%s'.", item["title"])
                return "No data added"
        if not imdb_id:
            return "No data added"

        if item_clean["year"]:
            try:
                year = int(item_clean["year"])
            except (ValueError, TypeError):
                try:
                    year = int(item_clean["year"].split("-")[0].strip())
                except ValueError:
                    year = -1
        else:
            year = -1

        log.debug("Processing %s...", imdb_id)

        # Find whether the movie is already in Neo4j
        query = """MATCH (m: Movie {imdb_id: "%s", slug: "%s"})
                    RETURN m.imdb_data, m.rotten_tomatoes_data, m.ibm_data;
                    """ % (
            imdb_id,
            slug,
        )
        media = utils.run_query(query, session).data()
        if media:
            if not media[0]["m.imdb_data"]:
                add_imdb_data.apply_async(kwargs={"imdb_id": imdb_id})
            # if not media[0]["m.rotten_tomatoes_data"]:
            #     add_rotten_tomatoes_data.apply_async(
            #         kwargs={"imdb_id": imdb_id}
            #     )
            # if not media[0]["m.ibm_data"]:
            #     add_ibm_data.apply_async(kwargs={"imdb_id": imdb_id})
            log.info("Skipping %s, already in Neo4j", imdb_id)
            return "Skipping"

        # Add the movie to Neo4j
        queries = []
        if source == "ororo":
            if not item_clean["imdb_rating"]:
                item_clean["imdb_rating"] = "-1"
            if not item_clean["length"]:
                item_clean["length"] = "-1"

            query = """MERGE (m: Movie {name: "%s",
                                   source: "ororo",
                                   slug: "%s",
                                   type: "%s",
                                   year: %i,
                                   imdb_rating: %f,
                                   imdb_id: "%s",
                                   description: "%s",
                                   length: %i,
                                   link: "%s",
                                   poster: "%s",
                                   imdb_data: false,
                                   rotten_tomatoes_data: false,
                                   ibm_data: false
                                   });
                                   """ % (
                item_clean["name"],
                slug,
                media_type,
                year,
                float(item_clean["imdb_rating"]),
                f'tt{item_clean["imdb_id"]}',
                item_clean["desc"],
                int(item_clean["length"]),
                str(f'https://ororo.tv/en/{media_type}/{item_clean["slug"]}'),
                item_clean["poster_thumb"],
            )
            queries.append(query)

            for genre in set(item_clean["array_genres"]):
                query = """MATCH (m: Movie {imdb_id: "%s"})
                                       MERGE (g: Genre {name: "%s"})
                                       WITH g, m
                                       MERGE (g)-[:HAS_MOVIE]->(m);
                                       """ % (
                    f'tt{item_clean["imdb_id"]}',
                    genre.lower().strip(),
                )
                queries.append(query)

            for country in set(item_clean["array_countries"]):
                query = """MATCH (m: Movie {imdb_id: "%s"})
                                               MERGE (c: Country {name: "%s"})
                                               WITH c, m
                                               MERGE (c)-[:HAS_MOVIE]->(m);
                                               """ % (
                    f'tt{item_clean["imdb_id"]}',
                    country.strip(),
                )
                queries.append(query)

        elif source == "mubi":
            query = """MERGE (m: Movie {
                imdb_id: "%s",
                name: "%s", 
                slug: "%s",
                year: %i, 
                source: "mubi", 
                mubi_popularity: "%s", 
                still_average_colour: "%s",
                link: "%s",
                poster: "%s",
                imdb_data: false,
                rotten_tomatoes_data: false,
                ibm_data: false
                })
            """ % (
                imdb_id,
                item_clean["title"],
                slug,
                year,
                int(item_clean["popularity"]),
                item_clean["still_average_colour"],
                item_clean["canonical_url"],
                item_clean["still_url"],
            )
            queries.append(query)
        log.debug("Uploading %s data to Neo4j.", imdb_id)
        for query in queries:
            utils.run_query(query, session)

    # Add extra information
    job = chain(
        add_imdb_data.si(imdb_id),
        # add_rotten_tomatoes_data.si(imdb_id),
        # add_ibm_data.si(imdb_id),
    )
    job.apply_async()

    return "Media added"


@celery_app.task(name="tasks.add_imdb_data", base=TaskWithRetry)
def add_imdb_data(imdb_id: str) -> str:
    with add_imdb_data.neo4j_client.session() as session:
        query = (
            'MATCH (m: Movie {imdb_id: "%s"}) RETURN m.imdb_data;' % imdb_id
        )
        data_flag = utils.run_query(query, session).data()
        if data_flag[0]["m.imdb_data"]:
            log.debug("IMDB data already present for %s.", imdb_id)
            return "No data added"

    queries = []
    try:
        log.debug("Getting IMDB data for %s.", imdb_id)
        imdb_data = next(add_imdb_data.imdb_client.get(params={"i": imdb_id}))
    except (requests.exceptions.RequestException, StopIteration) as err:
        log.warning("Cannot get IMDB info for %s: %s.", imdb_id, repr(err))
        raise

    query = (
        'MATCH (m: Movie {imdb_id: "%s"}) SET m.imdb_data = true;' % imdb_id
    )
    queries.append(query)
    if "Plot" in imdb_data.keys():
        query = 'MATCH (m: Movie {imdb_id: "%s"}) SET m.plot = "%s";' % (
            imdb_id,
            utils.cypher_escape(imdb_data["Plot"]),
        )
        queries.append(query)
    if "Genre" in imdb_data.keys():
        for genre in set(imdb_data["Genre"].split(",")):
            query = """MATCH (m: Movie {imdb_id: "%s"})
                MERGE (g: Genre {name: "%s"})
                WITH g, m
                MERGE (g)-[:HAS_MOVIE]->(m);
                """ % (
                imdb_id,
                genre.lower().strip(),
            )
            queries.append(query)
    if "Actors" in imdb_data.keys():
        for actor in set(imdb_data["Actors"].split(",")):
            if actor != "n/a":
                query = """MATCH (m: Movie {imdb_id: "%s"})
                    MERGE (p: Person {name: "%s"})
                    WITH p, m
                    MERGE (p)-[:ACTED_IN]->(m);
                    """ % (
                    imdb_id,
                    actor.lower().strip(),
                )
                queries.append(query)
    if "Director" in imdb_data.keys():
        for director in set(imdb_data["Director"].split(",")):
            if director != "n/a":
                query = """MATCH (m: Movie {imdb_id: "%s"})
                    MERGE (p: Person {name: "%s"})
                    WITH p, m
                    MERGE (p)-[:DIRECTED]->(m);
                    """ % (
                    imdb_id,
                    director.lower().strip(),
                )
                queries.append(query)

    with add_imdb_data.neo4j_client.session() as session:
        for query in queries:
            utils.run_query(query, session)

    return "Data added"


@celery_app.task(name="tasks.add_rotten_tomatoes_data", base=TaskWithRetry)
def add_rotten_tomatoes_data(imdb_id: str) -> str:
    with add_rotten_tomatoes_data.neo4j_client.session() as session:
        query = (
            """MATCH (m: Movie {imdb_id: "%s"})
        RETURN m.rotten_tomatoes_data, m.slug, m.name;
        """
            % imdb_id
        )
        data_flag = utils.run_query(query, session).data()
        if data_flag[0]["m.rotten_tomatoes_data"]:
            log.debug("Rotten Tomatoes data already present for %s.", imdb_id)
            return "No data added"
        slug = data_flag[0]["m.slug"].replace("-", "_").replace("the_", "")
        title = data_flag[0]["m.name"]

    queries = []
    try:
        log.info("Getting Rotten Tomatoes data for %s.", imdb_id)
        rt_data = next(
            add_rotten_tomatoes_data.rotten_tomatoes_client.get(
                path=slug, params={"title": title}
            )
        )
    except (requests.exceptions.RequestException, StopIteration) as err:
        log.warning(
            "Cannot get Rotten Tomatoes info for %s: %s.", imdb_id, repr(err)
        )
        raise

    query = (
        """MATCH (m: Movie {imdb_id: "%s"})
            SET m.rotten_tomatoes_data = true;
            """
        % imdb_id
    )
    queries.append(query)

    if "ratingSummary" in rt_data.keys():
        query = """MATCH (m: Movie {imdb_id: "%s"})
                    SET m.consensus = "%s"
                    """ % (
            imdb_id,
            utils.cypher_escape(rt_data["ratingSummary"]["consensus"]),
        )
        queries.append(query)

    try:
        if rt_data["ratingSummary"]["topCritics"]["averageRating"] != -1:
            query = """MATCH (m: Movie {imdb_id: "%s"})
                        SET m.critics_rating = %f
                        """ % (
                imdb_id,
                float(rt_data["ratingSummary"]["topCritics"]["averageRating"]),
            )
            queries.append(query)
    except KeyError:
        pass

    if "ratings" in rt_data.keys():
        query = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.critics_score = %f
                SET m.audience_score = %f;
                """ % (
            imdb_id,
            float(rt_data["ratings"]["critics_score"]),
            float(rt_data["ratings"]["audience_score"]),
        )
        queries.append(query)

    if "synopsis" in rt_data.keys():
        query = """MATCH (m: Movie {imdb_id: "%s"})
                SET m.synopsis = "%s";
                """ % (
            imdb_id,
            utils.cypher_escape(rt_data["synopsis"]),
        )
        queries.append(query)

    if "reviews" in rt_data.keys():
        reviews_list = []
        for review in rt_data["reviews"]["reviews"]:
            try:
                reviews_list.append(review["quote"])
            except KeyError:
                pass
        reviews = ". ".join(reviews_list)
        query = """MATCH (m: Movie {imdb_id: "%s"})
                    SET m.reviews = "%s";
                    """ % (
            imdb_id,
            utils.cypher_escape(reviews),
        )
        queries.append(query)

    with add_rotten_tomatoes_data.neo4_client.session() as session:
        for query in queries:
            utils.run_query(query, session)

    return "Data added"


@celery_app.task(name="tasks.add_ibm_data", base=TaskWithRetry)
def add_ibm_data(  # pylint: disable=too-many-locals, too-many-branches
    imdb_id: str,
) -> str:
    with add_ibm_data.neo4j_client.session() as session:
        query = (
            """MATCH (m: Movie {imdb_id: "%s"})
            RETURN m.ibm_data, m.plot, m.description,
                   m.synopsis, m.reviews, m.consensus;
            """
            % imdb_id
        )
        data_flag = utils.run_query(query, session).data()

        if data_flag[0]["m.ibm_data"]:
            return "No data added"
        text_list = []
        for item in data_flag[0]:
            if data_flag[0][item] and isinstance(data_flag[0][item], str):
                text_list.append(data_flag[0][item])
        text = ". ".join(text_list)

    if not text:
        log.info("Movie %s does not have text, skipping.", imdb_id)
        return "No data added"
    queries = []
    try:
        log.info("Getting IBM data for %s.", imdb_id)
        ibm_data = next(
            add_ibm_data.ibm_client.get(
                path="/v1/analyze",
                params={
                    "version": "2019-07-12",
                    "features": "emotion,categories",
                    "text": text,
                },
            )
        )
    except (requests.exceptions.RequestException, StopIteration) as err:
        log.warning("Cannot get IBM info for %s: %s.", imdb_id, repr(err))
        raise

    for cat in set(ibm_data["categories"]):
        if cat["score"] > 0.75:
            categories = cat["label"].split("/")
            categories = [c.lower().strip() for c in categories if c]
            if categories:
                for i, category in enumerate(categories[1:]):
                    if i == 0:
                        query = (
                            'MERGE (c:Category {name: "%s"})'
                            % utils.cypher_escape(category.lower().strip())
                        )
                    else:
                        query = """MATCH (n:Category {name: "%s"})
                        MERGE (c:Category {name: "%s"})
                        WITH c, n
                        MERGE (n)-[:HAS_SUBCATEGORY]->(c)
                        """ % (
                            utils.cypher_escape(categories[i - 1]),
                            utils.cypher_escape(category),
                        )
                    queries.append(query)
                for category in categories[1:]:
                    query = """
                        MATCH (m:Movie {imdb_id: "%s"})
                        SET m.ibm_data = true
                        MERGE (c:Category {name: "%s"})
                        WITH m, c
                        MERGE (c)-[r:HAS_MOVIE]->(m)
                        SET r.score = %f;
                        """ % (
                        imdb_id,
                        utils.cypher_escape(category),
                        cat["score"],
                    )
                    queries.append(query)
    for emotion, score in ibm_data["emotion"]["document"]["emotion"].items():
        query = """MATCH (m: Movie {imdb_id: "%s"})
            SET m.ibm_data = true
            SET m.%s = %f
            """ % (
            imdb_id,
            emotion.lower().strip(),
            score,
        )
        queries.append(query)

    with add_ibm_data.neo4j_client.session() as session:
        for query in queries:
            utils.run_query(query, session)

    return "Data added"


@celery_app.task(name="tasks.update_database", base=TaskWithRetry)
def update_database() -> str:
    log.info("Updating movie database...")

    constraints = [
        "CREATE CONSTRAINT ON (m:Movie) ASSERT m.imdb_id IS UNIQUE",
        "CREATE CONSTRAINT ON (g:Genre) ASSERT g.name IS UNIQUE",
        "CREATE CONSTRAINT ON (c:Category) ASSERT c.name IS UNIQUE",
        "CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
    ]
    for constraint in constraints:
        with update_database.neo4j_client.session() as session:
            utils.run_query(constraint, session)

    # Get movies and series from Ororo
    ororo_movies = update_database.ororo_client.get(path="movies")
    job = group(
        (add_media.si(item, "movies", "ororo") for item in ororo_movies)
    )
    job.apply_async()
    ororo_shows = update_database.ororo_client.get(path="shows")
    job = group((add_media.si(item, "shows", "ororo") for item in ororo_shows))
    job.apply_async()

    # # Get movies from Mubi
    # mubi_movies = update_database.mubi_client.get(path="films")
    # job = group((add_media.si(item, "movies", "mubi") for item in mubi_movies))
    # job.apply_async()

    return "Started update"
