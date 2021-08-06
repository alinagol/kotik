import logging

import gensim
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import RegexpTokenizer
from params import NUM_TOPICS, TEXT_KEYS, mubi, neo, ororo
from sklearn.preprocessing import MinMaxScaler
from utils import (
    add_ibm_data,
    add_imdb_data,
    add_rotten_tomatoes_data,
    calculate_correlations,
    cypher_escape,
)

from asyncworker import celery_app  # type: ignore[attr-defined]

log = logging.getLogger(__name__)


@celery_app.task(name="tasks.find_similarities")
def find_similarities():
    log.info("Finding similarities...")
    # Clean up old similarities
    with neo.session() as session:
        q = "MATCH (:Movie)-[r:SIMILAR]-(:Movie) DETACH DELETE r"
        session.run(q)

    with neo.session() as session:
        q = "MATCH (m:Movie) RETURN m as movie"
        movies = session.run(q).data()

    tokenizer = RegexpTokenizer(r"\w+")
    stemmer = SnowballStemmer("english")
    stop_list = stopwords.words("english")

    dictionary = gensim.corpora.Dictionary()

    for movie in movies:
        text_list = []

        for key in TEXT_KEYS:
            if movie["movie"][key] and type(movie["movie"][key]) == str:
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
        for key in TEXT_KEYS:
            if movie["movie"][key] and type(movie["movie"][key]) == str:
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
        corpus_tfidf, id2word=dictionary, num_topics=NUM_TOPICS
    )

    index = gensim.similarities.MatrixSimilarity(lsi[corpus_tfidf])

    corr = calculate_correlations()
    corr_scaled = MinMaxScaler(feature_range=(-1, 1)).fit_transform(corr)

    log.info("Updating neo4j with similarities")

    for i, similarities in enumerate(index):
        log.debug(
            f"Updating neo4j with similarities for {movies[i]['movie']['slug']}"
        )
        assert i == movies[i]["gensim_id"]
        correlations = corr_scaled[i]
        sim_corr = 0.25 * similarities + 0.75 * correlations
        neighbours = sorted(enumerate(sim_corr), key=lambda item: -item[1])[
            1:11
        ]
        for j, similarity in neighbours:
            if similarity > 0.25:
                with neo.session() as session:
                    q = """MATCH (m:Movie {slug: "%s"})
                    MATCH (sm: Movie {slug: "%s"}) 
                    MERGE (m)-[r:SIMILAR]-(sm)
                    SET r.similarity = %f
                    """ % (
                        movies[i]["movie"]["slug"],
                        movies[j]["movie"]["slug"],
                        similarity,
                    )
                    session.run(q)


@celery_app.task(name="tasks.update_database")
def update_database():
    log.info("Updating movie database...")

    index = [
        "CREATE INDEX movie_slug IF NOT EXISTS FOR (m:Movie) ON (m.slug, m.name)",
        "CREATE INDEX genre_name IF NOT EXISTS FOR (g:Genre) ON (g.name)",
        "CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)",
        "CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
    ]
    for i in index:
        with neo.session() as session:
            session.run(i)

    # Get movies and series from Ororo
    ororo_movies = ororo.get(path="movies")["movies"]
    ororo_shows = ororo.get(path="shows")["shows"]
    log.info(
        f"Found {len(ororo_movies)} movies and {len(ororo_shows)} shows in Ororo."
    )

    ororo_library = {"movies": ororo_movies, "shows": ororo_shows}

    for media_type, items in ororo_library.items():
        for item in items:
            item_clean = {
                key: cypher_escape(value) for key, value in item.items()
            }
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
                        add_imdb_data(imdb_id)
                    if not media[0]["m.rotten_tomatoes_data"]:
                        add_rotten_tomatoes_data(imdb_id)
                    if not media[0]["m.ibm_data"]:
                        add_ibm_data(imdb_id)
                    log.info(
                        f'Skipping {media[0]["m.name"]}, already in Neo4j'
                    )
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
                item_clean["poster_thumb"],
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
            add_imdb_data(imdb_id)
            add_rotten_tomatoes_data(imdb_id)
            add_ibm_data(imdb_id)

    # Get movies from Mubi
    mubi_movies = mubi.get(path="films")
    for movie in mubi_movies:
        with neo.session() as session:

            q = """MERGE (m: Movie {name: "%s", 
            year: %i,
            """ % (
                movie["title"],
                movie["year"],
            )
            session.run(q)

            q = """MATCH (m: Movie {name: "%s"}) 
            SET m.mubi=true, m.mubi_popularity=%i, m.still_average_colour="%s"
            """ % (
                movie["title"],
                int(movie["popularity"]),
                movie["still_average_colour"],
            )

            session.run(q)
