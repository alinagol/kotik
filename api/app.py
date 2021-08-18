# pylint: disable=line-too-long

import json
import logging
import os
from urllib.parse import unquote

from celery import Celery
from flask import Flask, redirect, render_template, request, url_for
from neo4j import GraphDatabase
from utils import emotions_chart

# App
app = Flask(__name__)

# Parameters
app.config["UPLOAD_FOLDER"] = os.path.join("static", "plots")
if not os.path.exists(os.path.join("static", "plots")):
    os.makedirs(os.path.join("static", "plots"))

# Logging
log = logging.getLogger(__name__)

# Configs
with open("config.json") as f:
    config = json.load(f)

# Database
neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

# Asyncworker
broker_url = result_backend = os.getenv("REDIS_URL", "redis://redis:6379")
celery_parameters = {
    "main": "asyncworker",
    "broker": broker_url,
    "backend": broker_url,
}
celery_app = Celery(**celery_parameters)


@app.errorhandler(404)
def not_found_error(_error):
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template("500.html", params=request.args, error=error), 500


@app.route("/")
def home():
    with neo.session() as session:
        count = (
            session.run("MATCH (m:Movie) RETURN count(m.slug)")
            .single()
            .value()
        )
    return render_template("home.html", count=count)


@app.route("/choose")
def choose():
    with neo.session() as session:
        categories = session.run(
            "MATCH (c:Category) RETURN DISTINCT c.name ORDER BY c.name",
        ).values()
        categories = [item for sublist in categories for item in sublist]
        genres = session.run(
            "MATCH (g:Genre) RETURN DISTINCT g.name ORDER BY g.name"
        ).values()
        genres = [item for sublist in genres for item in sublist]
    return render_template("choose.html", categories=categories, genres=genres)


@app.route("/choose/results")
def choose_results():

    media_type = request.args.get("type")
    genres = request.args.getlist("genres")
    categories = request.args.getlist("categories")
    rating = request.args.get("rating")
    year = request.args.get("year")

    params = []
    if media_type:
        params.append(f'type: "{media_type}"')
    if year:
        params.append(f"year: {year}")
    join_params = ",".join(params)

    if join_params:
        queries = ["MATCH (m: Movie {%s})\n" % join_params]
    else:
        queries = ["MATCH (m: Movie)\n"]

    if rating:
        queries.append(f"WHERE m.imdb_rating > {rating}\n")
    if genres:
        for genre in genres:
            if genre:
                queries.append(
                    'MATCH (m)<-[:HAS_MOVIE]-(:Genre {name: "%s"})\n'
                    % unquote(genre)
                )
    if categories:
        for category in categories:
            if category:
                queries.append(
                    'MATCH (m)<-[:HAS_MOVIE]-(:Category {name: "%s"})\n'
                    % unquote(category)
                )
    queries.append(
        """RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating}
        ORDER BY m.imdb_rating DESC
        """
    )

    query = " ".join(queries)

    with neo.session() as session:
        media = session.run(query).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", media=media, filter=request.args)


@app.route("/update")
def update_db():
    celery_app.send_task("tasks.update_database", queue="default")
    return redirect(url_for("home"))


@app.route("/similarity")
def similarity():
    celery_app.send_task("tasks.find_similarities", queue="default")
    return redirect(url_for("home"))


@app.route("/rating")
def choose_best():
    ratings = [
        "critics_rating",
        "audience_score",
        "imdb_rating",
        "critics_score",
        "joy",
        "disgust",
        "fear",
        "anger",
        "sadness",
    ]
    return render_template("media_filter.html", filter="rating", items=ratings)


@app.route("/rating/media")
def best_media():
    rating = unquote(request.args.get("rating"))
    with neo.session() as session:
        media = session.run(
            """MATCH (m:Movie)
            WHERE m.%s IS NOT NULL
            WITH m
            ORDER BY m.%s DESC
            RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.%s}
            """
            % (rating, rating, rating)
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=rating, media=media)


@app.route("/media")
def search_media():
    return render_template("media_search.html")


@app.route("/media/details")
def get_media():
    media_id = unquote(request.args.get("id"))
    with neo.session() as session:
        media = (
            session.run(
                """MATCH (m:Movie {imdb_id: '%s'})
                RETURN m as movie
                """
                % media_id
            )
            .single()
            .value()
        )
        categories = session.run(
            """MATCH (m:Movie {imdb_id: '%s'})<-[:HAS_MOVIE]-(c:Category)
            RETURN DISTINCT c.name
            """
            % media_id
        ).values()
        genres = session.run(
            """MATCH (m:Movie {imdb_id: '%s'})<-[:HAS_MOVIE]-(g:Genre)
            RETURN DISTINCT g.name
            """
            % media_id
        ).values()

        categories = [item for sublist in categories for item in sublist]
        genres = [item for sublist in genres for item in sublist]

        similar = session.run(
            """MATCH (m:Movie {imdb_id: '%s'})
            OPTIONAL MATCH (m)-[:SIMILAR]-(om:Movie)
            WITH om
            ORDER BY om.imdb_rating DESC
            RETURN DISTINCT collect({id: om.imdb_id, title: om.name, poster: om.poster, description: om.description, rating: om.imdb_rating}) as similar
            """
            % media_id
        ).values()
        similar = similar[0][0]

    try:
        filename = emotions_chart(media, app.config["UPLOAD_FOLDER"])
    except KeyError:
        filename = None
    return render_template(
        "media_details.html",
        media=media,
        genres=genres,
        categories=categories,
        similar=similar,
        plot=filename,
    )


@app.route("/actors")
def choose_actors():
    with neo.session() as session:
        actors = session.run(
            """MATCH (p:Person)-[:ACTED_IN]->(:Movie)
            RETURN DISTINCT p.name
            ORDER BY p.name
            """
        ).values()
        actors = [item for sublist in actors for item in sublist]
    return render_template("media_filter.html", filter="actors", items=actors)


@app.route("/actors/media")
def actors_media():
    actor = unquote(request.args.get("actors"))
    with neo.session() as session:
        media = session.run(
            """MATCH (p:Person {name: "%s"})-[:ACTED_IN]->(m:Movie)
            RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating}
            ORDER BY m.imdb_rating DESC
            """
            % actor
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template(
        "media_list.html", filter=actor.capitalize(), media=media
    )


@app.route("/directors")
def choose_directors():
    with neo.session() as session:
        directors = session.run(
            """MATCH (p:Person)-[:DIRECTED]->(:Movie)
            RETURN DISTINCT p.name
            ORDER BY p.name
            """
        ).values()
        directors = [item for sublist in directors for item in sublist]
    return render_template(
        "media_filter.html", filter="directors", items=directors
    )


@app.route("/directors/media")
def directors_media():
    director = unquote(request.args.get("directors"))
    with neo.session() as session:
        media = session.run(
            """MATCH (p:Person {name: "%s"})-[:DIRECTED]->(m:Movie)
            RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating}
            ORDER BY m.imdb_rating DESC
            """
            % director
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template(
        "media_list.html", filter=director.capitalize(), media=media
    )


@app.route("/categories/subcategories")
def choose_subcategories():
    with neo.session() as session:
        subcategories = session.run(
            """MATCH (c:Category)
            WHERE NOT (c)<-[:HAS_SUBCATEGORY]-()
            MATCH (c:Category)-[:HAS_MOVIE]->(m:Movie)
            OPTIONAL MATCH (c)-[:HAS_SUBCATEGORY]->(sc:Category)
            WITH c, sc, count(distinct m) as n
            ORDER BY n DESC
            RETURN {category: c.name, subcategories: collect(distinct sc.name), movies: n}
            """
        ).values()
        subcategories = [item for sublist in subcategories for item in sublist]
    return render_template("categories.html", subcategories=subcategories)


@app.route("/categories")
def choose_categories():
    with neo.session() as session:
        categories = session.run(
            """MATCH (c:Category)
            WHERE NOT (c)<-[:HAS_SUBCATEGORY]-()
            RETURN DISTINCT c.name
            ORDER BY c.name
            """
        ).values()
        categories = [item for sublist in categories for item in sublist]
    return render_template(
        "media_filter.html", filter="categories", items=categories
    )


@app.route("/categories/media")
def categories_media():
    category = unquote(request.args.get("categories"))
    with neo.session() as session:
        media = session.run(
            """MATCH (c:Category {name: "%s"})-[:HAS_MOVIE]->(m:Movie)
            RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating}
            ORDER BY m.imdb_rating DESC
            """
            % category
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=category, media=media)


@app.route("/genres")
def choose_genres():
    with neo.session() as session:
        genres = session.run(
            """MATCH (g:Genre)
            RETURN DISTINCT g.name
            ORDER BY g.name
            """
        ).values()
        genres = [item for sublist in genres for item in sublist]
    return render_template("media_filter.html", filter="genres", items=genres)


@app.route("/genres/media")
def genres_media():
    genre = unquote(request.args.get("genres"))
    with neo.session() as session:
        media = session.run(
            """MATCH (g:Genre {name: "%s"})-[:HAS_MOVIE]->(m:Movie)
            RETURN {title: m.name, id: m.imdb_id, description: m.description, poster: m.poster, rating: m.imdb_rating}
            ORDER BY m.imdb_rating DESC
            """
            % genre
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=genre, media=media)
