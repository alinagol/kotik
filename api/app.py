import warnings
import json
import logging
from urllib.parse import unquote

from flask import Flask, request, render_template, redirect, url_for
from neo4j import GraphDatabase

from worker import celery

# App
app = Flask(__name__)

# Parameters
warnings.filterwarnings("ignore")

# Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
log = logging.getLogger(__name__)

# Configs
with open("config.json") as f:
    config = json.load(f)

# Database
neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)

# Endpoints
@app.route("/")
def home():
    with neo.session() as s:
        count = s.run("MATCH (m:Movie) RETURN count(m.slug)").single().value()
    return render_template("home.html", count=count)


@app.route("/choose")
def choose():
    with neo.session() as s:
        categories = s.run("MATCH (c:Category) RETURN c.name").values()
        categories = [item for sublist in categories for item in sublist]
        genres = s.run("MATCH (g:Genre) RETURN g.name").values()
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
    params = ",".join(params)

    if params:
        queries = ["MATCH (m: Movie {%s})\n" % params]
    else:
        queries = ["MATCH (m: Movie)\n"]

    if rating:
        queries.append(f"WHERE m.imdb_rating > {rating}\n")
    if genres:
        for genre in genres:
            if genre:
                queries.append(
                    'MATCH (m)<-[:HAS_MOVIE]-(:Genre {name: "%s"})\n' % unquote(genre)
                )
    if categories:
        for category in categories:
            if category:
                queries.append(
                    'MATCH (m)<-[:HAS_MOVIE]-(:Category {name: "%s"})\n'
                    % unquote(category)
                )
    queries.append("RETURN {title: m.name, slug: m.slug}")

    q = " ".join(queries)

    with neo.session() as s:
        media = s.run(q).values()
        media = [item for sublist in media for item in sublist]
    return render_template("choose_results.html", media=media, filters=request.args)


@app.route("/update")
def update_db():
    celery.send_task("tasks.update_database")
    return redirect(url_for("home"))


@app.route("/media/details")
def get_media():
    title = unquote(request.args.get("slug"))
    with neo.session() as s:
        response = (
            s.run("MATCH (m:Movie {slug: '%s'}) RETURN m" % title).single().value()
        )
    return render_template("media_details.html", media=response)


@app.route("/categories")
def topics():
    with neo.session() as s:
        categories = s.run("MATCH (c:Category) RETURN c.name").values()
        categories = [item for sublist in categories for item in sublist]
    return render_template("categories.html", categories=categories)


@app.route("/categories/media")
def topics_media():
    category = unquote(request.args.get("category"))
    with neo.session() as s:
        media = s.run(
            'MATCH (c:Category {name: "%s"})-[:HAS_MOVIE]->(m:Movie) RETURN {title: m.name, slug: m.slug}'
            % category
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("categories_media.html", category=category, media=media)


@app.route("/genres")
def genres():
    with neo.session() as s:
        genres = s.run("MATCH (g:Genre) RETURN g.name").values()
        genres = [item for sublist in genres for item in sublist]
    return render_template("genres.html", genres=genres)


@app.route("/genres/media")
def genres_media():
    genre = unquote(request.args.get("genre"))
    with neo.session() as s:
        media = s.run(
            'MATCH (g:Genre {name: "%s"})-[:HAS_MOVIE]->(m:Movie) RETURN {title: m.name, slug: m.slug}'
            % genre
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("genres_media.html", genre=genre, media=media)


@app.errorhandler(404)
def not_found_error():
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template("500.html", params=request.args, error=error), 500
