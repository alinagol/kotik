import json
from math import pi
import logging
import os
from urllib.parse import unquote
import warnings

from flask import Flask, request, render_template, redirect, url_for, jsonify
import matplotlib
import matplotlib.pyplot as plt
from neo4j import GraphDatabase

from worker import celery

# App
app = Flask(__name__)

# Parameters
warnings.filterwarnings("ignore")
app.config["UPLOAD_FOLDER"] = os.path.join("static", "plots")

matplotlib.use("agg")

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
@app.route("/config")
def config():
    return jsonify(app.config())


@app.route("/")
def home():
    with neo.session() as s:
        count = s.run("MATCH (m:Movie) RETURN count(m.slug)").single().value()
    return render_template("home.html", count=count)


@app.route("/choose")
def choose():
    with neo.session() as s:
        categories = s.run(
            "MATCH (c:Category) RETURN DISTINCT c.name ORDER BY c.name"
        ).values()
        categories = [item for sublist in categories for item in sublist]
        genres = s.run(
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
    queries.append("RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating} ORDER BY m.imdb_rating DESC")

    q = " ".join(queries)

    with neo.session() as s:
        media = s.run(q).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", media=media, filter=request.args)


@app.route("/update")
def update_db():
    celery.send_task("tasks.update_database")
    return redirect(url_for("home"))


@app.route("/similarity")
def similarity():
    celery.send_task("tasks.find_similarities")
    return redirect(url_for("home"))

@app.route("/rating")
def best():
    ratings = ["critics_rating", "audience_score", "imdb_rating", "critics_score", "joy", "disgust", "fear", "anger", "sadness"]
    return render_template("media_filter.html", filter="rating", items=ratings)


@app.route("/rating/media")
def best_media():
    rating = unquote(request.args.get("rating"))
    with neo.session() as s:
        media = s.run(
            'MATCH (m:Movie) WHERE m.%s IS NOT NULL WITH m ORDER BY m.%s DESC RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.%s}'
            % (rating, rating, rating)
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=rating, media=media)


@app.route("/media")
def search_media():
    return render_template("media_search.html")


@app.route("/media/details")
def get_media():
    id = unquote(request.args.get("id"))
    with neo.session() as s:
        media = (
            s.run("MATCH (m:Movie {imdb_id: '%s'}) RETURN m as movie" % id).single().value()
        )
        categories = s.run("MATCH (m:Movie {imdb_id: '%s'})<-[:HAS_MOVIE]-(c:Category) RETURN DISTINCT c.name" % id).values()
        genres = s.run("MATCH (m:Movie {imdb_id: '%s'})<-[:HAS_MOVIE]-(g:Genre) RETURN DISTINCT g.name" % id).values()

        categories = [item for sublist in categories for item in sublist]
        genres = [item for sublist in genres for item in sublist]

        similar = s.run("MATCH (m:Movie {imdb_id: '%s'}) OPTIONAL MATCH (m)-[:SIMILAR]-(om:Movie) WITH om ORDER BY om.imdb_rating DESC RETURN DISTINCT collect({id: om.imdb_id, title: om.name, poster: om.poster, description: om.description, rating: om.imdb_rating}) as similar" % id).values()
        similar = similar[0][0]

    filename = emotions_chart(media)
    return render_template("media_details.html", media=media, genres=genres, categories=categories, similar=similar, plot=filename)


@app.route("/actors")
def actors():
    with neo.session() as s:
        actors = s.run(
            "MATCH (p:Person)-[:ACTED_IN]->(:Movie) RETURN DISTINCT p.name ORDER BY p.name"
        ).values()
        actors = [item for sublist in actors for item in sublist]
    return render_template("media_filter.html", filter="actors", items=actors)


@app.route("/actors/media")
def actors_media():
    actor = unquote(request.args.get("actors"))
    with neo.session() as s:
        media = s.run(
            'MATCH (p:Person {name: "%s"})-[:ACTED_IN]->(m:Movie) RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating} ORDER BY m.imdb_rating DESC'
            % actor
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=actor.capitalize(), media=media)


@app.route("/directors")
def directors():
    with neo.session() as s:
        directors = s.run(
            "MATCH (p:Person)-[:DIRECTED]->(:Movie) RETURN DISTINCT p.name ORDER BY p.name"
        ).values()
        directors = [item for sublist in directors for item in sublist]
    return render_template("media_filter.html", filter="directors", items=directors)


@app.route("/directors/media")
def directors_media():
    director = unquote(request.args.get("directors"))
    with neo.session() as s:
        media = s.run(
            'MATCH (p:Person {name: "%s"})-[:DIRECTED]->(m:Movie) RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating} ORDER BY m.imdb_rating DESC'
            % director
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template(
        "media_list.html", filter=director.capitalize(), media=media
    )

@app.route("/categories/subcategories")
def subcategories():
    with neo.session() as s:
        subcategories = s.run(
            "MATCH (c:Category) WHERE NOT (c)<-[:HAS_SUBCATEGORY]-() MATCH (c:Category)-[:HAS_MOVIE]->(m:Movie) OPTIONAL MATCH (c)-[:HAS_SUBCATEGORY]->(sc:Category) WITH c, sc, count(distinct m) as n ORDER BY n DESC RETURN {category: c.name, subcategories: collect(distinct sc.name), movies: n}"
        ).values()
        subcategories = [item for sublist in subcategories for item in sublist]
    return render_template("categories.html", subcategories=subcategories)

@app.route("/categories")
def categories():
    with neo.session() as s:
        categories = s.run("MATCH (c:Category) WHERE NOT (c)<-[:HAS_SUBCATEGORY]-() RETURN DISTINCT c.name ORDER BY c.name").values()
        categories = [item for sublist in categories for item in sublist]
    return render_template("media_filter.html", filter="categories", items=categories)


@app.route("/categories/media")
def categories_media():
    category = unquote(request.args.get("categories"))
    with neo.session() as s:
        media = s.run(
            'MATCH (c:Category {name: "%s"})-[:HAS_MOVIE]->(m:Movie) RETURN {title: m.name, id: m.imdb_id, poster: m.poster, description: m.description, rating: m.imdb_rating} ORDER BY m.imdb_rating DESC'
            % category
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=category, media=media)


@app.route("/genres")
def genres():
    with neo.session() as s:
        genres = s.run("MATCH (g:Genre) RETURN DISTINCT g.name ORDER BY g.name").values()
        genres = [item for sublist in genres for item in sublist]
    return render_template("media_filter.html", filter="genres", items=genres)


@app.route("/genres/media")
def genres_media():
    genre = unquote(request.args.get("genres"))
    with neo.session() as s:
        media = s.run(
            'MATCH (g:Genre {name: "%s"})-[:HAS_MOVIE]->(m:Movie) RETURN {title: m.name, id: m.imdb_id, description: m.description, poster: m.poster, rating: m.imdb_rating} ORDER BY m.imdb_rating DESC'
            % genre
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("media_list.html", filter=genre, media=media)


@app.errorhandler(404)
def not_found_error():
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template("500.html", params=request.args, error=error), 500


def emotions_chart(media):
    emotions = ["sadness", "anger", "joy", "disgust", "fear"]
    N = len(emotions)

    try:
        values = [media[emotion] for emotion in emotions]
    except KeyError:
        return

    values.append(values[0])

    angles = [n / float(N) * 2 * pi for n in range(N)]
    angles += angles[:1]

    fig = plt.figure(figsize=(3,3))
    ax = fig.add_subplot(111, polar=True)
    plt.xticks(angles[:-1], emotions, color="grey", size=12)
    ax.set_rlabel_position(0)
    plt.yticks([0.25, 0.5, 0.75], ["0.25", "0.5", "0.75"], color="grey", size=8)
    plt.ylim(0, 1)
    ax.plot(angles, values, linewidth=1, linestyle="solid")
    ax.fill(angles, values, "b", alpha=0.1)

    filename = f'{app.config["UPLOAD_FOLDER"]}/{media["slug"]}.png'
    plt.tight_layout()
    plt.savefig(filename)
    plt.clf()

    return filename
