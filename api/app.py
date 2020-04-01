import os
import warnings
import json
from urllib.parse import unquote

from flask import Flask, request, render_template, redirect, url_for
from neo4j import GraphDatabase

from worker import celery

# App
app = Flask(__name__)


# parameters
warnings.filterwarnings("ignore")

# Configs
with open("config.json") as f:
    config = json.load(f)

# Flask configs
app.config["UPLOAD_FOLDER"] = os.path.join("static", "plots")

# Database
neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False,)

# Endpoints
@app.route("/")
def home():
    return render_template("home.html")


@app.route("/update")
def update_db():
    celery.send_task("tasks.update_database")
    return redirect(url_for("home"))


@app.route("/media")
def media():
    return render_template("media.html")


@app.route("/media/details")
def get_media():
    title = request.args.get("title")
    with neo.session() as s:
        response = s.run("MATCH (m:Movie {name: '%s'}) RETURN m" % title).data()[0]
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
            'MATCH (c:Category {name: "%s"})-[:HAS_MOVIE]->(m:Movie) RETURN m.name'
            % category
        ).values()
        media = [item for sublist in media for item in sublist]
    return render_template("categories_media.html", category=category, media=media)


@app.errorhandler(404)
def not_found_error():
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template("500.html", error=error), 500
