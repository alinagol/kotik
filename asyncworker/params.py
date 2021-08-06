import json

from datasource import IMDB, IBMWatson, Mubi, Ororo, RottenTomatoes
from neo4j import GraphDatabase

NUM_TOPICS = 500
TEXT_KEYS = ["plot", "description", "synopsis", "consensus"]

# Config
with open("config.json") as f:
    config = json.load(f)

# Clients
neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)
ororo = Ororo(
    url=config["ororo"]["url"],
    username=config["ororo"]["username"],
    password=config["ororo"]["password"],
)
mubi = Mubi(url=config["mubi"]["url"])
rotten_tomatoes_client = RottenTomatoes(url=config["rotten_tomatoes"]["url"])
ibm_client = IBMWatson(
    url=config["ibm"]["url"], api_key=config["ibm"]["apikey"]
)
imdb_client = IMDB(url=config["imdb"]["url"], api_key=config["imdb"]["apikey"])
