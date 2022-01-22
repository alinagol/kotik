import json

from datasource import IMDB, IBMWatson, Ororo, RottenTomatoes
from neo4j import GraphDatabase

with open("config.json") as f:
    config = json.load(f)


def init_neo4j_client():
    neo = GraphDatabase.driver(config["neo4j"]["url"], encrypted=False)
    return neo


def init_ororo_client():
    ororo = Ororo(
        url=config["ororo"]["url"],
        username=config["ororo"]["username"],
        password=config["ororo"]["password"],
    )
    return ororo


def init_ibm_client():
    ibm_client = IBMWatson(url=config["url"], api_key=config["apikey"])
    return ibm_client


def init_rotten_tomatoes_client():
    rotten_tomatoes_client = RottenTomatoes(url=config["url"])
    return rotten_tomatoes_client


def init_imdb_client():
    imdb_client = IMDB(url=config["url"], api_key=config["apikey"])
    return imdb_client
