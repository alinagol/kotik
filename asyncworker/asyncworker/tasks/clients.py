import json

from neo4j import GraphDatabase

from asyncworker.tasks.datasource import (
    IMDB,
    IBMWatson,
    Mubi,
    Ororo,
    RottenTomatoes,
)

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


def init_mubi_client():
    mubi = Mubi(url=config["mubi"]["url"])
    return mubi


def init_ibm_client():
    ibm_client = IBMWatson(
        url=config["ibm"]["url"], api_key=config["ibm"]["apikey"]
    )
    return ibm_client


def init_rotten_tomatoes_client():
    rotten_tomatoes_client = RottenTomatoes(
        url=config["rotten_tomatoes"]["url"]
    )
    return rotten_tomatoes_client


def init_imdb_client():
    imdb_client = IMDB(
        url=config["imdb"]["url"], api_key=config["imdb"]["apikey"]
    )
    return imdb_client
