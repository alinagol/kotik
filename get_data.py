import json
from pymongo import MongoClient
import imdbdriver


def get_movie(imdbdriver, database, title):

    m = imdbdriver.get_movie(title)
    database.insert_one(m)


def main():

    imdb = imdbdriver.imdb(config['imdb']['key'])

    with MongoClient(config["mongodb"]["host"], config["mongodb"]["port"]) as client:
        movies = client[config['mongodb']['database']][config['mongodb']['collection']]

    titles = config['movies']

    for title in titles:
        print(title)
        get_movie(imdb, movies, title)


if __name__ == "__main__":

    config = json.load(open('config/config.json'))

    main()
