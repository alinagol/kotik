import requests


class imdb:

    """
    Driver for omdbapi data
    """

    def __init__(self, key):
        self.url = f'http://www.omdbapi.com/?apikey={key}&'

    def get_movie(self, title):

        r = requests.get(self.url+f't={title}')

        return r.json()
