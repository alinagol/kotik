import requests


class imdbClient:

    """
    Driver for omdbapi data
    """

    def __init__(self, key):
        self.url = f"http://www.omdbapi.com/?apikey={key}&"

    def get_movie(self, imdbid):

        r = requests.get(self.url + f"i={imdbid}")

        if r.status_code == 200:
            return r.json()
        else:
            print(f"Error: {r.status_code}")
            return
