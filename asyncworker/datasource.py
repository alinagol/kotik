import requests
import warnings

warnings.filterwarnings("ignore")  # TODO add certificates and remove filter


class Datasource:
    def __init__(self, url, auth, **kwargs):
        self.url = url
        self.auth = auth
        self.kwargs = kwargs

    def get(self, params=None, path=None, **kwargs):
        if "headers" in kwargs:
            headers = kwargs["headers"]
        else:
            headers = None
        with requests.session() as s:
            s.headers = headers
            s.auth = self.auth
            response = s.get(f"{self.url}/{path}", params=params, verify=False)
            if response.status_code == 200:
                return response.json()


class Ororo(Datasource):
    def __init__(self, url, username, password):
        Datasource.__init__(
            self,
            url=url,
            auth=(username, password),
            headers={"User-Agent": "curl/7.51.0"},
        )


class IMDB(Datasource):
    def __init__(self, url, api_key):
        Datasource.__init__(self, url=f"{url}/?apikey={api_key}&plot=full&", auth=None)


class IBMWatson(Datasource):
    def __init__(self, url, api_key):
        Datasource.__init__(self, url=url, auth=("apikey", api_key))
