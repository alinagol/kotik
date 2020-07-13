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
            response.raise_for_status()
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


class RottenTomatoes(Datasource):
    def __init__(self, url):
        Datasource.__init__(self, url=f"{url}/", auth=None)
    def get(self, params=None, path=None, **kwargs):
        with requests.session() as s:
            response = s.get(f"{self.url}/v1.0/movies/{path}", verify=False)
            if response.status_code == 200:
                return response.json()
            else:
                response = requests.get(f"{self.url}/v2.0/search", params={"q": params["title"], "type": "movies"})
                if response.status_code == 200:
                    correct_path = response.json()["movies"][0]["url"].split("/")[-1]
                    return self.get(path=correct_path)
