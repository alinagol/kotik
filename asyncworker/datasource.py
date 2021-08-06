import logging
from typing import Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

retry_strategy = Retry(
    total=3, backoff_factor=1, status_forcelist=[502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)


class DataSource:
    def __init__(
        self, url: str, auth: Union[None, Tuple[str, str]] = None, **kwargs
    ):
        self.url = url
        self.auth = auth
        self.kwargs = kwargs

    def get(self, params: dict = None, path: str = None, **kwargs):
        if "headers" in kwargs:
            headers = kwargs["headers"]
        else:
            headers = None

        try:
            with requests.session() as session:

                session.headers = headers
                session.auth = self.auth
                session.mount("https://", adapter)
                session.mount("http://", adapter)

                response = session.get(
                    f"{self.url}/{path}", params=params, verify=False
                )
                response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            log.error(http_err)
            raise

        return response.json()


class Ororo(DataSource):
    def __init__(self, url: str, username: str, password: str):
        DataSource.__init__(
            self,
            url=url,
            auth=(username, password),
            headers={"User-Agent": "curl/7.51.0"},
        )


class Mubi(DataSource):
    def get(self, params: dict = None, path: str = None, **kwargs):
        result = []
        page = 1
        while True:
            try:
                with requests.session() as session:
                    session.mount("https://", adapter)
                    session.mount("http://", adapter)

                    params = {"page": page}
                    response = session.get(
                        f"{self.url}/{path}", params=params, verify=False
                    )
                    response.raise_for_status()
            except requests.exceptions.HTTPError as http_err:
                log.error(http_err)
                raise
            if response.json():
                result.append(response.json())
                page += 1
            else:
                break
        return result


class IMDB(DataSource):
    def __init__(self, url: str, api_key: str):
        DataSource.__init__(
            self, url=f"{url}/?apikey={api_key}&plot=full&", auth=None
        )


class IBMWatson(DataSource):
    def __init__(self, url, api_key: str):
        DataSource.__init__(self, url=url, auth=("apikey", api_key))


class RottenTomatoes(DataSource):
    def __init__(self, url: str):
        DataSource.__init__(self, url=f"{url}/", auth=None)

    def get(self, params: dict = None, path: str = None, **kwargs):

        try:
            with requests.session() as session:
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                response = session.get(
                    f"{self.url}/v1.0/movies/{path}", verify=False
                )
                return response.json()
        except requests.exceptions.HTTPError as http_err:
            log.warning(http_err)

            if params:
                try:
                    with requests.session() as session:
                        session.mount("https://", adapter)
                        session.mount("http://", adapter)
                        response = requests.get(
                            f"{self.url}/v2.0/search",
                            params={"q": params["title"], "type": "movies"},
                        )
                        try:
                            correct_path = response.json()["movies"][0][
                                "url"
                            ].split("/")[-1]
                        except IndexError:
                            log.error(
                                "Cannot find %s in Rotten Tomatoes.",
                                params["title"],
                            )
                            raise
                        return self.get(path=correct_path)
                except requests.exceptions.HTTPError as http_err:
                    log.error(http_err)
                    raise
            else:
                raise
