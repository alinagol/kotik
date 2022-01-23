import logging
from typing import Any, Dict, Generator, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

retry_strategy = Retry(
    total=3, backoff_factor=1, status_forcelist=[502, 503, 504, 429]
)
adapter = HTTPAdapter(max_retries=retry_strategy)


class DataSource:
    def __init__(
        self,
        url: str,
        auth: Union[None, Tuple[str, str]] = None,
        **kwargs: Any,
    ):
        self.url = url
        self.auth = auth
        self.kwargs = kwargs

    def get(
        self,
        params: Union[Dict[str, Any], None] = None,
        path: Union[str, None] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        headers = self.kwargs.get("headers")
        try:
            with requests.session() as session:
                if headers:
                    session.headers = headers
                session.auth = self.auth
                session.mount("https://", adapter)
                session.mount("http://", adapter)

                response = session.get(f"{self.url}/{path}", params=params)
                response.raise_for_status()

                if path and path in response.json().keys():
                    result = response.json()[path]
                else:
                    result = response.json()

                if isinstance(result, list):
                    for item in result:
                        yield item
                else:
                    yield result

        except requests.exceptions.HTTPError as http_err:
            log.error(http_err)
            raise


class Ororo(DataSource):
    def __init__(self, url: str, username: str, password: str):
        DataSource.__init__(
            self,
            url=url,
            auth=(username, password),
            headers={"User-Agent": "kotik"},
        )


class Mubi(DataSource):
    def get(
        self,
        params: Union[Dict[str, Any], None] = None,
        path: Union[str, None] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        page = 1
        while True:
            try:
                with requests.session() as session:
                    session.mount("https://", adapter)
                    session.mount("http://", adapter)

                    params = {"page": page}
                    response = session.get(f"{self.url}/{path}", params=params)
                    response.raise_for_status()
            except requests.exceptions.HTTPError as http_err:
                log.error(http_err)
                raise
            if response.json():
                page += 1
                for movie in response.json():
                    yield movie
            else:
                break


class IMDB(DataSource):
    def __init__(self, url: str, api_key: str):
        DataSource.__init__(
            self, url=f"{url}/?apikey={api_key}&plot=full&", auth=None
        )

    def get_id(self, title: str, year: int) -> str:
        imdb_data = self.get(params={"t": title, "y": year})
        return next(imdb_data)["imdbID"]


class IBMWatson(DataSource):
    def __init__(self, url, api_key: str):
        DataSource.__init__(self, url=url, auth=("apikey", api_key))


class RottenTomatoes(DataSource):
    def __init__(self, url: str):
        DataSource.__init__(self, url=f"{url}/", auth=None)

    def get(
        self,
        params: Union[Dict[str, Any], None] = None,
        path: Union[str, None] = None,
    ) -> Generator[Dict[str, Any], None, None]:

        try:
            with requests.session() as session:
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                response = session.get(f"{self.url}/v1.0/movies/{path}")
                yield response.json()
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
                        result = self.get(path=correct_path)
                        yield from result
                except requests.exceptions.HTTPError as http_err:
                    log.error(http_err)
                    raise
            else:
                raise
