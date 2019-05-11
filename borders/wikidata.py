import json
import typing
import urllib.parse

import requests
import shapely.geometry
import shapely.wkt


class WikidataSimcEntry:
    def __init__(self, dct):
        self._point = shapely.wkt.loads(dct["coords"]["value"])
        self._wikidata = dct["miejscowosc"]["value"].replace(
            "http://www.wikidata.org/entity/", ""
        )
        self._terc = dct["terc"]["value"]
        wikipedia = dct["article"]["value"]
        self._wikipedia = urllib.parse.unquote_plus(
            wikipedia.replace("https://pl.wikipedia.org/wiki/", "")
        )
        self._miejscowosc = dct["miejscowoscLabel"]["value"]

    @property
    def wikidata(self) -> str:
        return self._wikidata

    @property
    def terc(self) -> str:
        return self._terc

    @property
    def wikipedia(self) -> str:
        return "pl:" + self._wikipedia

    @property
    def miejscowosc(self) -> str:
        return self._miejscowosc

    @property
    def point(self) -> shapely.geometry.Point:
        return self._point

    def __str__(self):
        return self.wikipedia + "/" + self.wikidata


def fetch_from_wikidata(terc: str) -> typing.List[WikidataSimcEntry]:
    query = """
    SELECT ?miejscowosc ?miejscowoscLabel ?gmina ?terc ?article ?coords
    WHERE
    {{
        ?gmina wdt:P1653 ?terc
        filter (?terc = '{0}') .
        ?miejscowosc wdt:P131 ?gmina .
        ?article schema:about ?miejscowosc .
        ?article schema:inLanguage "pl" .
        ?miejscowosc  wdt:P625 ?coords
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl" }}
    }}
    """.format(
        terc
    )
    resp = requests.get(
        "https://query.wikidata.org/sparql", params={"query": query, "format": "json"}
    )
    return from_json(resp.text)


def from_json(s: str) -> typing.List[WikidataSimcEntry]:
    rv = json.loads(s)
    return [WikidataSimcEntry(x) for x in rv["results"]["bindings"]]
