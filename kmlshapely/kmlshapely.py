import logging
from typing import List
from xml.etree.ElementTree import Element, fromstring

from bs4 import BeautifulSoup
from shapely.geometry.base import BaseGeometry
from shapely.geometry.polygon import Polygon
from shapely.ops import cascaded_union

from .border import Border

__log = logging.getLogger(__name__)

ns = "{http://www.opengis.net/kml/2.2}"

Borders = List[Border]
Shapes = List[BaseGeometry]


def kml_to_shapely(data: bytes) -> Borders:
    """

    :rtype: Borders
    :param data:
    """

    tree = fromstring(data)
    rv = []
    for placemark in tree.findall(".//" + ns + "Placemark"):
        name = placemark.findtext(ns + "name")
        __log.debug("Parsing placemark: %s", name)
        # MultiGeometry lub LinearRing?
        geo = placemark.find(ns + "MultiGeometry")
        description = BeautifulSoup(placemark.findtext("{http://www.opengis.net/kml/2.2}description"), "html.parser")
        tags = dict(zip(
            map(lambda x: x.text, description.find_all('span', class_='atr-name')),
            map(lambda x: x.text, description.find_all('span', class_='atr-value'))
        ))
        polygon = geo.find(ns + "Polygon")
        outer = cascaded_union([ring_to_shape(x) for x in polygon.findall(ns + "outerBoundaryIs/" + ns + "LinearRing")])
        inner = cascaded_union([ring_to_shape(x) for x in polygon.findall(ns + "innerBoundaryIs/" + ns + "LinearRing")])
        border = Border(outer.difference(inner))
        border.set_tag('name', name)
        for key, value in tags.items():
            border.set_tag(key, value)
        rv.append(border)
    return rv


def ring_to_shape(tree: Element) -> BaseGeometry:
    coordinates = []
    for point_text in tree.findtext(ns + "coordinates").split():
        floats = point_text.split(",")
        coordinates.append((float(floats[0]), float(floats[1])))
    if coordinates[0] == coordinates[-1]:
        return Polygon(coordinates)
    else:
        raise Exception("Not a polygon")
        # return LineString(coordinates)
