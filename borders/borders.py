import itertools
import math
import time
import typing
import xml.etree.ElementTree as ET

import cachetools.func
import requests
import shapely.geometry
import shapely.ops
from overpy import Overpass

from converters.kmlshapely import Feature
from converters.kmlshapely import kml_to_shapely
from converters.overpyshapely import OverToShape


@cachetools.func.ttl_cache(maxsize=128, ttl=600)
def get_adm_border(terc: str) -> shapely.geometry.Polygon:
    api = Overpass()
    result = api.query("""
[out:json];
relation
    ["teryt:terc"="%s"]
    ["boundary"="administrative"]
    ["admin_level"~"[79]"];
out bb;
>;
out bb;
    """ % (terc,))
    if not result.relations:
        raise ValueError("No relation found for terc: {0}".format(terc))
    return OverToShape(result).get_relation_shape()


def divide_bbox(bbox):
    (minx, miny, maxx, maxy) = bbox
    # EPSG:2180
    # __MAX_BBOX_X = 20000
    # __MAX_BBOX_Y = 45000
    # __PRECISION = 10
    __PRECISION = 1000000
    __MAX_BBOX_X = int(0.03 * __PRECISION)
    __MAX_BBOX_Y = int(0.04 * __PRECISION)
    return [
        (x / __PRECISION,
         y / __PRECISION,
         min(x / __PRECISION + __MAX_BBOX_X, maxx),
         min(y / __PRECISION + __MAX_BBOX_Y, maxy))
        for x in range(math.floor(minx * __PRECISION), math.ceil(maxx * __PRECISION), __MAX_BBOX_X * __PRECISION)
        for y in range(math.floor(miny * __PRECISION), math.ceil(maxy * __PRECISION), __MAX_BBOX_Y * __PRECISION)
        ]


@cachetools.func.ttl_cache(maxsize=128, ttl=600)
def fetch_from_emuia(bbox):
    resp = requests.get("http://emuia1.gugik.gov.pl/wmsproxy/emuia/wms",
                        params={
                            "FORMAT": "application/vnd.google-earth.kml+xml",
                            "VERSION": "1.1.1",
                            "SERVICE": "WMS",
                            "REQUEST": "GetMap",
                            # "LAYERS": "emuia:layer_adresy_labels",
                            "LAYERS": "emuia:layer_miejscowosci_granica",
                            "STYLES": "",
                            # "SRS": "EPSG:2180",
                            "SRS": "EPSG:4326",
                            "WIDTH": "16000",
                            "HEIGHT": "16000",
                            "BBOX": "{0},{1},{2},{3}".format(*bbox)
                        },
                        verify=False)
    return kml_to_shapely(resp.text)


def get_borders(terc: str):
    adm_bound = get_adm_border(terc)
    borders = []
    for bbox in divide_bbox(adm_bound.bounds):  # area we need to fetch from EMUiA
        borders.extend(fetch_from_emuia(bbox))
    return process(adm_bound, borders)


def process(adm_bound, borders):
    adm_bound = adm_bound.buffer(0.001)  # ~ 100m along meridian
    borders = [x for x in borders if
               x.border.within(adm_bound) and (x.tags.get('DO') is None or int(x.tags.get('DO')) > time.time() * 1000)]
    id_ = itertools.count(-1, -1)
    out_xml = ET.Element("osm", {'generator': 'osm-borders', 'version': '0.6'})
    for border in borders:
        dump_relation(out_xml, border, id_)
    return ET.tostring(out_xml, encoding='utf-8')


def create_MLS(obj1, obj2):
    geoms = []

    def to_list(obj):
        if not obj.is_empty and isinstance(obj, shapely.geometry.base.BaseMultipartGeometry):
            obj = shapely.ops.linemerge(obj)
            if isinstance(obj, shapely.geometry.base.BaseMultipartGeometry):
                return [x for x in obj.geoms]
        if obj.is_empty:
            return []
        return [obj, ]

    geoms.extend(to_list(obj1))
    geoms.extend(to_list(obj2))
    return shapely.geometry.MultiLineString(geoms)


def split_by_common_ways(borders: typing.List[Feature]) -> typing.List[Feature]:
    for border in borders:
        for other in borders:
            if border == other:
                continue
            intersec = border.border.intersection(other.border)

            border.border = create_MLS(intersec, border.border.difference(intersec))
            other.border = create_MLS(intersec, other.border.difference(intersec))
    return borders


def dump_relation(tree, border: Feature, id_):
    rel = ET.SubElement(tree, "relation", {'id': str(next(id_))})
    (outer, inner) = dump_ways(tree, border, id_)
    for key, value in border.tags.items():
        ET.SubElement(rel, "tag", {'k': key, 'v': value})

    for way in outer:
        ET.SubElement(rel, 'member', {'ref': str(way), 'role': 'outer', 'type': 'way'})

    for way in inner:
        ET.SubElement(rel, 'member', {'ref': str(way), 'role': 'inner', 'type': 'way'})


def dump_ways(tree, border: Feature, id_) -> typing.Tuple[typing.List[int], typing.List[int]]:
    outer = []
    inner = []
    geojson = shapely.geometry.mapping(border.border)

    def algo(way):
        nodes = dump_points(tree, way, id_)
        current_id = next(id_)
        way = ET.SubElement(tree, "way", {'id': str(current_id)})
        for node in nodes:
            ET.SubElement(way, "nd", {'ref': str(node)})
        return current_id

    if geojson['type'] == 'Polygon':
        coords = geojson['coordinates']
        outer.append(algo(coords[0]))
        if len(coords) > 1:
            for way in coords[1:]:
                inner.append(algo(way))
    if geojson['type'] == 'MultiPolygon':
        for polygon in geojson['coordinates']:
            outer.append(algo(polygon[0]))
            if len(polygon) > 1:
                for way in polygon[1:]:
                    inner.append(algo(way))
    return outer, inner


def dump_points(tree, points: list, id_) -> typing.List[int]:
    rv = []
    for point in points:
        current_id = next(id_)
        rv.append(current_id)
        ET.SubElement(tree, "node", {'id': str(current_id), 'lon': str(point[0]), 'lat': str(point[1])})
    return rv
