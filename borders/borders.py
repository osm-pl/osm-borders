import itertools
import logging
import time
import typing
import xml.etree.ElementTree as ET

import cachetools.func
import math
import requests
import shapely.geometry
import shapely.ops
from overpy import Overpass

from converters.feature import ImmutableFeature
from converters.kmlshapely import Feature
from converters.kmlshapely import kml_to_shapely
from converters.overpyshapely import OverToShape

__log = logging.getLogger(__name__)


@cachetools.func.ttl_cache(maxsize=128, ttl=600)
def get_adm_border(terc: str) -> Feature:
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
    return OverToShape(result).get_relation_feature()


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


@cachetools.func.ttl_cache(maxsize=128, ttl=24 * 3600)
def fetch_from_emuia_cached(bbox):
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
    return resp.text


def fetch_from_emuia(bbox):
    return kml_to_shapely(fetch_from_emuia_cached(bbox))


def get_borders(terc: str):
    adm_bound = get_adm_border(terc).geometry
    borders = []
    for bbox in divide_bbox(adm_bound.bounds):  # area we need to fetch from EMUiA
        borders.extend(fetch_from_emuia(bbox))
    return process(adm_bound, borders)


__TAG_MAPPING = {
    "relation": {
        'NAZWA': 'name',
        'TERYT_MIEJSCOWOSCI': 'teryt:simc'},
    "way": {
        'ZRODLO_GEOMETRII': 'source:geometry',
    }
}

__DEFAULT_TAGS = {
    "relation": {
        'admin_level': 'TODO',
        'boundary': 'administrative',
        'type': 'boundary'
    }
}

def process(adm_bound: shapely.geometry.base.BaseGeometry, borders: typing.List[Feature]):
    adm_bound = adm_bound.buffer(0.001)  # ~ 100m along meridian

    def valid_border(x):
        return x.geometry.within(adm_bound) and (x.tags.get('DO') is None or int(x.tags.get('DO')) > time.time() * 1000)

    # __log.debug("Names before dedup: {0}".format(", ".join(sorted(x.tags['NAZWA'] for x in borders))))
    __log.debug("Names before dedup: {0}".format(len([x.tags['NAZWA'] for x in borders])))
    borders = [im.to_feature() for im in set(ImmutableFeature(x) for x in borders if valid_border(x))]
    # __log.debug("Names after  dedup: {0}".format(", ".join(sorted(x.tags['NAZWA'] for x in borders))))
    __log.debug("Names after dedup: {0}".format(len([x.tags['NAZWA'] for x in borders])))


    for border in borders:
        border.geometry = border.geometry.boundary  # use LineStrings instead of Polygons

    return FeatureToOsm(borders, __TAG_MAPPING, __DEFAULT_TAGS, 'emuia:').tostring()


def try_linemerge(obj):
    if not obj.is_empty \
            and isinstance(obj, shapely.geometry.base.BaseMultipartGeometry) \
            and len(obj) > 1 \
            and not isinstance(obj, shapely.geometry.MultiPoint):
        return shapely.ops.linemerge(
            shapely.geometry.MultiLineString([x for x in obj if not isinstance(x, shapely.geometry.Point)]))
    return obj


def get_raw_geometries(obj: shapely.geometry.base.BaseGeometry) -> typing.List[shapely.geometry.base.BaseGeometry]:
    if not obj.is_empty and isinstance(obj, shapely.geometry.base.BaseMultipartGeometry):
        return [x for x in obj.geoms]
    if obj.is_empty:
        return []
    return [obj, ]


def create_multi_string(obj1, obj2):
    geoms = []

    geoms.extend(get_raw_geometries(obj1))
    geoms.extend(get_raw_geometries(obj2))
    return shapely.geometry.MultiLineString(geoms)


def split_intersec(intersec, objs):
    rv = intersec
    for obj in objs:
        if not isinstance(obj, shapely.geometry.base.BaseMultipartGeometry):
            continue  # nothing to be done
        for geom in obj.geoms:
            small_intersec = try_linemerge(geom.intersection(intersec))
            if geom.intersects(intersec) and not isinstance(small_intersec,
                                                            (shapely.geometry.Point, shapely.geometry.MultiPoint)):
                rest = get_raw_geometries(rv.difference(small_intersec))
                if rest:
                    rv = shapely.geometry.MultiLineString((*get_raw_geometries(small_intersec), *rest))
    return rv


def split_by_common_ways(borders: typing.List[Feature]) -> typing.List[Feature]:
    for border in borders:
        for other in borders:
            if border == other:
                continue
            __log.debug("Processing border ({0}, {1})".format(borders.index(border), borders.index(other)))
            intersec = border.geometry.intersection(other.geometry)
            if intersec.is_empty:
                continue  # nothing will change anyway
            if isinstance(intersec, shapely.geometry.GeometryCollection):
                intersec = shapely.ops.cascaded_union(
                    [x for x in intersec.geoms if not isinstance(x, shapely.geometry.Point)])
            if isinstance(intersec, (shapely.geometry.Point, shapely.geometry.MultiPoint)):
                intersec = shapely.geometry.LineString()  # empty geometry
            intersec = try_linemerge(intersec)
            intersec = split_intersec(intersec, [border.geometry, other.geometry])
            border.geometry = create_multi_string(intersec, border.geometry.difference(intersec))
            other.geometry = create_multi_string(intersec, other.geometry.difference(intersec))
    return borders


TAG_MAPPING_TYPE = typing.Dict[str, typing.Dict[str, str]]
class FeatureToOsm:
    __log = logging.getLogger(__name__)
    __allowed_mapping_types = {"relation", "way", "node"}

    def __init__(self, borders, tag_mapping: TAG_MAPPING_TYPE = None, default_tags: TAG_MAPPING_TYPE = None,
                 tag_default_prefix: str = ""):
        self.__object_store = {
            'way': {},
            'point': {},
            'relation': {}
        }
        self.id_ = itertools.count(-1, -1)
        self.borders = borders
        if not set(tag_mapping.keys()).issubset(self.__allowed_mapping_types):
            raise ValueError("Unknown mapping for types: {0}".format(
                ", ".join(set(tag_mapping).difference(self.__allowed_mapping_types))))
        self.tag_mapping = tag_mapping if tag_mapping else {}
        if not set(default_tags.keys()).issubset(self.__allowed_mapping_types):
            raise ValueError("Unknown mapping for types: {0}".format(
                ", ".join(set(default_tags).difference(self.__allowed_mapping_types))))
        self.default_tags = default_tags if default_tags else {}
        self.tag_default_prefix = tag_default_prefix

    def tostring(self):
        out_xml = ET.Element("osm", {'generator': 'osm-borders', 'version': '0.6'})
        for border in split_by_common_ways(self.borders):
            self.dump_relation(out_xml, border)
        return ET.tostring(out_xml, encoding='utf-8')

    def dump_relation(self, tree, border: Feature):
        self.__log.debug("Dumping relation: {0}".format(border))
        rel = ET.SubElement(tree, "relation", {'id': str(next(self.id_))})
        (outer, inner) = self.dump_ways(tree, border)
        for key, value in self.default_tags.get("relation", {}).items():
            ET.SubElement(rel, "tag", {'k': key, 'v': value})

        for key, value in border.tags.items():
            ET.SubElement(rel, "tag",
                          {'k': self.tag_mapping.get("relation", {}).get(key, self.tag_default_prefix + key),
                           'v': value})

        for way in outer:
            ET.SubElement(rel, 'member', {'ref': str(way), 'role': 'outer', 'type': 'way'})

        for way in inner:
            ET.SubElement(rel, 'member', {'ref': str(way), 'role': 'inner', 'type': 'way'})

    def dump_ways(self, tree, border: Feature) -> typing.Tuple[typing.List[int], typing.List[int]]:
        outer = []
        inner = []
        geojson = shapely.geometry.mapping(border.geometry)

        def algo(way: typing.List[typing.Tuple[float, float]]):
            cached_way = self.__object_store['way'].get(way)
            if cached_way:
                return cached_way
            nodes = self.dump_points(tree, way)
            current_id = next(self.id_)
            self.__object_store['way'][way] = current_id
            way = ET.SubElement(tree, "way", {'id': str(current_id)})
            for key, value in self.default_tags.get("way", {}).items():
                ET.SubElement(way, "tag", {'k': key, 'v': value})
            for key, value in self.tag_mapping.get("way", {}).items():
                ET.SubElement(way, "tag", {'k': value, 'v': border.tags[key]})
            for node in nodes:
                ET.SubElement(way, "nd", {'ref': str(node)})
            return current_id

        if geojson['type'] == 'Polygon':
            coords = geojson['coordinates']  # use outer coordinates
            outer.append(algo(coords[0]))
            if len(coords) > 1:
                for way in coords[1:]:
                    inner.append(algo(way))
        elif geojson['type'] == 'LineString':
            coords = geojson['coordinates']
            outer.append(algo(coords))
        elif geojson['type'] == 'MultiLineString':
            outer.extend(algo(x) for x in geojson['coordinates'])

        elif geojson['type'] == 'MultiPolygon':
            for polygon in geojson['coordinates']:
                outer.append(algo(polygon[0]))
                if len(polygon) > 1:
                    for way in polygon[1:]:
                        inner.append(algo(way))
        else:
            raise ValueError("Unkown GeoJSON Type found: {0}".format(geojson['type']))
        return outer, inner

    def dump_points(self, tree, points: list) -> typing.List[int]:
        rv = []
        for point in points:
            cached_point = self.__object_store['point'].get(point)
            if cached_point:
                current_id = cached_point
            else:
                current_id = next(self.id_)
                self.__object_store['point'][point] = current_id
                node = ET.SubElement(tree, "node", {'id': str(current_id), 'lon': str(point[0]), 'lat': str(point[1])})
                for key, value in self.default_tags.get("node", {}).items():
                    ET.SubElement(node, "tag", {'k': key, 'v': value})
            rv.append(current_id)
        return rv
