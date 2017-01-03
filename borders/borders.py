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

from borders.geoutils import split_by_common_ways
from converters.feature import ImmutableFeature, Feature
from converters.kmlshapely import kml_to_shapely
from converters.overpyshapely import OverToShape
from converters.teryt import simc as SIMC_DICT

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


TYPE_BBOX = typing.Tuple[float, float, float, float]


def divide_bbox(bbox: TYPE_BBOX) -> typing.List[TYPE_BBOX]:
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
def fetch_from_emuia_cached(bbox: TYPE_BBOX) -> str:
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


def fetch_from_emuia(bbox: TYPE_BBOX) -> typing.List[Feature]:
    return kml_to_shapely(fetch_from_emuia_cached(bbox))


def get_borders(terc: str,
                filter:  typing.Callable[[Feature, ], bool] = lambda x: True,
                borders_mapping: typing.Callable[[typing.List[Feature], ], typing.List[Feature]] = split_by_common_ways) -> bytes:
    adm_bound = get_adm_border(terc).geometry
    borders = []
    for bbox in divide_bbox(adm_bound.bounds):  # area we need to fetch from EMUiA
        borders.extend(fetch_from_emuia(bbox))
    return process(adm_bound = adm_bound,
                   borders = borders,
                   filter = filter,
                   borders_mapping = borders_mapping)


def clean_borders(borders: typing.List[Feature]) -> None:
    for border in borders:
        simc_code = border.tags.get('TERYT_MIEJSCOWOSCI')
        parent_id = border.tags.get('IDENTYFIKATOR_NADRZEDNEJ')
        emuia_level = 10 if parent_id else 8

        simc_entry = SIMC_DICT.get(simc_code)
        if not simc_entry:
            __log.error(
                "No entry in TERYT dictionary for SIMC: {0}, name: {1}".format(simc_code, border.tags.get('NAZWA')))
            border.tags['admin_level'] = 'TODO'
            border.tags['fixme'] = "No entry in TERYT for this teryt:simc. EMUiA admin_level={0}".format(emuia_level)
            continue
        simc_level = 10 if simc_entry.parent else 8

        fixme = []
        level = simc_level

        if emuia_level == 10 and simc_level == 10:
            # verify that they have the same parent
            try:
                parent_border = [x for x in borders if x.tags.get('IDENTYFIKATOR_MIEJSCOWOSCI') == parent_id][0]
                if simc_entry.parent != parent_border.tags.get('TERYT_MIEJSCOWOSCI'):
                    fixme.append("Different parents. In EMUiA it is teryt:simc: {0}, name: {1}".format(
                        simc_entry.parent,
                        SIMC_DICT[simc_entry.parent].nazwa))
            except IndexError:
                fixme.append("Missing parent border: {0}".format(parent_id))

        if emuia_level == 10 and simc_level == 8:
            # raise the border level to admin_level 8
            parent_border = [x for x in borders if x.tags.get('IDENTYFIKATOR_MIEJSCOWOSCI') == parent_id][0]
            new_geo = parent_border.geometry.difference(border.geometry)
            if not new_geo.is_empty:
                parent_border.geometry = new_geo
            fixme.append("EMUiA points teryt:terc {0}, name: {1} as parent. In TERC this is standalone".format(
                parent_border.tags.get('TERYT_MIEJSCOWOSCI'),
                parent_border.tags.get('NAZWA')
            ))

        if emuia_level == 8 and simc_level == 10:
            fixme.append("TERC points this as part of teryt:terc={0}, name={1}".format(
                simc_entry.parent,
                SIMC_DICT[simc_entry.parent].nazwa
            ))
            level = emuia_level
            try:
                parent_border = [x for x in borders if x.tags.get('TERYT_MIEJSCOWOSCI') == simc_entry.parent][0]
                parent_border.geometry = parent_border.geometry.union(border.geometry)
                level = simc_level
            except IndexError:
                fixme.append('Missing parent border: {0}'.format(simc_entry.parent))

        border.tags['admin_level'] = str(level)
        if fixme:
            border.tags['fixme'] = ", ".join(fixme)


def process(adm_bound: shapely.geometry.base.BaseGeometry,
            borders: typing.List[Feature],
            filter: typing.Callable[[Feature, ], bool] = lambda x: True,
            borders_mapping: typing.Callable[[typing.List[Feature], ],
                                             typing.List[Feature]] = split_by_common_ways) -> bytes:
    """

    :param adm_bound: shape of the area that one should work on
    :param borders: list of features to process
    :param filter: output filtering function
    :param borders_mapping: function that converts all the features
    :return:
    """
    adm_bound = adm_bound.buffer(0.005)  # ~ 500m along meridian

    def valid_border(x):
        rv = x.geometry.intersects(adm_bound) and (
        x.tags.get('DO') is None or int(x.tags.get('DO')) > time.time() * 1000)
        if not rv:
            msg = ", ".join("{0}: {1}".format(key, x.tags[key]) for key in sorted(x.tags.keys()))
            if x.geometry.within(adm_bound):
                __log.debug("Removing border as it is outside working set: {0}".format(msg))
            else:
                __log.debug("Removing outdated border: {0}".format(msg))
        return rv

    __log.debug("Names before dedup: {0}".format(len(borders)))
    borders = [im.to_feature() for im in set(ImmutableFeature(x) for x in borders if valid_border(x))]
    __log.debug("Names after dedup: {0}".format(len(borders)))

    clean_borders(borders)

    for border in borders:
        border.geometry = border.geometry.boundary  # use LineStrings instead of Polygons

    def tag_mapping(obj_type: str, tags: typing.Dict[str, str]) -> typing.Generator[typing.Tuple[str, str], None, None]:
        if obj_type == "relation":
            yield ('boundary', 'administrative')
            yield ('type', 'boundary')
            yield ('source:generator', 'osm-borders.py')
            yield ('admin_level', tags['admin_level'])
            yield ('name', tags['NAZWA'])
            yield ('teryt:simc', tags['TERYT_MIEJSCOWOSCI'])
            yield ('name:prefix', tags['RODZAJ'].lower())
            if 'fixme' in tags:
                yield ('fixme', tags['fixme'])
        elif obj_type == "way":
            yield ('source:geometry', tags['ZRODLO_GEOMETRII'])
        elif obj_type == "node":
            pass
        else:
            raise ValueError("Unknown object type: {0}".format(obj_type))

    converter = FeatureToOsm(borders = borders,
                             tag_mapping= tag_mapping,
                             filter = lambda x: x.geometry.within(adm_bound) and filter(x),
                             borders_mapping = borders_mapping )
    return converter.tostring()


class FeatureToOsm:
    __log = logging.getLogger(__name__)

    def __init__(self,
                 borders: typing.List[Feature],
                 tag_mapping: typing.Callable[[str, typing.Dict[str, str]], typing.Generator] = lambda x, y: y,
                 filter: typing.Callable[[Feature], bool] = lambda x: True,
                 borders_mapping: typing.Callable[[typing.List[Feature], ], typing.List[Feature]] = split_by_common_ways
                 ):
        self.__object_store = {
            'way': {},
            'point': {},
            'relation': {}
        }
        self.id_ = itertools.count(-1, -1)
        self.borders = borders
        self.tag_mapping = tag_mapping
        self.filter = filter
        self.borders_mapping = borders_mapping

    def tostring(self) -> bytes:
        out_xml = ET.Element("osm", {'generator': 'osm-borders', 'version': '0.6'})
        for border in self.borders_mapping(self.borders):
            if self.filter(border):
                self.dump_relation(out_xml, border)
            else:
                self.__log.debug("Filter excluded border: {0}".format(border))
        return ET.tostring(out_xml, encoding='utf-8')

    def dump_relation(self, tree, border: Feature) -> None:
        self.__log.debug("Dumping relation: {0}".format(border))
        rel = ET.SubElement(tree, "relation", {'id': str(next(self.id_))})
        (outer, inner) = self.dump_ways(tree, border)

        # false positive
        # noinspection PyTypeChecker
        for key, value in self.tag_mapping("relation", border.tags):
            ET.SubElement(rel, "tag",
                          {'k': key,
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
            nodes = self.dump_points(tree, way, border.tags)
            current_id = next(self.id_)
            self.__object_store['way'][way] = current_id
            way = ET.SubElement(tree, "way", {'id': str(current_id)})

            # false positive
            # noinspection PyTypeChecker
            for key, value in self.tag_mapping("way", border.tags):
                ET.SubElement(way, "tag", {'k': key, 'v': value})
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

    def dump_points(self, tree, points: list, tags: dict) -> typing.List[int]:
        rv = []
        for point in points:
            cached_point = self.__object_store['point'].get(point)
            if cached_point:
                current_id = cached_point
            else:
                current_id = next(self.id_)
                self.__object_store['point'][point] = current_id
                node = ET.SubElement(tree, "node", {'id': str(current_id), 'lon': str(point[0]), 'lat': str(point[1])})
                # false positive
                # noinspection PyTypeChecker
                for key, value in self.tag_mapping("node", tags):
                    ET.SubElement(node, "tag", {'k': key, 'v': value})
            rv.append(current_id)
        return rv
