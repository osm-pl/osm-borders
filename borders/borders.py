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


def clean_borders(borders: typing.List[Feature]):
    for border in borders:
        simc_code = border.tags.get('TERYT_MIEJSCOWOSCI')
        parent_id = border.tags.get('IDENTYFIKATOR_NADRZEDNEJ')
        emuia_level = 10 if parent_id else 8

        simc_entry = SIMC_DICT.get(simc_code)
        if not simc_entry:
            __log.error(
                "No entry in TERYT dictionary for SIMC: {0}, name: {1}".format(simc_code, border.tags.get('NAZWA')))
            border.tags['admin_level'] = str(emuia_level)
            border.tags['fixme'] = "No entry in TERYT for this SIMC"
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

        border.tags['admin_level'] = str(level)
        if fixme:
            border.tags['fixme'] = ", ".join(fixme)


def process(adm_bound: shapely.geometry.base.BaseGeometry, borders: typing.List[Feature]):
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

    converter = FeatureToOsm(borders, tag_mapping, lambda x: x.geometry.within(adm_bound))
    return converter.tostring()


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


class FeatureToOsm:
    __log = logging.getLogger(__name__)

    def __init__(self,
                 borders: typing.List[Feature],
                 tag_mapping: typing.Callable[[str, typing.Dict[str, str]], typing.Generator] = lambda x, y: y,
                 filter: typing.Callable[[Feature], bool] = lambda x: True
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

    def tostring(self):
        out_xml = ET.Element("osm", {'generator': 'osm-borders', 'version': '0.6'})
        for border in split_by_common_ways(self.borders):
            if self.filter(border):
                self.dump_relation(out_xml, border)
            else:
                self.__log.debug("Filter excluded border: {0}".format(border))
        return ET.tostring(out_xml, encoding='utf-8')

    def dump_relation(self, tree, border: Feature):
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
