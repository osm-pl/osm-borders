import json
import logging
import unittest
from xml.etree import ElementTree as ET

import overpy
import shapely.geometry

import borders.borders
import converters.feature
from converters.kmlshapely import kml_to_shapely
from converters.overpyshapely import OverToShape

logging.basicConfig(level=10)


class OverpyShapely(unittest.TestCase):
    def test_process(self):
        # res = overpy.Overpass().query("[out:json];relation(3094349);out;>;out;")#.get_relation(3094349)
        with open("example.json") as f:
            res = overpy.Result.from_json(json.load(f))
        with open("example.kml") as f:
            obj = kml_to_shapely(f.read())
            ret = borders.borders.process(OverToShape(res).get_relation_feature().geometry, obj)
        with open("../out.osm", "wb+") as f:
            f.write(ret)

        rv = overpy.Result.from_xml(ET.fromstring(ret))
        self.assertTrue(any(len([y for y in x.members if y.role == 'outer']) > 1 for x in rv.relations))
        print(rv)

    def test_split_extra_point(self):
        # 2 boxes exactly the same with extra point along the line
        border1 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 0),
                    (0, 2),
                    (2, 2),
                    (2, 0),
                    (0, 0)
                ]
            )
        )
        border2 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 1),
                    (0, 2),
                    (2, 2),
                    (2, 0),
                    (0, 0),
                    (0, 1)
                ]
            )
        )
        rv = borders.borders.split_by_common_ways([border1, border2])
        self.assertEqual(len(rv[0].geometry.geoms), 1)

    def test_split_same_geo(self):
        # 2 boxes exactly the same
        border1 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 0),
                    (0, 2),
                    (2, 2),
                    (2, 0),
                    (0, 0)
                ]
            )
        )
        border2 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 0),
                    (0, 2),
                    (2, 2),
                    (2, 0),
                    (0, 0),
                ]
            )
        )
        rv = borders.borders.split_by_common_ways([border1, border2])
        self.assertEqual(len(rv[0].geometry.geoms), 1)

    def test_split_one_line(self):
        # two boxes - left and right
        # (0,1)---(1,1)---(2,1)
        #   |       |       |
        # (0,0)---(1,0)---(2,0)
        # 2 small one
        left = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 0),
                    (0, 1),
                    (1, 1),
                    (1, 0),
                    (0, 0)
                ]
            )
        )
        right = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (1, 1),
                    (1, 0),
                    (2, 0),
                    (2, 1),
                    (1, 1),
                ]
            )
        )
        rv = borders.borders.split_by_common_ways([left, right])
        self.assertEqual(len(rv[0].geometry.geoms), 3)
        self.assertEqual(len(rv[1].geometry.geoms), 2)
        geoms = list(rv[0].geometry.geoms)
        self.assertTrue(shapely.geometry.LineString([(1, 1), (1, 0)]) in geoms)

    def test_3_geo_line_and_outline(self):
        # three boxes:
        # (0,2)---(1,2)
        #   |       |
        # (0,1)---(1,1)
        #   |       |
        # (0,0)---(1,0)
        # 2 small one - bottom and upper, and one outline
        bottom = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (0, 0),
                    (0, 1),
                    (1, 1),
                    (1, 0),
                ]
            ))

        upper = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (0, 1),
                    (1, 1),
                    (1, 2),
                    (0, 2),

                ]
            ))

        outline = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (0, 0),
                    (0, 2),
                    (1, 2),
                    (1, 0),
                ]
            ))

        rv = borders.borders.split_by_common_ways([bottom, upper, outline])

        self.assertEqual(len(rv[0].geometry.geoms), 2)
        self.assertTrue(
            (shapely.geometry.LineString([(0, 1), (1, 1)]) in list(rv[0].geometry.geoms)) or (
                shapely.geometry.LineString([(1, 1), (0, 1)]) in list(rv[0].geometry.geoms)
            )
        )
        self.assertEqual(len(rv[1].geometry.geoms), 2)
        self.assertTrue(shapely.geometry.LineString([(0, 1), (1, 1)]) in list(rv[1].geometry.geoms))
        self.assertEqual(len(rv[2].geometry.geoms), 2)
        self.assertTrue(shapely.geometry.LineString([(1, 1), (1, 2), (0, 2), (0, 1)]) in list(rv[2].geometry.geoms))
        self.assertTrue(shapely.geometry.LineString([(1, 1), (1, 0), (0, 0), (0, 1)]) in list(rv[2].geometry.geoms))

    def test_3_geo_line_and_outline_reversed(self):
        # three boxes:
        # (0,2)---(1,2)
        #   |       |
        # (0,1)---(1,1)
        #   |       |
        # (0,0)---(1,0)
        # 2 small one - bottom and upper, and one outline
        bottom = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (0, 0),
                    (0, 1),
                    (1, 1),
                    (1, 0),
                ]
            ))

        upper = converters.feature.Feature(
            shapely.geometry.LinearRing(
                reversed([
                    (0, 1),
                    (1, 1),
                    (1, 2),
                    (0, 2),

                ])
            ))

        outline = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (0, 0),
                    (1, 0),
                    (1, 2),
                    (0, 2),
                ]
            ))

        rv = borders.borders.split_by_common_ways([bottom, upper, outline])

        self.assertEqual(len(rv[0].geometry.geoms), 2)
        self.assertTrue(
            shapely.geometry.LineString([(0, 1), (1, 1)]) in list(rv[0].geometry.geoms)
        )
        self.assertEqual(len(rv[1].geometry.geoms), 2)
        self.assertTrue(shapely.geometry.LineString([(0, 1), (1, 1)]) in list(rv[1].geometry.geoms))
        self.assertEqual(len(rv[2].geometry.geoms), 2)
        self.assertTrue(shapely.geometry.LineString([(0, 1), (0, 2), (1, 2), (1, 1)]) in list(rv[2].geometry.geoms))
        self.assertTrue(shapely.geometry.LineString([(1, 1), (1, 0), (0, 0), (0, 1)]) in list(rv[2].geometry.geoms))

    @unittest.skip("Desn't work yet")
    def test_split_extra_line(self):
        # 2 boxes exactly the same with extra point along the line
        border1 = converters.feature.Feature(
            shapely.geometry.MultiLineString([
                [
                    (0, 0),
                    (0, 2),
                    (2, 2),
                ], [
                    (2, 2),
                    (2, 0),
                    (0, 0)
                ]
            ])
        )
        border2 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 1),
                    (0, 2),
                    (2, 2),
                    (2, 0),
                    (0, 0),
                    (0, 1)
                ]
            )
        )
        rv = borders.borders.split_by_common_ways([border1, border2])
        self.assertEqual(len(rv[0].geometry.geoms), 2)

    def test_split_one_common_edge(self):
        # 2 boxes exactly the same with extra point along the line
        border1 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 0),
                    (0, 2),
                ]
            )
        )
        border2 = converters.feature.Feature(
            shapely.geometry.LineString(
                [
                    (0, 0),
                    (0, 2),
                    (2, 2),
                    (2, 0),
                    (0, 0),
                ]
            )
        )
        rv = borders.borders.split_by_common_ways([border1, border2])
        self.assertEqual(len(rv[1].geometry.geoms), 2)

    def test_lines_shared_by_3(self):
        # three boxes:
        # (0,2)---(1,2)---(2,2)
        #   |       |       |
        # (0,1)---(1,1)     |
        #   |       |       |
        # (0,0)---(1,0)----(2,0)
        # 2 small one - bottom and upper, and one outline
        bottom = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (0, 0),
                    (0, 1),
                    (1, 1),
                    (1, 0),
                ]
            ))

        upper = converters.feature.Feature(
            shapely.geometry.LinearRing(
                reversed([
                    (0, 1),
                    (1, 1),
                    (1, 2),
                    (0, 2),

                ])
            ))

        right = converters.feature.Feature(
            shapely.geometry.LinearRing(
                [
                    (1, 0),
                    (2, 0),
                    (2, 2),
                    (1, 2),
                ]
            ))

        rv = borders.borders.split_by_common_ways([bottom, upper, right])

        self.assertEqual(len(rv[0].geometry.geoms), 4)
        self.assertTrue(
            shapely.geometry.LineString([(0, 1), (1, 1)]) in list(rv[0].geometry.geoms)
        )
        self.assertTrue(
            shapely.geometry.LineString([(1, 1), (1, 0)]) in list(rv[0].geometry.geoms)
        )

        self.assertEqual(len(rv[1].geometry.geoms), 4)
        self.assertTrue(shapely.geometry.LineString([(0, 1), (1, 1)]) in list(rv[1].geometry.geoms))
        self.assertTrue(shapely.geometry.LineString([(1, 2), (1, 1)]) in list(rv[1].geometry.geoms))
        self.assertEqual(len(rv[2].geometry.geoms), 3)
        self.assertTrue(shapely.geometry.LineString([(1, 2), (1, 1)]) in list(rv[2].geometry.geoms))
        self.assertTrue(shapely.geometry.LineString([(1, 1), (1, 0)]) in list(rv[2].geometry.geoms))
