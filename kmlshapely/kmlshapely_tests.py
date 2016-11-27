import logging
import os
import unittest

import shapely.geometry

import kmlshapely

logging.basicConfig(level=10)

class KmlShapelyTests(unittest.TestCase):
    def test1(self):
        print(os.getcwd())
        with open("../example.kml") as f:
            obj = kmlshapely.kml_to_shapely(f)
            self.assertEqual("Krynki-Sobole (0028702)", obj[0].get_tag('name'))
            self.assertEqual("2010042", obj[0].get_tag('TERYT_JEDNOSTKI'))
            self.assertTrue(
                shapely.geometry.Polygon(
                    [
                        (22.754402, 52.515541),
                        (22.754402, 52.553393),
                        (22.804012, 52.553393),
                        (22.804012, 52.515541),
                        (22.754402, 52.515541)
                    ]
                ).contains(
                    obj[0].border
                )
            )
            # print(json.dumps(obj[0].geojson))
