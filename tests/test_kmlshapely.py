import logging
import unittest

import shapely.geometry

import converters.kmlshapely

logging.basicConfig(level=10)


class KmlShapelyTests(unittest.TestCase):
    def test1(self):
        with open("example.kml") as f:
            content = f.read()
            obj = converters.kmlshapely.kml_to_shapely(content)
            self.assertEqual("layer_miejscowosci_granica.1488043", obj[0].get_tag('name'))
            self.assertEqual("2005062", obj[0].get_tag('TERYT_JEDNOSTKI'))
            self.assertTrue(
                shapely.geometry.Polygon(
                    [
                        (23.480308, 52.737712),
                        (23.480308, 52.776607),
                        (23.537287, 52.776607),
                        (23.537287, 52.737712),
                        (23.480308, 52.737712)
                    ]
                ).contains(
                    obj[0].geometry
                )
            )
