import json
import logging
import unittest

import overpy

import borders.borders
from converters.kmlshapely import kml_to_shapely
from converters.overpyshapely import OverToShape

logging.basicConfig(level=10)


class OverpyShapely(unittest.TestCase):
    def test(self):
        # res = overpy.Overpass().query("[out:json];relation(3094349);out;>;out;")#.get_relation(3094349)
        with open("example.json") as f:
            res = overpy.Result.from_json(json.load(f))
        with open("example.kml") as f:
            obj = kml_to_shapely(f.read())
            ret = borders.borders.process(OverToShape(res).get_relation_shape(), obj)
        with open("../out.osm", "wb+") as f:
            f.write(ret)

