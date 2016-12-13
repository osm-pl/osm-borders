import itertools
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

logging.basicConfig(level=logging.DEBUG)


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

    def test_verify_no_cache_poison(self):
        # res = overpy.Overpass().query("[out:json];relation(3094349);out;>;out;")#.get_relation(3094349)
        with open("example.json") as f:
            res = overpy.Result.from_json(json.load(f))
        with open("example.kml") as f:
            obj = kml_to_shapely(f.read())
            ret = borders.borders.process(OverToShape(res).get_relation_feature().geometry, obj)
        with open("../out.osm", "wb+") as f:
            f.write(ret)

        with open("example.kml") as f:
            obj = kml_to_shapely(f.read())
            ret2 = borders.borders.process(OverToShape(res).get_relation_feature().geometry, obj)
        self.assertEqual(ret, ret2)

    def test_no_overlapping_ways(self):
        # res = overpy.Overpass().query("[out:json];relation(3094349);out;>;out;")#.get_relation(3094349)
        with open("example.json") as f:
            adm_boundary = overpy.Result.from_json(json.load(f))
        with open("test_overlapping_ways.kml") as part1, open("2010042_part_2.kml") as part2:
            obj = kml_to_shapely(part1.read())
            # obj.extend(kml_to_shapely(part2.read()))
            self.assertEqual(len(obj), 3)
            ret = borders.borders.process(OverToShape(adm_boundary).get_relation_feature().geometry, obj)
        with open("../out.osm", "wb+") as f:
            f.write(ret)

        rv = overpy.Result.from_xml(ET.fromstring(ret))
        inner_nodes = sorted(list(x.id for x in itertools.chain(*(way.nodes[1:-1] for way in rv.get_ways()))))
        dup_nodes = [x[0] for x in itertools.groupby(inner_nodes) if len(list(x[1])) > 1]
        self.assertEqual(len(dup_nodes), 0, "Duplicate nodes found: {0}".format(len(dup_nodes)))


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

    def test(self):
        other_geo = shapely.geometry.asShape({"type": "MultiLineString", "coordinates": [
            [[22.76047092, 52.61546139], [22.76064297, 52.61612468], [22.76105567, 52.61771505],
             [22.76108073, 52.61783719], [22.76108384, 52.6178525], [22.76139736, 52.61938449],
             [22.76244925, 52.61976631], [22.76268426, 52.62022516], [22.76264229, 52.62045121],
             [22.76251974, 52.62111035], [22.76245299, 52.62147661], [22.76236904, 52.62193968],
             [22.76232238, 52.62219597], [22.76227375, 52.62235994], [22.76217722, 52.62268248],
             [22.7620684, 52.62304795], [22.76186122, 52.62317808], [22.76159148, 52.62334756],
             [22.76137031, 52.62348662], [22.76090434, 52.62373826], [22.76064532, 52.62387845],
             [22.76008432, 52.62403247], [22.75894507, 52.62430293], [22.75882211, 52.62433202],
             [22.75862406, 52.62437911], [22.75785951, 52.62461745], [22.75717809, 52.62482996],
             [22.75711238, 52.6248503], [22.75691077, 52.62491325], [22.75632337, 52.62509655],
             [22.75618569, 52.62528501], [22.75590341, 52.62573943], [22.75583175, 52.62585498],
             [22.75570535, 52.62592699], [22.75561602, 52.62597818], [22.75556603, 52.62600668]],
            [[22.75925843, 52.61034505], [22.75973145, 52.61195017], [22.75988232, 52.61253082],
             [22.759887, 52.6125484], [22.75989522, 52.61258129], [22.75990058, 52.61260076], [22.7599031, 52.61261058],
             [22.76003255, 52.6131086], [22.76005563, 52.61323251], [22.76016858, 52.61383874],
             [22.76047092, 52.61546139]],
            [[22.72576493, 52.63109363], [22.72101464, 52.62983602], [22.72103002, 52.62981515],
             [22.72131641, 52.62942593], [22.72143714, 52.62926195], [22.72146099, 52.62922914],
             [22.72151835, 52.6291512], [22.72153358, 52.62913061], [22.72158187, 52.62906507],
             [22.72164523, 52.62897893], [22.72170875, 52.6288928], [22.72179319, 52.62877794],
             [22.72187748, 52.62866309], [22.72200435, 52.62849091], [22.72203895, 52.62844392],
             [22.72207386, 52.62839648], [22.72214293, 52.62830223], [22.72231209, 52.62807253],
             [22.72236638, 52.62799878], [22.72249618, 52.62782232], [22.72256509, 52.62772842],
             [22.72262598, 52.62764603], [22.72269504, 52.62755178], [22.72274624, 52.62748267],
             [22.7227647, 52.62745735], [22.72283421, 52.6273631], [22.72290311, 52.62726911],
             [22.7229514, 52.62720348], [22.72299369, 52.62714606], [22.72303598, 52.62708863],
             [22.72309334, 52.62701079], [22.72314302, 52.62694311], [22.7231627, 52.62691636],
             [22.72322606, 52.62683031], [22.72328927, 52.62674417], [22.72331924, 52.62670387],
             [22.72336492, 52.62664163], [22.72344658, 52.62653061], [22.72359913, 52.62632329],
             [22.72363373, 52.6262763], [22.72409245, 52.62565282], [22.72410383, 52.6256373],
             [22.72414012, 52.62558799], [22.72434033, 52.62531584], [22.72447826, 52.62512841],
             [22.72454039, 52.62504387], [22.72455761, 52.62502051], [22.72474735, 52.6247629],
             [22.72496523, 52.62446712], [22.72514281, 52.62422637], [22.7253035, 52.62400781],
             [22.72543926, 52.62382367], [22.72588237, 52.62322213], [22.72609532, 52.62293259],
             [22.72621631, 52.62276861], [22.72656334, 52.62231181], [22.72684035, 52.6219471],
             [22.72738528, 52.62122972], [22.72754728, 52.6210163], [22.72864445, 52.61957198],
             [22.72954448, 52.61972167], [22.73064326, 52.61998447], [22.73215682, 52.61806204],
             [22.73222718, 52.61797264], [22.73225203, 52.61794073], [22.73225616, 52.61793548],
             [22.73228102, 52.61790303], [22.73231219, 52.61786193], [22.73216854, 52.6178097],
             [22.73100139, 52.61738494], [22.73069583, 52.61727381], [22.73010433, 52.61692513],
             [22.73009184, 52.61691772], [22.72942935, 52.61652711], [22.73197264, 52.61384718],
             [22.73255974, 52.61328409], [22.73283424, 52.61292087], [22.73285281, 52.61289635],
             [22.73337307, 52.61220798], [22.73374827, 52.61151643], [22.73388815, 52.61127903],
             [22.73393916, 52.61119959], [22.73407844, 52.61098313], [22.73437007, 52.61036771],
             [22.73404209, 52.60938185], [22.73392481, 52.60903052], [22.73350555, 52.60777064],
             [22.73349438, 52.60773799], [22.73334522, 52.60728961], [22.7344778, 52.60694347],
             [22.73465652, 52.60688957], [22.73588683, 52.6065378], [22.73633532, 52.60641002],
             [22.73673446, 52.60629906], [22.73709713, 52.6061984], [22.73718835, 52.60617303],
             [22.73801434, 52.60594205], [22.73841825, 52.60582906], [22.73854563, 52.60579402],
             [22.7392066, 52.60561285], [22.73922921, 52.60560662], [22.7394579, 52.60554443],
             [22.7396244, 52.60549911], [22.73991781, 52.60541937], [22.74019238, 52.60529125],
             [22.74032466, 52.60522938], [22.74081219, 52.60500062], [22.74157158, 52.60465538],
             [22.74169633, 52.60459864], [22.74183738, 52.60453426], [22.74224484, 52.60434841],
             [22.74294525, 52.60403076], [22.74328812, 52.60387307], [22.74337698, 52.60383204],
             [22.74417268, 52.603558], [22.74444148, 52.60346549], [22.74468271, 52.6033823],
             [22.74622063, 52.60285475], [22.74630173, 52.60282685], [22.7467573, 52.60267097],
             [22.74727817, 52.6024925], [22.74744396, 52.60243521], [22.74799512, 52.60224483],
             [22.74895135, 52.60191457], [22.74943184, 52.6017487], [22.75034621, 52.60143596],
             [22.75157606, 52.60099685], [22.75176441, 52.60092963], [22.75249538, 52.60061261],
             [22.75332682, 52.60020919], [22.75334198, 52.60021061], [22.75328828, 52.60133782],
             [22.75327185, 52.60168232], [22.75327034, 52.60170945], [22.75327009, 52.60171825],
             [22.75325825, 52.60191703], [22.75324916, 52.60207592], [22.75324536, 52.60213682],
             [22.7532399, 52.60223042], [22.75323901, 52.60224613], [22.75323021, 52.60240018],
             [22.75322605, 52.60246853], [22.75318684, 52.60313716], [22.75322709, 52.60332076],
             [22.75329186, 52.60361839], [22.75334057, 52.60384234], [22.75334946, 52.60388252],
             [22.75336759, 52.60396665], [22.75340364, 52.60413196], [22.75342423, 52.60434284],
             [22.753432, 52.60442265], [22.75347485, 52.60486322], [22.75348926, 52.60501068],
             [22.75352326, 52.60535761], [22.75355167, 52.60565144], [22.75356001, 52.60573727],
             [22.75356608, 52.60579908], [22.75358222, 52.60595357], [22.75359428, 52.60606955],
             [22.75361218, 52.606245], [22.75362136, 52.60633237], [22.75363411, 52.60645527],
             [22.75364685, 52.60657854], [22.75365623, 52.60666914], [22.75368496, 52.6069468],
             [22.75368984, 52.6069935], [22.7538572, 52.60712028], [22.75395244, 52.60719238],
             [22.75404783, 52.60726456], [22.75428327, 52.60744284], [22.75436384, 52.60751163],
             [22.75469882, 52.607798], [22.75504994, 52.60809828], [22.75533289, 52.60834033],
             [22.75538939, 52.60838864], [22.7555053, 52.60846104], [22.75570274, 52.60858427],
             [22.75597954, 52.60875705], [22.75617683, 52.60888027], [22.75702336, 52.60927752],
             [22.75726962, 52.60933502], [22.75737743, 52.60936014], [22.75832595, 52.60958184],
             [22.75876948, 52.60968544], [22.75887259, 52.60982449], [22.75925843, 52.61034505]],
            [[22.75556603, 52.62600668], [22.75546505, 52.62594253], [22.75534349, 52.62586486],
             [22.75524599, 52.62580281], [22.75517374, 52.62575693], [22.7551382, 52.62573418],
             [22.75507842, 52.62569643], [22.75501458, 52.6256563], [22.75493013, 52.62560293],
             [22.75480492, 52.62552396], [22.75452299, 52.62534627], [22.75427546, 52.62519016],
             [22.75420505, 52.62513145], [22.75384206, 52.62482852], [22.75331633, 52.62474354],
             [22.75261638, 52.62463048], [22.75240306, 52.62459597], [22.75214784, 52.62455481],
             [22.75179838, 52.62449828], [22.75159982, 52.62446932], [22.75129655, 52.62442496],
             [22.75100415, 52.62438224], [22.75066308, 52.62433236], [22.75044217, 52.6243001],
             [22.75023214, 52.62426939], [22.74999976, 52.62423539], [22.7498048, 52.62420979],
             [22.74969239, 52.62419485], [22.74955996, 52.62417736], [22.74955246, 52.62417629],
             [22.74954775, 52.62417571], [22.74945093, 52.62416309], [22.74925701, 52.6241374],
             [22.74908427, 52.62411464], [22.74898157, 52.62410097], [22.74887254, 52.62408661],
             [22.74875395, 52.62407089], [22.74862771, 52.62405418], [22.74857901, 52.62404783],
             [22.74851263, 52.62403445], [22.7484138, 52.62401472], [22.74840543, 52.6240131],
             [22.74824272, 52.62398064], [22.74821966, 52.62397608], [22.74821129, 52.62397438],
             [22.74803375, 52.62393889], [22.74781744, 52.62389553], [22.74743753, 52.62381961],
             [22.74724384, 52.62378089], [22.74705572, 52.62374331], [22.74688641, 52.62378682],
             [22.74674998, 52.62382178], [22.74660848, 52.62385821], [22.74645628, 52.62389722],
             [22.74628519, 52.62394116], [22.74603256, 52.62400589], [22.74579436, 52.62406709],
             [22.74574883, 52.62407353], [22.74550013, 52.62410936], [22.74527353, 52.62414201],
             [22.74494979, 52.62418856], [22.74481706, 52.62420755], [22.74471533, 52.6242222],
             [22.74441309, 52.62426565], [22.74436692, 52.62427414], [22.74412893, 52.62431737],
             [22.74392198, 52.6243549], [22.74367493, 52.62439982], [22.74355141, 52.62442224],
             [22.74340087, 52.62444959], [22.74328685, 52.6244704], [22.74323999, 52.62448194],
             [22.74309437, 52.62451788], [22.74299382, 52.62454269], [22.74290249, 52.62456545],
             [22.74279539, 52.62459191], [22.74260708, 52.62463853], [22.74246562, 52.62467343],
             [22.74237682, 52.6246955], [22.74228355, 52.6247185], [22.74214849, 52.62475185],
             [22.74207084, 52.62477116], [22.74204705, 52.62477702], [22.74201596, 52.62478478],
             [22.74190188, 52.62484324], [22.74182049, 52.62488516], [22.7408833, 52.62541779],
             [22.74026866, 52.6256674], [22.74005422, 52.62575437], [22.73975549, 52.62587567],
             [22.73935756, 52.62588743], [22.7390016, 52.62589794], [22.73864624, 52.62590844],
             [22.73828393, 52.62591905], [22.73764534, 52.62593796], [22.73755044, 52.62594063],
             [22.73634886, 52.62585079], [22.73587519, 52.62581545], [22.73580032, 52.62580989],
             [22.73564099, 52.62581367], [22.73496748, 52.62582958], [22.73490748, 52.62583101],
             [22.7348992, 52.62583128], [22.7342551, 52.6258466], [22.73410269, 52.62586645],
             [22.73375252, 52.62591214], [22.73340115, 52.62595799], [22.73294527, 52.62601747],
             [22.73291989, 52.62602151], [22.73255074, 52.62607994], [22.73194893, 52.62617515],
             [22.73120796, 52.62654685], [22.73106318, 52.62661954], [22.7305061, 52.62689902],
             [22.72937027, 52.62775622], [22.72826782, 52.62806175], [22.72807097, 52.62811625],
             [22.726655, 52.62834299], [22.72609278, 52.62960115], [22.72583574, 52.63111227],
             [22.72576493, 52.63109363]]]})
        intersec = shapely.geometry.asShape({"type": "LineString",
                                             "coordinates": [[22.75925843, 52.61034505], [22.75973145, 52.61195017],
                                                             [22.75988232, 52.61253082], [22.759887, 52.6125484],
                                                             [22.75989522, 52.61258129], [22.75990058, 52.61260076],
                                                             [22.7599031, 52.61261058], [22.76003255, 52.6131086],
                                                             [22.76005563, 52.61323251], [22.76016858, 52.61383874],
                                                             [22.76047092, 52.61546139], [22.76064297, 52.61612468],
                                                             [22.76105567, 52.61771505], [22.76108073, 52.61783719],
                                                             [22.76108384, 52.6178525], [22.76139736, 52.61938449],
                                                             [22.76244925, 52.61976631], [22.76268426, 52.62022516],
                                                             [22.76264229, 52.62045121], [22.76251974, 52.62111035],
                                                             [22.76245299, 52.62147661], [22.76236904, 52.62193968],
                                                             [22.76232238, 52.62219597], [22.76227375, 52.62235994],
                                                             [22.76217722, 52.62268248], [22.7620684, 52.62304795],
                                                             [22.76186122, 52.62317808], [22.76159148, 52.62334756],
                                                             [22.76137031, 52.62348662], [22.76090434, 52.62373826],
                                                             [22.76064532, 52.62387845], [22.76008432, 52.62403247],
                                                             [22.75894507, 52.62430293], [22.75882211, 52.62433202],
                                                             [22.75862406, 52.62437911], [22.75785951, 52.62461745],
                                                             [22.75717809, 52.62482996], [22.75711238, 52.6248503],
                                                             [22.75691077, 52.62491325], [22.75632337, 52.62509655],
                                                             [22.75618569, 52.62528501], [22.75590341, 52.62573943],
                                                             [22.75583175, 52.62585498], [22.75570535, 52.62592699],
                                                             [22.75561602, 52.62597818], [22.75556603, 52.62600668]]})

        self.assertTrue(len(other_geo) == len(other_geo.difference(shapely.geometry.LineString())))
        rv = borders.borders.create_multi_string(intersec, other_geo.difference(intersec))
        for i in other_geo.geoms:
            print(json.dumps(shapely.geometry.mapping(i)))
        print("---")

        for i in rv.geoms:
            print(json.dumps(shapely.geometry.mapping(i)))
        self.assertTrue(len(other_geo) <= len(rv))
