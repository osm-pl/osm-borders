import unittest

from converters import emuia


class EmuiaTests(unittest.TestCase):
    def test_fetch(self):
        adresy = emuia.get_addresses("0601032")
        self.assertEqual(4697, len(adresy))

    def test_fetch_warszawa(self):
        adresy = emuia.get_addresses("1465011")
        self.assertEqual(113211, len(adresy))
