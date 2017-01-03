import collections
import functools
import io
import logging
import os
import shelve
import pickle
import tempfile
import threading
import time
import typing
import zipfile
from urllib.request import urlopen
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

__log = logging.getLogger(__name__)

# structure:
# województwo:
#   - id == terc
#   - terc == terc
#   - value == nazwa
# powiat:
#   - id == terc
#   - parent == województwo.terc
#   - terc == terc
#   - value == nazwa
# gmina:
#   - id == terc
#   - parnet == powiat.terc
#   - terc == terc
#   - value == nazwa
# miejscowość:
#   - id == simc
#   - parent == gmina.terc lub miejscowość.simc
#   - simc
#   - value - nazwa
#   - wojeództwo, powiat, gmina
# ulica:
#   - id = symul
#   - sim = lista [simc]
#   - nazwa


_TERC_RODZAJ_MAPPING = {
    '1': 'gmina miejska',
    '2': 'gmina wiejska',
    '3': 'gmina miejsko-wiejska',
    '4': 'miasto w gminie miejsko-wiejskiej',
    '5': 'obszar wiejski w gminie miejsko-wiejskiej',
    '8': 'dzielnica m.st. Warszawa',
    '9': 'delegatury w gminach miejskich',
}

_ULIC_CECHA_MAPPING = {
    'UL.': 'Ulica',
    'AL.': 'Aleja',
    'PL.': 'Plac',
    'SKWER': 'Skwer',
    'BULW.': 'Bulwar',
    'RONDO': 'Rondo',
    'PARK': 'Park',
    'RYNEK': 'Rynek',
    'SZOSA': 'Szosa',
    'DROGA': 'Droga',
    'OS.': 'Osiedle',
    'OGRÓD': 'Ogród',
    'WYSPA': 'Wyspa',
    'WYB.': 'Wybrzeże',
    'INNE': ''
}


class TercEntry(object):
    nazwa = None

    def __init__(self, dct):
        self.woj = dct.get('woj')
        self.powiat = dct.get('pow')
        self.gmi = dct.get('gmi')
        self.rodz = dct.get('rodz')
        self.rodz_nazwa = _TERC_RODZAJ_MAPPING.get(dct.get('rodz'), '') if self.rodz else \
            {2: 'województwo', 4: 'powiat'}[len(self.terc)]
        self.nazwadod = dct.get('nazwadod', '')
        self.nazwa = dct.get('nazwa')

    @property
    def terc_base(self):
        return (y for y in
                (self.woj, self.powiat) + ((self.gmi, self.rodz) if self.gmi else ())
                if y
                )

    @property
    def terc(self):
        return "".join(y for y in
                       (self.woj, self.powiat) + ((self.gmi, self.rodz) if self.gmi else ())
                       if y
                       )

    @property
    def parent_terc(self):
        if self.gmi:
            return "".join((self.woj, self.powiat))
        if self.powiat:
            return "".join((self.woj,))
        return ""

    @property
    def json(self):
        return (
            "add", {
                "doc": {
                    'id': "terc_" + self.terc,
                    'parent': ("terc_" + self.parent_terc) if self.parent_terc else '',
                    'terc': self.terc,
                    'rodzaj': self.rodz_nazwa,
                    'value': (self.nazwadod + ' ' + self.nazwa).strip(),
                    'typ': 'terc',
                },
                'boost': 7 - len(self.parent_terc)
            }
        )


class SimcEntry(object):
    terc = None
    nazwa = None

    def __init__(self, dct: dict):
        self.terc = dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')
        self.rm = wmrodz[dct.get('rm')]
        self.nazwa = dct.get('nazwa')
        self.sym = dct.get('sym')
        self.parent = None
        if dct.get('sym') != dct.get('sympod'):
            self.parent = dct.get('sympod')

    @property
    def json(self):
        return (
            "add", {
                "doc": {
                    'id': 'simc_' + self.sym,
                    'parent': ('simc_' + self.parent) if self.parent else ('terc_' + self.terc),
                    # 'terc': self.terc,
                    'rodzaj': self.rm,
                    'value': self.nazwa,
                    'simc': self.sym,
                    'wojewodztwo': self.woj,
                    'powiat': self.powiat,
                    'gmina': self.gmi,
                    'typ': 'simc',
                }
            }
        )

    @property
    def gmi(self):
        return teryt[self.terc].nazwa

    @property
    def woj(self):
        return teryt[self.terc[:2]].nazwa

    @property
    def powiat(self):
        return teryt[self.terc[:4]].nazwa


class BasicEntry(object):
    def __init__(self, dct):
        for i in dct.keys():
            setattr(self, i, dct[i])


def _clean_street_name(cecha: str, nazwa1: str, nazwa2: str):
    def mapper(name: str):
        if name.casefold().startswith(cecha.casefold()):
            return name[len(cecha):].strip()
        elif name.casefold().startswith(_ULIC_CECHA_MAPPING[cecha.upper()].casefold()):
            return name[len(_ULIC_CECHA_MAPPING.get(cecha.upper())):].strip()
        return name

    nazwa1 = mapper(nazwa1)
    nazwa2 = mapper(nazwa2)
    return " ".join((x for x in (_ULIC_CECHA_MAPPING.get(cecha.upper()), nazwa1, nazwa2) if x))


class UlicEntry(object):
    def __init__(self, dct: dict):
        self.sym = dct.get('sym')
        self.symul = dct.get('sym_ul')
        self.cecha = _ULIC_CECHA_MAPPING[dct.get('cecha').upper()]
        self.nazwa = _clean_street_name(dct.get('cecha', ''), dct.get('nazwa_2', ''), dct.get('nazwa_1', ''))
        assert self.terc == dct.get('woj') + dct.get('pow') + dct.get('gmi') + dct.get('rodz_gmi')

    @property
    def json(self):
        return (
            "add", {
                "doc": {
                    'id': 'ulic:' + self.sym + self.symul,
                    'parent': 'simc_' + self.sym,
                    'terc': self.terc,
                    'value': self.nazwa,
                    'cecha': self.cecha,
                    'symul': self.symul,
                    'wojewodztwo': self.woj,
                    'powiat': self.powiat,
                    'gmina': self.gmi,
                    'miejscowosc': self.miejscowosc
                }
            }
        )

    @property
    def woj(self):
        return teryt[self.terc[:2]].nazwa

    @property
    def powiat(self):
        return teryt[self.terc[:4]].nazwa

    @property
    def gmi(self):
        return teryt[self.terc].nazwa

    @property
    def miejscowosc(self):
        return simc[self.sym].nazwa

    @property
    def terc(self):
        return simc[self.sym].terc


class UlicMultiEntry(object):
    __log = logging.getLogger(__name__)

    def __init__(self, ulic: UlicEntry):
        self.symul = ulic.symul
        self.cecha = ulic.cecha
        self.nazwa = ulic.nazwa
        self.entries = {ulic.sym: ulic}

    def __getitem__(self, item: str):
        return self.entries[item]

    def add_entry(self, value: UlicEntry):
        if self.symul != value.symul:
            raise ValueError("Symul {0} different than expected {1}".format(value.symul, self.symul))

        if self.cecha != value.cecha:
            self.__log.info("Different CECHA {0} for street {1} in {2} [TERYT: {3}] than expected {4}".format(
                value.cecha, self.nazwa, value.miejscowosc, value.terc, self.cecha))

        if self.nazwa != value.nazwa:
            self.__log.info("Different NAZWA {0} for street {1} in {2} [TERYT: {3}] than expected {4}".format(
                value.nazwa, self.symul, value.miejscowosc, value.terc, self.nazwa
            ))

        self.entries[value.sym] = value

    @staticmethod
    def from_list(lst: typing.List[UlicEntry]):
        if len(lst) < 1:
            raise ValueError("At least one entry is needed")
        rv = UlicMultiEntry(lst[0])
        if len(lst) > 1:
            for entry in lst:
                rv.add_entry(entry)
        return rv


TYPE_DICT_ENTRIES = typing.TypeVar('V', TercEntry, BasicEntry, SimcEntry, UlicMultiEntry)


class Dictionary(typing.Generic[TYPE_DICT_ENTRIES]):
    def __init__(self, dct: typing.Dict[str, TYPE_DICT_ENTRIES]):
        self.dct = dct

    def __getitem__(self, item: str) -> TYPE_DICT_ENTRIES:
        return self.dct[item]

    def get(self, item) -> TYPE_DICT_ENTRIES:
        try:
            return self[item]
        except KeyError:
            # noinspection PyTypeChecker
            return None


class DictonarySentinel(typing.Generic[TYPE_DICT_ENTRIES]):
    def __init__(self, name, clz: TYPE_DICT_ENTRIES):
        self.name = name

    def __getitem__(self, item) -> TYPE_DICT_ENTRIES:
        _init()
        return globals()[self.name][item]

    def get(self, item) -> TYPE_DICT_ENTRIES:
        try:
            return self[item]
        except KeyError:
            # noinspection PyTypeChecker
            return None


def _zip_read(url: str, fname: str):
    dictionary_zip = zipfile.ZipFile(io.BytesIO(urlopen(url).read()))
    return dictionary_zip.read(fname)


def _row_as_dict(elem: ET.Element):
    return dict(
        (x.get('name').lower(), x.text.strip()) for x in elem.iter('col') if x.text
    )


def _groupby(lst: typing.Iterable, keyfunc=lambda x: x, valuefunc=lambda x: x):
    rv = collections.defaultdict(list)
    for i in lst:
        rv[keyfunc(i)].append(valuefunc(i))
    return rv


def _stored_dict(filename: str,
                 fetcher: typing.Callable[[], typing.Dict[str, TYPE_DICT_ENTRIES]]) -> Dictionary:
    try:
        with open(filename, "rb") as f:
            data = pickle.load(f)
    except IOError:
        __log.debug("Can't read a file: %s, starting with a new one", filename, exc_info=True)
        data = {
            'time': 0
        }
    if data['time'] < time.time() - 180 * 24 * 60 * 60:
        new = fetcher()
        data['time'] = time.time()
        with shelve.open(filename +'.shlv', flag='n') as dct:
            dct.update(new)
        with open(filename, 'wb') as f:
            pickle.dump(data, f)

    shlv = shelve.open(filename +'.shlv', flag='r')
    return Dictionary(shlv)


@functools.lru_cache()
def files():
    soup = BeautifulSoup(urlopen("http://www.stat.gov.pl/broker/access/prefile/listPreFiles.jspa"), "html.parser")

    return dict(
        (
            x + '.xml',
            'http://www.stat.gov.pl/broker/access/prefile/' +
            soup.find('td', text=x).parent.find_all('a')[1]['href']
        ) for x in ('ULIC', 'TERC', 'SIMC', 'WMRODZ')
    )


def _init():
    global __is_initialized, __init_lock, teryt, wmrodz, simc, ulic
    if not __is_initialized:
        with __init_lock:
            if not __is_initialized:
                def get_dict(name: str, cls: typing.ClassVar) -> typing.Iterable[TYPE_DICT_ENTRIES]:
                    tree = ET.XML(_zip_read(files()[name], name))
                    return (cls(_row_as_dict(x)) for x in tree.find('catalog').iter('row'))

                teryt = _stored_dict(__DB_TERYT, lambda: dict(((x.terc, x) for x in get_dict('TERC.xml', TercEntry))))
                wmrodz = _stored_dict(__DB_WMRODZ,
                                      lambda: dict((x.rm, x.nazwa_rm) for x in get_dict('WMRODZ.xml', BasicEntry)))
                simc = _stored_dict(__DB_SIMC, lambda: dict((x.sym, x) for x in get_dict('SIMC.xml', SimcEntry)))

                def create_ulic():
                    grouped = _groupby(get_dict('ULIC.xml', UlicEntry), lambda x: x.symul)
                    return dict((key, UlicMultiEntry.from_list(value)) for key, value in grouped.items())

                ulic = _stored_dict(__DB_ULIC, create_ulic)
                __is_initialized = True


__DB_TERYT = os.path.join(tempfile.gettempdir(), 'osm_borders_teryt_v1.db')
__DB_WMRODZ = os.path.join(tempfile.gettempdir(), 'osm_borders_wmrodz_v1.db')
__DB_SIMC = os.path.join(tempfile.gettempdir(), 'osm_borders_simc_v1.db')
__DB_ULIC = os.path.join(tempfile.gettempdir(), 'osm_borders_ulic_v1.db')
__init_lock = threading.Lock()
__is_initialized = False

teryt = DictonarySentinel('teryt', TercEntry)
wmrodz = DictonarySentinel('wmrodz', BasicEntry)
simc = DictonarySentinel('simc', SimcEntry)
ulic = DictonarySentinel('ulic', UlicMultiEntry)
