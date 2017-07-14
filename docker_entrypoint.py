#!/usr/bin/env python3

import argparse
import logging
import os
from xml.sax.saxutils import quoteattr

import borders.borders
from converters import teryt


def get_all_borders(terc):
    return borders.borders.get_borders(terc)


def get_nosplit_borders(terc):
    return borders.borders.get_borders(terc, borders_mapping=lambda x: x, do_clean_borders=False)


def get_lvl8_borders(terc):
    return borders.borders.get_borders(terc, lambda x: x.tags.get('admin_level') == "8")


def get_gminy(terc):
    return borders.borders.gminy_prg_as_osm(terc)


def main():
    parser = argparse.ArgumentParser(
        description="""Export admin_level=8 and admin_level=9 borders from EMUiA"""
    )
    parser.add_argument('terc', 
        nargs=1,
        help='TERC code of municipiality', 
        required=True, 
        type=int
    )
    parser.add_argument('--log-level',
        help='Set logging level (debug=10, info=20, warning=30, error=40, critical=50), default: 20',
        dest='log_level', 
        default=20, 
        type=int
    )
    parser.add_argument('--output', type=argparse.FileType('w+b'), 
        help="Output file, defaults to <terc>.osm")
    parser.add_argument('--mode', 
        help="""Mode to run:
all_borders - returns borders splited by common paths
nosplit_borders - returns one line per border
only_lvl8 - returns only admin_level=8 borders with splitting
prg - returns admin_level=7 borders from PRG (use 2 or 4 digits code for prg)""",
        choices=['all_borders', 'nosplit_borders', 'only_lvl8', 'prg'],
        default='all_borders'
    )
    
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    teryt_entry = teryt.teryt[args.terc]
    print("Working with {0} {1}".format(teryt_entry.rodz, teryt_entry.nazwa))

    if args.mode == 'all_borders':
        data = get_all_borders(args.terc)
    elif args.mode == 'nosplit_borders':
        data = get_nosplit_borders(args.terc)
    elif args.mode == 'only_lvl8':
        data = get_lvl8_borders(args.terc)
    elif args.mode == 'prg':
        data = get_gminy(args.terc)
    else:
        raise ValueError("Unkown mode: {0}".format(args.mode))

    with args.output if args.output else open("%s.osm" % (args.terc), "w+b") as output:
        output.write(data)
        print("Wrote {0} bytes to {1}".format(len(data), output.name))

if __name__ == '__main__':
    main()
