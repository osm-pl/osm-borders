import argparse
import logging

from borders.borders import get_borders


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description="""Fetch administrative borders from EMUiA"""
                                     )
    parser.add_argument('--log-level',
                        help='Set logging level (debug=10, info=20, warning=30, error=40, critical=50), default: 20',
                        dest='log_level', default=20, type=int)
    parser.add_argument('--output', type=argparse.FileType('w+b'), default='result.osm',
                        help='output file with merged data (default: result.osm)')

    parser.add_argument('terc', nargs=1, help='county terc code')
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    args.output.write(get_borders(args.terc[0], filter=lambda x: x.tags.get('admin_level') == "8"))


if __name__ == '__main__':
    main()
