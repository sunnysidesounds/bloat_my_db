from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
from bloat_my_db.utilities import read_file
import json
import argparse
import logging
import sys

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


def parse_args(args):
    parser = argparse.ArgumentParser(description="Bloat my Database!")
    parser.add_argument('-rebuild', help="rebuilds of the schema and analyzer JSON files", action='store_true')
    parser.add_argument('-config', help="rebuilds of the schema and analyzer JSON files", type=str)

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    log_format = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=log_format, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    args = parse_args(args)
    setup_logging(args.loglevel)

    if args.config is None:
        logging.error(" Missing -config argument, -config=/path/to/config.json required")
        sys.exit()

    conn_info = json.loads(read_file(args.config))
    builder = PgSchemaBuilder(conn_info)
    schema = builder.build_schema(force_rebuild=args.rebuild)
    analyzer = PgSchemaAnalyzer(schema, conn_info)
    analyzed_schema = analyzer.analyze()

    analyzer.display_table_insertion_order()


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    run()
