from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
from bloat_my_db.utilities import read_file, purge_generated_files, get_generated_file_path, open_file_in_browser
import json
import argparse
import logging
import sys
import webbrowser

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


def parse_args(args):
    parser = argparse.ArgumentParser(description="Bloat my Database!")
    parser.add_argument('-rebuild', help="rebuilds of the schema and analyzer JSON files", action='store_true')
    parser.add_argument('-config', help="rebuilds of the schema and analyzer JSON files", type=str, required=True)
    parser.add_argument('-purge', help="purges all generated files (schemas, analyzer, csv)", action='store_true')
    parser.add_argument('-buildSchemaOnly', help="builds ONLY the database schema JSON, open in browser tab", action='store_true')
    parser.add_argument('-buildAnalyzerOnly', help="builds ONLY the database schema JSON, open in browser tab", action='store_true')

    return parser.parse_args(args)


def setup_logging():
    log_format = "[%(asctime)s] %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format=log_format, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    args = parse_args(args)
    setup_logging()
    conn_info = json.loads(read_file(args.config))
    database = conn_info['database']

    if args.purge:
        purge_generated_files()
        logging.info("Completed generated file purge!")
        sys.exit()

    if args.buildSchemaOnly:
        builder = PgSchemaBuilder(conn_info)
        builder.build_schema(force_rebuild=args.rebuild)
        schema_path = get_generated_file_path(database, 'schemas')
        open_file_in_browser(schema_path)
        logging.info("Completed schema only build!")
        sys.exit()

    if args.buildAnalyzerOnly:
        builder = PgSchemaBuilder(conn_info)
        schema = builder.build_schema(force_rebuild=args.rebuild)
        analyzer = PgSchemaAnalyzer(schema, conn_info)
        analyzer.analyze()
        analyzer_path = get_generated_file_path(database, 'analyzers')
        open_file_in_browser(analyzer_path)
        logging.info("Completed analyzer only build!")
        sys.exit()


    builder = PgSchemaBuilder(conn_info)
    schema = builder.build_schema(force_rebuild=args.rebuild)

    analyzer = PgSchemaAnalyzer(schema, conn_info)
    analyzed_schema = analyzer.analyze()

    analyzer.display_table_insertion_order()


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    run()
