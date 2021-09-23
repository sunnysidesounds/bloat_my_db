import json
import argparse
import logging
import sys
from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
from bloat_my_db.data_bloaters.pg_data_bloater import PgDataBloater
from bloat_my_db.utilities import read_file, purge_generated_files, get_generated_file_path, open_file_in_browser

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Bloat my Database!")


def parse_args(args):
    parser.add_argument('-build', help="builds the schema and analyzer JSON files", action='store_true')
    parser.add_argument('-config', help="required configuration file", type=str, required=True)
    parser.add_argument('-rows', help="How may rows do you want to populate, default is 25", type=int)
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
    print("------------------------------------------------------------------")
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
        builder.build_schema(force_rebuild=True)
        schema_path = get_generated_file_path(database, 'schemas')
        open_file_in_browser(schema_path)
        print("- Completed schema only build for {database}!".format(database=database))
        sys.exit()

    if args.buildAnalyzerOnly:
        builder = PgSchemaBuilder(conn_info)
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, conn_info)
        analyzer.build_analyzer_schema(force_rebuild=True)
        analyzer_path = get_generated_file_path(database, 'analyzers')
        open_file_in_browser(analyzer_path)
        print("- Completed analyzer only build for {database}!".format(database=database))
        sys.exit()

    if args.build:
        default_rows_to_generate = 25
        builder = PgSchemaBuilder(conn_info)
        schema = builder.build_schema(force_rebuild=False) # Change this
        analyzer = PgSchemaAnalyzer(schema, conn_info)
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=False)
        if analyzed_schema:
            print("- Part 1: completed built & analyzed for {database} database!".format(database=database))
            bloater = PgDataBloater(analyzed_schema, conn_info)
            rows_to_create = args.rows if args.rows else default_rows_to_generate
            bloater.bloat_data(rows_to_create)





        #analyzer.display_table_insertion_order()
        #builder.display_table_count()
        #builder.display_row_count_by_table()
    else:
        print("- Please choose an option:\n")
        parser.print_help()
        print("\n")
    print("------------------------------------------------------------------")


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    run()
