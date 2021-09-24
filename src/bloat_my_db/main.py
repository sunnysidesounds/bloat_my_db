import os
import json
import argparse
import logging
import sys
from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
from bloat_my_db.data_bloaters.pg_data_bloater import PgDataBloater
from bloat_my_db.utilities import read_file, purge_generated_files, get_generated_file_path, open_file_in_browser, script_intro_title

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Utility tool that adds random data to your database for development.",
                                 epilog="Version: {version}".format(version=__version__))


def parse_args(args):
    parser.add_argument('-buildSchema', help="builds the database schema file", action='store_true')
    parser.add_argument('-buildAnalyzedSchema', help="builds the analyzed database schema file", action='store_true')
    parser.add_argument('-populate', help="Takes the analyzed schema and bloats the database with random data", action='store_true')
    parser.add_argument('-config', help="configuration file", type=str)
    parser.add_argument('-rows', help="How may rows do you want to bloat the database, default is 25", type=int)
    parser.add_argument('-purge', help="purges both the schema and analyzed schema JSON files", action='store_true')
    parser.add_argument('-force', help="force rebuilds both schemas (used with -populate flag)", action='store_true')
    parser.add_argument('-openSchema', help="Opens the built schema in Chrome browser", action='store_true')
    parser.add_argument('-openAnalyzedSchema', help="Opens the built analyzed schema in Chrome browser", action='store_true')

    return parser.parse_args(args)


def setup_logging():
    log_format = "[%(asctime)s] %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format=log_format, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    print("--------------------------------------------------------------------------------------------------------")
    script_intro_title()
    args = parse_args(args)

    configuration = os.getenv('BLOAT_CONFIG')
    if not args.config and not configuration:
        print("Error: please provide a configuration file. -config flag or set BLOAT_CONFIG=<path> env variable")
        sys.exit()

    if args.config:
        configuration = args.config

    setup_logging()
    conn_info = json.loads(read_file(configuration))
    database = conn_info['database']
    force_rebuild = args.force if args.force else False

    if args.purge:
        purge_generated_files()
        logging.info("Completed generated file purge!")
        sys.exit()

    if args.buildSchema:
        builder = PgSchemaBuilder(conn_info)
        builder.build_schema(force_rebuild=True)
        print("- Completed building schema for {database}!".format(database=database))
        sys.exit()

    if args.buildAnalyzedSchema:
        builder = PgSchemaBuilder(conn_info)
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, conn_info)
        analyzer.build_analyzer_schema(force_rebuild=True)
        print("- Completed building analyzed schema for {database}!".format(database=database))
        sys.exit()

    if args.openSchema:
        schema_path = get_generated_file_path(database, 'schemas')
        open_file_in_browser(schema_path)
        print("- Opened schema for {database} in browser!".format(database=database))
        sys.exit()

    if args.openAnalyzedSchema:
        analyzer_path = get_generated_file_path(database, 'analyzers')
        open_file_in_browser(analyzer_path)
        print("- Opened analyzed schema for {database} in browser!".format(database=database))
        sys.exit()

    if args.populate:
        default_rows_to_generate = 25
        builder = PgSchemaBuilder(conn_info)
        schema = builder.build_schema(force_rebuild=force_rebuild)
        analyzer = PgSchemaAnalyzer(schema, conn_info)
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=force_rebuild)
        if analyzed_schema:
            print("- Completed building & analyzing {database} database!".format(database=database))
            bloater = PgDataBloater(analyzed_schema, conn_info)
            rows_to_create = args.rows if args.rows else default_rows_to_generate
            bloater.bloat_data(rows_to_create)
            print("- Completed bloating {database} database!".format(database=database, rows=rows_to_create))
            builder.display_stat_results(rows_to_create)

        # analyzer.display_table_insertion_order()
        # builder.display_table_count()
        # builder.display_row_count_by_table()
    else:
        parser.print_help()
        print("\n")
    print("--------------------------------------------------------------------------------------------------------")


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    run()
