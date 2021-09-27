import os
import json
import argparse
import logging
import sys
from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
from bloat_my_db.data_bloaters.pg_data_bloater import PgDataBloater
from bloat_my_db.exporters.csv_export import CsvExporter
from bloat_my_db.utilities.file import FileUtility
from bloat_my_db.utilities import open_file_in_browser, script_intro_title

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
    parser.add_argument('-buildCSVSchema', help="builds CSV files of all the table schemas (To had your own data)", action='store_true')
    parser.add_argument('-populate', help="Takes the analyzed schema and bloats the database with random data", action='store_true')
    parser.add_argument('-config', help="configuration file or set BLOAT_CONFIG=<path> env variable", type=str)
    parser.add_argument('-rows', help="How may rows do you want to bloat the database, default is 25", type=int)
    parser.add_argument('-purge', help="purges both the schema and analyzed schema JSON files", action='store_true')
    parser.add_argument('-force', help="force rebuilds both schemas (used with -populate flag)", action='store_true')
    parser.add_argument('-openSchema', help="Opens the built schema in Chrome browser", action='store_true')
    parser.add_argument('-openAnalyzedSchema', help="Opens the built analyzed schema in Chrome browser", action='store_true')
    parser.add_argument('-importType', help="The kind of data you want to import [random, csv]", type=str)
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
    configuration_values = json.loads(FileUtility.read_file(configuration))
    database = configuration_values['db']['database']
    export_path = configuration_values['paths']['export_path']
    force_rebuild = args.force if args.force else False
    default_rows_to_generate = 25

    if args.purge:
        FileUtility.purge_generated_files()
        logging.info("Completed generated file purge!")
        sys.exit()

    if args.buildSchema:
        builder = PgSchemaBuilder(configuration_values['db'])
        builder.build_schema(force_rebuild=True)
        print("- Completed building schema for {database}!".format(database=database))
        sys.exit()

    if args.buildAnalyzedSchema:
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzer.build_analyzer_schema(force_rebuild=True)
        print("- Completed building analyzed schema for {database}!".format(database=database))
        sys.exit()

    if args.openSchema:
        schema_path = FileUtility.get_generated_file_path(database, 'schemas')
        open_file_in_browser(schema_path)
        print("- Opened schema for {database} in browser!".format(database=database))
        sys.exit()

    if args.openAnalyzedSchema:
        analyzer_path = FileUtility.get_generated_file_path(database, 'analyzers')
        open_file_in_browser(analyzer_path)
        print("- Opened analyzed schema for {database} in browser!".format(database=database))
        sys.exit()

    if args.buildCSVSchema:
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=force_rebuild)
        if analyzed_schema:
            exporter = CsvExporter(analyzed_schema, configuration_values['db'])
            rows_to_create = args.rows if args.rows else default_rows_to_generate
            exporter.export_db(rows_to_create)
            csv_file_path = FileUtility.get_csv_file_directory_path(database)
            export_path =  "{export_path}{database}".format(export_path=export_path, database=database)
            FileUtility.zip_and_save_folder(csv_file_path, export_path)
            print("- Completed building CSV export files for {database} database!".format(database=database))
        sys.exit()

    if args.populate:
        default_import_type = 'random'
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=force_rebuild)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=force_rebuild)
        if analyzed_schema:
            print("- Completed building & analyzing {database} database!".format(database=database))
            import_type = args.importType if args.importType else default_import_type
            bloater = PgDataBloater(analyzed_schema, configuration_values['db'], import_type)
            rows_to_create = args.rows if args.rows else default_rows_to_generate
            bloater.feed_db(rows_to_create)
            print("- Completed bloating {database} database!".format(database=database, rows=rows_to_create))
            builder.display_stat_results(rows_to_create)
    else:
        parser.print_help()
        print("\n")
    print("--------------------------------------------------------------------------------------------------------")


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    run()
