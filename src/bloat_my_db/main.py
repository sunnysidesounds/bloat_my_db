import os
import json
import argparse
import logging
import sys
from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
from bloat_my_db.data_bloaters.pg_data_bloater import PgDataBloater
from bloat_my_db.importers.csv_import import CsvImporter
from bloat_my_db.exporters.csv_export import CsvExporter
from bloat_my_db.utilities.file import FileUtility
from bloat_my_db.utilities.db import DatabaseUtility
from bloat_my_db.utilities import open_file_in_browser, script_intro_title

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Utility tool that populates random or CSV data to your database for development purposes",
                                 epilog="Version: {version}".format(version=__version__))

def parse_args(args):
    parser.add_argument('-buildSchema', help="builds the database schema file", action='store_true')
    parser.add_argument('-buildAnalyzedSchema', help="builds the analyzed database schema file", action='store_true')
    parser.add_argument('-buildCSVSchema', help="builds CSV files of all the table schemas (To had your own data)", action='store_true')
    parser.add_argument('-populateRandomData', help="Takes the analyzed schema and bloats the database with random data", action='store_true')
    parser.add_argument('-populateCSVData', help="Takes the analyzed schema and bloats the database with specified CSV data", action='store_true')
    parser.add_argument('-config', help="configuration file or set BLOAT_CONFIG=<path> env variable", type=str)
    parser.add_argument('-workSpacePath', help="The path on were to import/export files (zips, csv), can also set it in the bloat_config.json", type=str)
    parser.add_argument('-importCSVFile', help="The CSV or zip file (of CSV's) you want to import into the database", type=str)
    parser.add_argument('-rows', help="How may rows do you want to bloat the database, default is 25", type=int)
    parser.add_argument('-purge', help="purges all generated files (both the schema, analyzed schema and CSV export files)", action='store_true')
    parser.add_argument('-force', help="force rebuilds both schemas (used with -populateRandomData flag)", action='store_true')
    parser.add_argument('-disablePrompt', help="Disables the user CLI input prompt", action='store_true')
    parser.add_argument('-openSchema', help="Opens the built schema in Chrome browser", action='store_true')
    parser.add_argument('-openAnalyzedSchema', help="Opens the built analyzed schema in Chrome browser", action='store_true')
    parser.add_argument('-truncateDb', help="Truncates all the values in the database", action='store_true')
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
    db_utility = DatabaseUtility(configuration_values['db'])
    database = configuration_values['db']['database']
    force_rebuild = args.force if args.force else False
    default_rows_to_generate = 25

    workspace_path = args.workSpacePath if args.workSpacePath else configuration_values['paths']['workspace_path']
    if not workspace_path:
        print("Error: required set your -workSpacePath flag or set it in your configuration file")
        sys.exit()

    if args.purge:
        FileUtility.purge_generated_files()
        logging.info("Completed generated file purge!")
        sys.exit()

    if args.truncateDb:
        db_utility.truncate_db()
        print("- Completed truncating table for {database}!".format(database=database))

    if args.buildSchema:
        FileUtility.purge_schema_files()
        builder = PgSchemaBuilder(configuration_values['db'])
        builder.build_schema(force_rebuild=True)
        print("- Completed building schema for {database}!".format(database=database))

    if args.buildAnalyzedSchema:
        FileUtility.purge_analyzer_files()
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzer.build_analyzer_schema(force_rebuild=True)
        print("- Completed building analyzed schema for {database}!".format(database=database))

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
        FileUtility.purge_csv_files(database)
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=force_rebuild)
        if analyzed_schema:
            exporter = CsvExporter(analyzed_schema, configuration_values['db'])
            rows_to_create = args.rows if args.rows else default_rows_to_generate
            exporter.export_db(rows_to_create)
            csv_file_path = FileUtility.get_csv_file_directory_path(database)
            workspace_path = "{export_path}{database}".format(export_path=workspace_path, database=database)
            FileUtility.zip_and_save_folder(csv_file_path, workspace_path)
            print("- Completed building CSV export files for {database} database!".format(database=database))
        sys.exit()

    if args.populateCSVData:
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=False)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=False)
        acceptable_files = FileUtility.get_files_in_workspace(workspace_path, args)
        # if user prompt is disabled get first matching user option file.
        user_option = 1
        if not args.disablePrompt:
            user_option = int(input('Please choose a (csv or zip) file to import:'))

        selected_file = acceptable_files[user_option]
        importer = CsvImporter(analyzed_schema, configuration_values['db'])
        db_utility.truncate_db()
        csv_files = importer.import_db(selected_file, analyzed_schema, database)

        print(csv_files)
        sys.exit()

    if args.populateRandomData:
        builder = PgSchemaBuilder(configuration_values['db'])
        schema = builder.build_schema(force_rebuild=force_rebuild)
        analyzer = PgSchemaAnalyzer(schema, configuration_values['db'])
        analyzed_schema = analyzer.build_analyzer_schema(force_rebuild=force_rebuild)
        if analyzed_schema:
            print("- Completed building & analyzing {database} database!".format(database=database))
            bloater = PgDataBloater(analyzed_schema, configuration_values['db'])
            rows_to_create = args.rows if args.rows else default_rows_to_generate
            bloater.feed_db(rows_to_create)
            print("- Completed bloating {database} database!".format(database=database, rows=rows_to_create))
            builder.display_stat_results(rows_to_create)

    print("--------------------------------------------------------------------------------------------------------")


def run():
    main(sys.argv[1:])


if __name__ == '__main__':
    run()
