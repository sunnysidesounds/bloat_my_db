import argparse
import logging
import sys
import os
import random
import psycopg2
from progress.bar import Bar
from bloat_my_db.utilities import generate_json_file, read_file, display_in_table, is_generated_file_exist, get_filename, load_generated_file

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


class PgSchemaAnalyzer:

    def __init__(self, schema, conn_info):
        self.connection = psycopg2.connect(**conn_info)
        self.database = conn_info['database']
        self.cursor = self.connection.cursor()
        self.schema = schema
        self.analyzed_schema = dict()
        self.insert_order = 1

    def build_analyzer_schema(self, force_rebuild=False):
        if is_generated_file_exist(self.database, 'analyzers') and not force_rebuild:
            print("- {schema_file}.json already exists, using this generated analyzer schema...".format(schema_file=get_filename(self.database)))
            self.analyzed_schema = load_generated_file(self.database, 'analyzers')
        else:
            insertion_table_order = self.get_insertion_table_order()
            progress_bar = Bar('- Analyzing schema for {database}, determining insertion order...'.format(database=self.database),
                               max=len(insertion_table_order))
            for table in insertion_table_order:
                table = table.replace("\"", "")
                self.analyzed_schema[self.insert_order] = {
                    table: self.schema[table]
                }
                self.insert_order += 1
                progress_bar.next()
            progress_bar.finish()
            generate_json_file(self.database, self.analyzed_schema, 'analyzers')
        return self.analyzed_schema

    def display_table_insertion_order(self):
        display_list = []
        for order in self.analyzed_schema:
            table = self.analyzed_schema[order]
            table_name = list(table.keys())[0]
            display_list.append([order, table_name])
        display_in_table("Insertion Table Order Results:", display_list, ["INSERT_ORDER", "TABLE_NAME"])

    def get_insertion_table_order(self):
        sql_file = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'sql/get_insertion_table_order.sql')
        query = read_file(sql_file)
        self.cursor.execute(query)
        insertion_data = self.cursor.fetchall()
        output = []
        for insertion_name in insertion_data:
            output.append(insertion_name[0])
        return output
