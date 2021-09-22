import argparse
import logging
import sys
import os
import random
import psycopg2
from progress.bar import Bar
from bloat_my_db.utilities import generate_json_file, read_file

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
        self.tables_with_insert_orders = []

    def analyze(self):
        insertion_table_order = self.get_insertion_table_order()
        progress_bar = Bar('Analyzing schema for {database}, determining insertion order...'.format(database=self.database),
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
        for order in self.analyzed_schema:
            table = self.analyzed_schema[order]
            table_name = list(table.keys())[0]
            print("{order} - {table}".format(order=order, table=table_name))

    def get_insertion_table_order(self):
        sql_file = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'sql/get_insertion_table_order.sql')
        query = read_file(sql_file)

        self.cursor.execute(query)
        insertion_data = self.cursor.fetchall()
        output = []
        for insertion_name in insertion_data:
            output.append(insertion_name[0])
        return output
