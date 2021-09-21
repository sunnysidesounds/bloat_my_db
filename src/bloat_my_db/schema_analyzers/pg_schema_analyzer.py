import argparse
import logging
import sys
import random
import psycopg2
from progress.bar import Bar
from bloat_my_db.utilities import is_list_a_subset

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

        for table in self.get_no_foreign_key_tables():
            self.analyzed_schema[self.insert_order] = table
            self.tables_with_insert_orders.append(table)
            self.insert_order += 1

        foreign_key_tables = self.get_foreign_key_tables()

        index = 0
        t_length = len(foreign_key_tables)
        while t_length != 0:
            if index >= t_length:
                index = 0
            table = foreign_key_tables[index]
            constraint_table_schema = list(set(self.schema[table]['@table_metadata']['foreign_constraint_tables']))
            if is_list_a_subset(self.tables_with_insert_orders, constraint_table_schema):
                self.analyzed_schema[self.insert_order] = table
                self.tables_with_insert_orders.append(table)
                foreign_key_tables.remove(table)
                t_length = len(foreign_key_tables)
                self.insert_order += 1

            index += 1


        print(foreign_key_tables)
        return self.analyzed_schema

    def get_no_foreign_key_tables(self):
        return self.schema['@database_metadata']['no_foreign_key_tables']

    def get_foreign_key_tables(self):
        return self.schema['@database_metadata']['foreign_key_tables']
