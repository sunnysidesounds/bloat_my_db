import argparse
import logging
import sys
import os
import uuid
import random
import psycopg2
import csv
from psycopg2 import sql
import psycopg2.extras as psql_extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from progress.bar import Bar
import pandas as pd
from bloat_my_db.randoms import Randoms
from bloat_my_db.utilities.file import FileUtility

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


class CsvExporter:

    def __init__(self, analyzed_schema, conn_info):
        self.connection = psycopg2.connect(**conn_info)
        self.database = conn_info['database']
        self.cursor = self.connection.cursor()
        self.analyzed_schema = analyzed_schema

    def export_db(self, how_many):
        for key, value in self.analyzed_schema.items():
            table = list(value.keys())[0]
            table_file = FileUtility.get_csv_file_path("{key}_{table}".format(key=key, table=table), self.database)

            csv_header = self.get_csv_header_from_schema(key, table)
            columns = self.analyzed_schema[key][table]['columns']
            csv_rows = []
            for index in range(how_many):
                sub_csv_rows = self.set_constraint_keys(columns)
                csv_rows.append(sub_csv_rows)

            with open(table_file, 'w') as f:
                writer = csv.writer(f)
                writer.writerow(csv_header)
                writer.writerows(csv_rows)

    def set_constraint_keys(self, columns):
        csv_columns = []
        for column in columns:
            if 'constraint' in column:
                constraint_list = list(column['constraint'])
                constraint_values = list(column['constraint'].values())
                types = [const_dict['type'] for const_dict in constraint_values]
                if 'FOREIGN KEY' in types:
                    index = types.index('FOREIGN KEY')
                    constraint_name = constraint_list[index]
                    constraint = column['constraint'][constraint_name]
                    constraint_table = constraint['referenced_table']
                    constraint_column = constraint['referenced_column']

                    random_row = self.get_random_row(constraint_column, constraint_table)
                    if not random_row:
                        print(random_row)

                    value = str(self.get_random_row(constraint_column, constraint_table)[0])
                    csv_columns.append(value)
                elif 'PRIMARY KEY' in types and column['data_type'] == 'uuid':
                    value = str(uuid.uuid4())
                    csv_columns.append(value)
                else:
                    csv_columns.append("")
            else:
                if column['is_nullable']:
                    csv_columns.append("NOT NULL")
                else:
                    csv_columns.append("")

        return csv_columns

    def get_csv_header_from_schema(self, key,  table):
        column_data = self.analyzed_schema[key][table]['columns']
        return [d['name'] for d in column_data]

    # TODO: Move this out
    def get_random_row(self, columns, table):
        self.cursor.execute("SELECT {columns} FROM {table} ORDER BY random() LIMIT 1".format(columns=columns, table=table))
        return self.cursor.fetchone()
