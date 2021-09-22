import argparse
import logging
import sys
import json
import time
import os
import psycopg2
from progress.bar import Bar
from bloat_my_db.utilities import generate_json_file, is_generated_file_exist, load_generated_file, get_filename, read_file

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


class PgSchemaBuilder:

    def __init__(self, conn_info):
        self.connection = psycopg2.connect(**conn_info)
        self.database = conn_info['database']
        self.cursor = self.connection.cursor()
        self.schema = dict()
        self.table_count = 0

    def build_schema(self, table_schema_name='public', force_rebuild=False):

        if is_generated_file_exist(self.database, 'schemas') and not force_rebuild:
            print("{schema_file}.json already exists, using this generated schema...".format(schema_file=get_filename(self.database)))
            self.schema = load_generated_file(self.database, 'schemas')
        else:
            tables = self.build_tables(table_schema_name)
            no_foreign_keys = []
            has_foreign_keys = []
            progress_bar = Bar('Building schema for {database}...'.format(database=self.database), max=len(tables))
            for table in tables:
                columns = self.build_columns(table)
                self.schema[table] = columns
                if columns['@table_metadata']['has_foreign_keys']:
                    has_foreign_keys.append(table)
                else:
                    no_foreign_keys.append(table)
                progress_bar.next()
            progress_bar.finish()
            self.schema["@database_metadata"] = {
                "no_foreign_key_tables": no_foreign_keys,
                "foreign_key_tables": has_foreign_keys
            }
            generate_json_file(self.database, self.schema, 'schemas')

        return self.schema

    def build_tables(self, table_schema_name='public'):
        sql_file = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'sql/build_tables.sql')
        query = read_file(sql_file).format(name=table_schema_name)

        self.cursor.execute(query)
        table_data = self.cursor.fetchall()
        data = []
        for table in table_data:
            data.append(table[1])
            self.table_count += 1
        return data

    def build_columns(self, table_name, table_schema_name='public'):
        sql_file = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'sql/build_columns.sql')
        query = read_file(sql_file).format(schema_name=table_schema_name, table_name=table_name)

        self.cursor.execute(query)
        column_data = self.cursor.fetchall()
        column_data_list = []

        foreign_key_tables = []
        has_foreign_keys = False
        has_user_defined_keys = False
        for column in column_data:
            constraint = self.get_column_constraint(table_name, column[2])
            insert_object = {
                "name": column[2],
                "data_type": column[5],
                "column_default": column[3],
                "is_nullable": column[4],
            }

            if column[5] == 'USER-DEFINED':
                has_user_defined_keys = True
                insert_object["user_defined_type"] = {
                    "name": column[6],
                    "values": self.get_values_from_type(column[6])
                }

            if constraint:
                insert_object["constraint"] = constraint

                for key, value in constraint.items():
                    if value['type'] == 'FOREIGN KEY':
                        foreign_key_tables.append(value['referenced_table'])
                        has_foreign_keys = True
            column_data_list.append(insert_object)

        output = {
            "columns": column_data_list,
            "@table_metadata": {
                "column_count": len(column_data),
                "has_foreign_keys": has_foreign_keys,
                "has_user_defined_keys": has_user_defined_keys,
                "foreign_constraint_tables": foreign_key_tables
            }
        }
        return output

    def get_column_constraint(self, table_name, column_name, table_schema_name='public'):
        sql_file = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'sql/get_column_constraint.sql')
        query = read_file(sql_file).format(schema_name=table_schema_name, table_name=table_name, column_name=column_name)

        self.cursor.execute(query)
        constraint_data = self.cursor.fetchall()
        data = dict()

        for constraint in constraint_data:
            data[constraint[4]] = {
                "type": constraint[5],
                "referenced_table": constraint[8],
                "referenced_column": constraint[9]
            }
        return data

    def get_values_from_type(self, type):
        sql_file = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'sql/get_values_from_type.sql')
        query = read_file(sql_file).format(type=type)

        self.cursor.execute(query)
        types = []
        type_data = self.cursor.fetchall()
        for tdata in type_data:
            types.append(tdata[1])
        return types

    def get_table_count(self):
        return self.table_count
