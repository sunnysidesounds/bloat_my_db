import argparse
import logging
import sys
import os
import uuid
import random
import psycopg2
from psycopg2 import sql
import psycopg2.extras as psql_extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from progress.bar import Bar
import pandas as pd
from bloat_my_db.randoms import Randoms

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


class PgDataBloater:

    def __init__(self, analyzed_schema, conn_info):
        self.connection = psycopg2.connect(**conn_info)
        self.database = conn_info['database']
        self.cursor = self.connection.cursor()
        self.analyzed_schema = analyzed_schema

    def feed_db(self, how_many):
        for key, value in self.analyzed_schema.items():
            table = list(value.keys())[0]
            self.populate_table(how_many, table, value[table]['columns'])

    def build_dataframe(self, columns):
        dataframe = {}
        for column in columns:
            value = self.get_random_data_by_type(column)
            dataframe[column['name']] = [value]
        return pd.DataFrame(dataframe)

    def get_csv_data_by_type(self, column):
        return "TODO"

    def get_random_data_by_type(self, column):
        value = None
        # check if is_nullable is True (if False we need a value)
        if not column['is_nullable']:
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
                    value = str(self.get_random_row(constraint_column, constraint_table)[0])
                elif 'PRIMARY KEY' in types and column['data_type'] == 'uuid':
                    value = str(uuid.uuid4())
                elif 'PRIMARY KEY' in types:
                    value = Randoms.get_hash(25)
                else:
                    value = 'TODO'
            else:
                if column['data_type'] == 'character varying':
                    value = Randoms.get_custom_text(column['name'])
                elif column['data_type'] == 'timestamp without time zone':
                    value = Randoms.get_datetime(min_year=2000)
                elif column['data_type'] == 'text':
                    value = Randoms.get_custom_text(column['name'])
                elif column['data_type'] == 'boolean':
                    value = Randoms.get_boolean()
                elif column['data_type'] == 'USER-DEFINED':
                    user_defined_values = column['user_defined_type']['values']
                    value = Randoms.get_value_from_list(user_defined_values)
                elif column['data_type'] == 'integer':
                    value = Randoms.get_number()
                else:
                    print(column['data_type'])

        return value

    def populate_table(self, how_many, table_name, columns_data):
        progress_bar = Bar(' - building [{}] table '.format(table_name), max=how_many)
        for index in range(how_many):
            dataframe = self.build_dataframe(columns_data)
            columns = ', '.join(dataframe.columns.tolist())
            query = """INSERT INTO "{table_name}"({columns} ) VALUES %s ON CONFLICT DO NOTHING""".format(table_name=table_name, columns=columns)
            self.insert_table_data(index + 1, query, self.connection, self.cursor, dataframe, how_many)
            progress_bar.next()
        progress_bar.finish()

    def insert_table_data(
            self,
            index: int,
            query: str,
            conn: psycopg2.extensions.connection,
            cur: psycopg2.extensions.cursor,
            df: pd.DataFrame,
            page_size: int
    ) -> None:
        data_tuples = [tuple(row.to_numpy()) for index, row in df.iterrows()]

        try:
            psql_extras.execute_values(cur, query, data_tuples, page_size=page_size)
        except Exception as error:
            print("- {index}) FAILED query execution for:: \"{query}\" ".format(index=index, query=cur.query.decode("utf-8")))
            print("\n")
            print("ERROR: {error}".format(error=error))
            conn.rollback()
            cur.close()
            sys.exit()
        else:
            conn.commit()

    # TODO: Move this out
    def get_random_row(self, columns, table):
        self.cursor.execute("SELECT {columns} FROM {table} ORDER BY random() LIMIT 1".format(columns=columns, table=table))
        return self.cursor.fetchone()

    def get_random_row_where(self, columns, table, query):
        self.cursor.execute("SELECT {columns} FROM {table} {query} ORDER BY random() LIMIT 1".format(columns=columns, table=table, query=query))
        return self.cursor.fetchone()
