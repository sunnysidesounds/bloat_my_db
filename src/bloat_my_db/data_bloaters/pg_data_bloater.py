import argparse
import logging
import sys
import os
import random
import psycopg2
from psycopg2 import sql
import psycopg2.extras as psql_extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from progress.bar import Bar
import pandas as pd
from bloat_my_db.utilities import generate_json_file, read_file, display_in_table

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

    def bloat_data(self, how_many=1):
        for key, value in self.analyzed_schema.items():
            table = list(value.keys())[0]

            # TODO Need to create multiple dataframework based on how_many
            dataframe = self.build_dataframe(value[table]['columns'])
            self.populate_table(how_many, table, dataframe)

    def build_dataframe(self, columns):
        dataframe = {}
        for column in columns:
            dataframe[column['name']] = [self.get_data_by_type(column['data_type'])]
        return pd.DataFrame(dataframe)

    def get_data_by_type(self, data_type):
        print(data_type)
        return 'TODO'

    def populate_table(self, how_many, table_name, dataframe):
        progress_bar = Bar(' - building [{}] table '.format(table_name), max=how_many)
        for index in range(how_many):
            columns = ', '.join(dataframe.columns.tolist())
            query = """INSERT INTO {table_name}({columns} ) VALUES %s""".format(table_name=table_name, columns=columns)
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
