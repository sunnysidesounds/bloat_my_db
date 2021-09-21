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
        for table in self.get_insertion_table_order():
            self.analyzed_schema[self.insert_order] = table
            self.insert_order += 1
        return self.analyzed_schema

    def get_insertion_table_order(self):
        query = """
                WITH RECURSIVE fkeys AS (
                   SELECT conrelid AS source,
                          confrelid AS target
                   FROM pg_constraint
                   WHERE contype = 'f'
                ),
                tables AS (
                      ( 
                          SELECT oid AS table_name,
                                 1 AS level,
                                 ARRAY[oid] AS trail,
                                 FALSE AS circular
                          FROM pg_class
                          WHERE relkind = 'r'
                            AND NOT relnamespace::regnamespace::text LIKE ANY
                                    (ARRAY['pg_catalog', 'information_schema', 'pg_temp_%'])
                       EXCEPT
                          SELECT source,
                                 1,
                                 ARRAY[ source ],
                                 FALSE
                          FROM fkeys
                      )
                   UNION ALL
                      SELECT fkeys.source,
                             tables.level + 1,
                             tables.trail || fkeys.source,
                             tables.trail @> ARRAY[fkeys.source]
                      FROM fkeys
                         JOIN tables ON tables.table_name = fkeys.target
                      WHERE cardinality(array_positions(tables.trail, fkeys.source)) < 2
                ),
                ordered_tables AS (
                   SELECT DISTINCT ON (table_name)
                          table_name,
                          level,
                          circular
                   FROM tables
                   ORDER BY table_name, level DESC
                )
                SELECT table_name::regclass,
                       level
                FROM ordered_tables
                WHERE NOT circular
                ORDER BY level, table_name;
        """
        self.cursor.execute(query)
        insertion_data = self.cursor.fetchall()
        output = []
        for insertion_name in insertion_data:
            output.append(insertion_name[0])
        return output
