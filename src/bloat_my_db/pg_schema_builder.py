import argparse
import logging
import sys
import psycopg2
from progress.bar import Bar

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
        self.view_count = 0

    def generate_schema(self, table_schema_name='public'):
        tables = self.generate_tables(table_schema_name)
        no_foreign_keys = []
        has_foreign_keys = []
        progress_bar = Bar('Building schema for {database}...'.format(database=self.database), max=len(tables))
        for table in tables:
            columns = self.generate_columns(table)
            self.schema[table] = columns

            if columns['@table_metadata']['has_foreign_keys']:
                has_foreign_keys.append(table)
            else:
                no_foreign_keys.append(table)

            progress_bar.next()
        progress_bar.finish()
        self.schema["@database_metadata"] = {
            "no_foreign_key_tables": no_foreign_keys,
            "has_foreign_key_tables": has_foreign_keys
        }
        return self.schema


    def generate_tables(self, table_schema_name='public'):
            query = """
            select
              table_schema,
              table_name,
              obj_description((table_schema || '."' || table_name || '"')::regclass, 'pg_class')
            from information_schema.tables
            where table_schema = '{name}'
            order by table_schema, table_name
            """.format(name=table_schema_name)

            self.cursor.execute(query)
            table_data = self.cursor.fetchall()
            data = []
            for table in table_data:
                data.append(table[1])
                self.table_count += 1
            return data

    def generate_columns(self, table_name, table_schema_name ='public'):
        query = """
        select
          table_schema,
          table_name,
          column_name,
          column_default,
          is_nullable::boolean,
          data_type,
          col_description((table_schema || '."' || table_name || '"')::regclass, ordinal_position)
        from information_schema.columns
        where table_schema = '{schema_name}' and table_name = '{table_name}'
        order by table_schema, table_name, ordinal_position
        """.format(schema_name=table_schema_name, table_name=table_name)

        self.cursor.execute(query)
        column_data = self.cursor.fetchall()
        data = {}
        foreign_key_tables = {}
        has_foreign_keys = False
        for column in column_data:
            constraint = self.get_column_constraint(table_name, column[2])
            data[column[2]] = {
                "data_type": column[5],
                "column_default": column[3],
                "is_nullable": column[4],
            }
            if constraint:
                data[column[2]] = {
                    "data_type": column[5],
                    "column_default": column[3],
                    "is_nullable": column[4],
                    "constraint": constraint
                }

                for key, value in constraint.items():
                    if value['type'] == 'FOREIGN KEY':
                        foreign_key_tables[value['referenced_table']] = value['referenced_column']
                        has_foreign_keys = True

        data['@table_metadata'] = {
            "column_count": len(column_data),
            "has_foreign_keys": has_foreign_keys,
            "foreign_constraint_tables": foreign_key_tables

        }
        return data

    def get_column_constraint(self, table_name, column_name, table_schema_name='public'):
        query = """
        select
          coalesce(table_schema, referenced_schema) as table_schema,
          coalesce(table_name, referenced_table) as table_name,
          coalesce(column_name, referenced_column) as column_name,
          constraint_schema,
          constraint_name,
          constraint_type,
          check_clause,
          referenced_schema,
          referenced_table,
          referenced_column
          
        from information_schema.table_constraints
        natural full join information_schema.key_column_usage
        natural full join information_schema.check_constraints
        inner join (
          select
            table_schema as referenced_schema,
            table_name as referenced_table,
            column_name as referenced_column,
            constraint_name
          from information_schema.constraint_column_usage
        ) as referenced_columns using (constraint_name)
        
        where constraint_schema = '{schema_name}' and table_name = '{table_name}'  and column_name = '{column_name}'
        order by table_schema, table_name, ordinal_position
        """.format(schema_name=table_schema_name, table_name=table_name, column_name=column_name)

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

    def get_table_count(self):
        return self.table_count



# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.

def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Just a Fibonacci demonstration")
    parser.add_argument(
        "--version",
        action="version",
        version="bloat_my_db {ver}".format(ver=__version__),
    )
    parser.add_argument(dest="n", help="n-th Fibonacci number", type=int, metavar="INT")
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)

def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )

def main(args):
    """Wrapper allowing :func:`fib` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    schema = SchemaBuilder()
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.debug("Starting crazy calculations...")
    print("The {}-th Fibonacci number is {}".format(args.n, schema.generate(args.n)))
    _logger.info("Script ends here")

def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
