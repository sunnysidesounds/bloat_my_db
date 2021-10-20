import logging
import time
import psycopg2
from zipfile import ZipFile
from progress.bar import Bar

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


class CsvImporter:

    def __init__(self, analyzed_schema, conn_info):
        self.connection = psycopg2.connect(**conn_info)
        self.database = conn_info['database']
        self.cursor = self.connection.cursor()
        self.analyzed_schema = analyzed_schema

    def import_db(self, selected_file, analyzed_schema, database):
        file_date = time.strftime("%Y%m%d")
        if selected_file.lower().endswith(('.zip')):
            with ZipFile(selected_file) as zf:
                progress_bar = Bar('- Importing CSVs to {database}...'.format(database=self.database), max=len(analyzed_schema.items()))
                for key, value in analyzed_schema.items():
                    table = list(value.keys())[0]
                    file_name = "{database}/{key}_{table}_{date}.csv".format(key=key, table=table, date=file_date, database=database)
                    with zf.open(file_name, 'r') as infile:
                        copy_sql = """COPY "{table_name}" FROM stdin WITH CSV HEADER DELIMITER as ','""".format(table_name=table)
                        self.cursor.copy_expert(sql=copy_sql, file=infile)
                        self.connection.commit()
                    progress_bar.next()
        progress_bar.finish()
