import logging
import json
import os
from os.path import exists
import time

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
generation_types = ['schemas', 'analyzers']


class FileUtility:
    @staticmethod
    def generate_json_file(database_name, data, generation_type):
        file_name = FileUtility.get_filename(database_name)
        json_schema = json.dumps(data, indent=4)

        if generation_type not in generation_types:
            raise Exception("generation_type {type} not found!".format(type=generation_type))

        path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'generated')
        with open('{path}/{type}/{file_name}.json'.format(path=path, file_name=file_name, type=generation_type),
                  'w') as outfile:
            outfile.write(json_schema)

    @staticmethod
    def get_filename(database_name):
        file_date = time.strftime("%Y%m%d")
        return "{database}_{date}".format(database=database_name, date=file_date)

    @staticmethod
    def is_generated_file_exist(database_name, generation_type):
        full_path = FileUtility.get_generated_file_path(database_name, generation_type)
        return exists(full_path)

    @staticmethod
    def load_generated_file(database_name, generation_type):
        full_path = FileUtility.get_generated_file_path(database_name, generation_type)
        with open(full_path) as json_file:
            return json.load(json_file)

    @staticmethod
    def get_generated_file_path(database_name, generation_type):
        file_name = FileUtility.get_filename(database_name)
        path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'generated')
        return '{path}/{type}/{file_name}.json'.format(path=path, file_name=file_name, type=generation_type)

    @staticmethod
    def read_file(file_path):
        file = open(file_path)
        contents = file.read()
        file.close()
        return contents

    @staticmethod
    def delete_files(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                _logger.error('Failed to delete %s. Reason: %s' % (file_path, e))

    @staticmethod
    def purge_generated_files():
        generated_schemas_path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'generated/schemas')
        generated_analyzers_path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'generated/analyzers')
        FileUtility.delete_files(generated_schemas_path)
        FileUtility.delete_files(generated_analyzers_path)
