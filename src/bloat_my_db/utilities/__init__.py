import time
import json
import os
from os.path import exists
from collections import Counter

generation_types = ['schemas', 'analyzers']


def generate_json_file(database_name, data, generation_type):
    file_date = time.strftime("%Y%m%d")
    file_name = "{database}_{date}".format(database=database_name, date=file_date)
    json_schema = json.dumps(data, indent=4)

    if generation_type not in generation_types:
        raise Exception("generation_type {type} not found!".format(type=generation_type))

    path = os.path.join(os.getcwd(), 'src/bloat_my_db/generated')
    with open('{path}/{type}/{file_name}.json'.format(path=path, file_name=file_name, type=generation_type), 'w') as outfile:
        outfile.write(json_schema)


def is_generated_file_exist(database_name, generation_type):
    full_path = get_current_file_path(database_name, generation_type)
    return exists(full_path)


def load_generated_file(database_name, generation_type):
    full_path = get_current_file_path(database_name, generation_type)
    with open(full_path) as json_file:
        return json.load(json_file)


def get_current_file_path(database_name, generation_type):
    file_name = get_filename(database_name)
    path = os.path.join(os.getcwd(), 'src/bloat_my_db/generated')
    return '{path}/{type}/{file_name}.json'.format(path=path, file_name=file_name, type=generation_type)


def get_filename(database_name):
    file_date = time.strftime("%Y%m%d")
    return "{database}_{date}".format(database=database_name, date=file_date)


def is_list_a_subset(test_list, sub_list):
    if set(sub_list).issubset(set(test_list)):
        return True
    return False
