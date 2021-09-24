import time
import json
import logging
import os
import pyfiglet
from os.path import exists
import webbrowser
from tabulate import tabulate

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
generation_types = ['schemas', 'analyzers']


def is_list_a_subset(test_list, sub_list):
    if set(sub_list).issubset(set(test_list)):
        return True
    return False


def open_file_in_browser(path):
    # TODO : Make the ability to choose different browsers.
    browser = webbrowser.get('chrome')
    browser.open_new("file://" + path)


def display_in_table(title, table, headers):
    print("\n{title}".format(title=title))
    print(tabulate(table, headers=headers, tablefmt="fancy_grid"))
    print("\n")


def script_intro_title():
    ascii_title = pyfiglet.figlet_format("B l o a t DB", font="3-d")
    print(ascii_title)

