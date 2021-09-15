import argparse
import logging
import sys

from bloat_my_db import __version__

__author__ = "Jason R Alexander"
__copyright__ = "Jason R Alexander"
__license__ = "MIT"

_logger = logging.getLogger(__name__)

class GenerateDbAudit:
    def __init__(self):
        print("Init")
        pass

    def audit(self, value):
        print("Generate DB Audit {}".format(value))
