version_number = '0.3'

import sys
import argparse
import json
import hashlib
import time
import requests
import wikipedia
import re
import inquirer
from inquirer.themes import GreenPassion
from py2neo import Graph

from collections import OrderedDict
from rich.progress import *
from rich import print
from fuzzy_match import algorithims as algorithms
from fuzzy_match import match

import logging

def removeDuplicateDictFromList(list):
    return [dict(t) for t in {tuple(d.items()) for d in list}]


def sortDictByValues(dict):
    return {k: v for k, v in sorted(dict.items(), key=lambda item: item[1])}


def sortListOfDictsByValue(list, value):
    return sorted(list, key=lambda k: k[value])


def searchDictInList(list, key, value):
    for item in list:
        if item[key] == value:
            return item


def removeDictInList(list, key, value):
    for item in list:
        if item[key] == value:
            list.remove(item)

def raise_error(error):
    if(error):
        print("Error: " + str(error))
    sys.exit(1)


def dump_failed_inserts():
    if very_verbose:
        print(f"[red]Insertion failed for the following objects:\n")
    for chunk in failed_inserts_chunks:
        if very_verbose:
            print(f"{chunk}\n")
        error_logger.error(f"\n{chunk}\n")
    for entity in failed_inserts_entities:
        if very_verbose:
            print(f"{entity}\n")
        error_logger.error(f"\n{entity}\n")
    print(f"[red bold]Saved failed insertions to error.log\n")




# --------  argument parser --------

def add_arguments(argument_parser, list):
    for section in list:
        if section == "api":
            argument_parser.add_argument('--api-address',
                                         help='url of the metasphere api to connect to',
                                         default='http://ecchr.metasphere.xyz:2342'
                                         )
        elif section == "database":
            argument_parser.add_argument('--db-address',
                                         help='url of the metasphere graph database to connect to',
                                         default='bolt://ecchr.metasphere.xyz:7687/'
                                         )
            argument_parser.add_argument('--db-username',
                                         help='username for graph database',
                                         default='neo4j'
                                         )
            argument_parser.add_argument('--db-password',
                                         help='password for graph database',
                                         default='burr-query-duel-cherry'
                                         )
        elif section == "common":
            argument_parser.add_argument('--dry-run',
                                         help='do not write to graph database',
                                         action="store_true", default=False
                                         )
            argument_parser.add_argument('-v', '--verbose',
                                         help='verbose output for debugging',
                                         action="store_true", default=False
                                         )
            argument_parser.add_argument('-vv',
                                         help='very verbose output for debugging',
                                         action="store_true", default=False
                                         )
            argument_parser.add_argument('-V', '--version', action='version',
                                         version="%(prog)s " + version_number
                                         )