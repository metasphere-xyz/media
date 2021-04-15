#!/usr/bin/env python3

version_number = '0.1'

from configuration.ecchr import matched_entities

augmented_entities = [
    'PERSON',
    'GPE'
]

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

argument_parser = argparse.ArgumentParser(
    description='Fetches entities from metasphere graph database and lets you augment their properties'
)

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

arguments = argument_parser.parse_args()

current_task = 'Starting'
db_address = vars(arguments)['db_address']
db_username = vars(arguments)['db_username']
db_password = vars(arguments)['db_password']
verbose = vars(arguments)['verbose']
very_verbose = vars(arguments)['vv']
dry_run = vars(arguments)['dry_run']

checkmark = f"[green]"u'\u2713 '
cross = f"[red]"u'\u00D7 '
eye = f"[white]"u'\u2022 '
arrow = f"[grey]"u'\u21B3 '
database = f"[white]DATABASE[/white]:"

if very_verbose:
    print (arguments)


def connect_to_graph_database():
    print (eye, f"[bold]Connecting to[/bold] graph database.")
    try:
        graph = Graph(
            db_address,
            auth=(db_username, db_password)
        )
    except:
        print (cross, f"Can't connect to graph database. Exiting.")
        sys.exit(1)
    finally:
        print (checkmark, f"Connected to graph database.")
    return graph
graph = connect_to_graph_database()

failed_requests = []
entities_to_process = []
def load_entities_from_database():
    print(database, f"Loading entities from database.")

    query = 'MATCH (e:Entity) WHERE e:PERSON or e:ORG RETURN e'
    parameters = {}
    results = [record for record in graph.run(query).data()]
    entities = []
    if verbose: print(results)
    for record in results:
        entities.append({
            "entity_id": record['e']["entity_id"],
            "name": record['e']["name"],
            "text": record['e']["text"],
            "url": record['e']["url"]
        })
    return entities


def save_entity_to_database(entity):
    entity_id = entity["entity_id"]
    name = entity["name"]
    text = entity["text"]
    print(database, f"Saving entity to database: [bold]{entity_name}[/bold] {entity_id}")
    if verbose: print (entity)
    query = 'MATCH (e:Entity) WHERE e.entity_id = "$entity_id" \
            SET e.name = "$name", \
            e.text = "$text" \
            RETURN e'
    parameters = {
        "entity_id": entity_id,
        "name": name,
        "text": text
    }
    results = [record for record in graph.run(query, parameters).data()]
    # db_result = graph.run(query, name=name, text=text).data()
    print (results)
    # results = [record for record in db_result]
    # if verbose: print(results)
    # if results['e']["entity_id"] == entity_id:
    #     return True
    # else:
    #     return False



def clean(text):
    text_clean = text \
        .replace("\n", ' ') \
        .replace('; ', '') \
        .replace("\s\s+", " ") \
        .strip()
    text_clean = re.sub(r'(\(([^\)])+\))\W?', '', text)
    return text_clean


questions = []
def inquire_list(message, list):
    questions = [
        inquirer.List(
            "list",
            message=message,
            choices=list,
        ),
    ]
    return inquirer.prompt(questions, theme=GreenPassion())


def inquire_text(message, default):
    questions = [
        inquirer.Text('text',
                      message=message,
                      default=default)
    ]
    return inquirer.prompt(questions, theme=GreenPassion())


def inquire_editor(message, text):
    questions = [
        inquirer.Editor('editor', message=message, default=text)
    ]
    return inquirer.prompt(questions)

def get_wikipedia_summary(entity_name):
    entries = wikipedia.search(entity_name)
    description = ''
    if verbose: print(entries)

    if len(entries) > 1:
        answers = inquire_list("Please choose entry:", entries)
        entry_name = str(answers["list"])
    else:
        entry_name = entity_name
    descriptions = []
    if verbose: print(eye, f"fetching summary for {entry_name}")
    try:
        descriptions = wikipedia.summary(entry_name, sentences=2).split("\n")
    except:
        print (cross, f"Could not fetch description for {entry_name}\n")
        # The suggest() method returns suggestions related to the search query entered as a parameter to it, or it will return "None" if no suggestions were found.
        # > use for entity disambiguation
        # suggestion = wikipedia.suggest(entity_name)
        # if verbose: print(suggestion)
        # answer = inquire_text("Correct entity name:", suggestion)

        failed_requests.append(entity)
    finally:
        if len(descriptions) >= 1:
            description = clean(descriptions[0])
        if description:
            return description

def inquire_submit_text(variable):
    answers = ['yes', 'edit', 'delete']
    answer = "no"
    while answer == "no":
        print (variable)
        answer = inquire_list("Submit?", answers)["list"]
    if answer == "edit":
        variable = inquire_editor("", variable)["editor"]
        inquire_submit_text(variable)
    if answer == "delete":
        variable = ''
    return variable


entities = load_entities_from_database()
# entities = [
#     {
#         "name": "Fred Ritchin",
#         "text": ""
#     },
#     {
#         "name": "George Bush",
#         "text": ""
#     },
#     {
#         "name": "John Coltrane",
#         "text": ""
#     }
# ]


for entity in entities:
    entity_name = entity["name"]
    entity_id = entity["entity_id"]
    print (f"Processing {entity_name} {entity_id}")
    # if entity["text"] == '':
    description = get_wikipedia_summary(entity_name)
    print(f"Summary for {entity_name}:")
    entity["text"] = inquire_submit_text(description)
    print (checkmark, "Updating entity")
    if save_entity_to_database(entity):
        print (checkmark, database, "Successfully updated entity.")


print(cross, f"\nThe following entities had request issues:")
for entity in failed_requests:
    entity_name = entity["name"]
    print (f"[red]{entity_name}[/red]")