#!/usr/bin/env python3

version_number = '0.3'

from configuration.ecchr import matched_entities

import sys
import argparse
import json
import hashlib
import time
import requests
import os.path
from pathlib import Path
from rich.progress import *
from rich import print
from fuzzy_match import algorithims as algorithms
from fuzzy_match import match

import logging

accepted_entities = []
rejected_entities = []
ambiguous_entities = []

recognized_tasks = [
    'extract_entities',
    'store_entities',
    'generate_summaries',
    'find_similar_chunks',
    'store_chunks'
]

argument_parser = argparse.ArgumentParser(
    description='Processes a metasphere collection.json and pushes it into the graph database'
)

argument_parser.add_argument('collection',
                             help='path to metasphere collection.json to process'
                             )
argument_parser.add_argument('-n', '--collection-name',
                             help='name of the collection (overwrites name specified in input file)'
                             )
argument_parser.add_argument('-s', '--start-chunk',
                             help='start processing at chunk', type=int, default=1
                             )
argument_parser.add_argument('-e', '--end-chunk',
                             help='end processing at chunk', type=int
                             )
argument_parser.add_argument('-o', '--output-file',
                             help='write collection.json to output file'
                             )
argument_parser.add_argument('-m', '--media-directory',
                             help='base location of media files',
                             default="files"
                             )
argument_parser.add_argument('--skip-media-check', action='store_true',
                             help='skip filesystem check for associated media files',
                             default=False
                             )
argument_parser.add_argument('--api-address',
                             help='url of the metasphere api to connect to',
                             default='http://ecchr.metasphere.xyz:2342'
                             )
argument_parser.add_argument('--task', type=str, action='append',
                             help=f"task to execute {recognized_tasks}",
                             default=[]
                             )
argument_parser.add_argument('--dry-run',
                             help='do not write to graph database',
                             action="store_true", default=True
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
api_base_url = vars(arguments)['api_address']
media_directory = vars(arguments)['media_directory']
collection_name = vars(arguments)['collection_name']
output_file = vars(arguments)['output_file']
start_chunk = vars(arguments)['start_chunk']
end_chunk = vars(arguments)['end_chunk']
skip_media_check = vars(arguments)['skip_media_check']
verbose = vars(arguments)['verbose']
very_verbose = vars(arguments)['vv']
dry_run = vars(arguments)['dry_run']

tasks = vars(arguments)['task']

timeout_for_reconnect = 15
max_reconnect_tries = 5
failed_inserts_chunks = []
failed_inserts_entities = []
extracted_entities = []
num_tasks = int(len(tasks))

if very_verbose:
    verbose = True

checkmark = f"[green]"u'\u2713 '
cross = f"[red]"u'\u00D7 '
eye = f"[white]"u'\u2022 '
arrow = f"[grey]"u'\u21B3 '

if very_verbose:
    print (arguments)

input_file = arguments.collection

print ("Loading " + input_file + ' ... ', end='')
try:
    with open(input_file) as f:
        collection = json.load(f)
    print("done.")
except (OSError, IOError) as error:
    raise_error(error)

parent_dir = os.path.dirname(sys.argv[0]) + '/../'
base_dir = os.path.abspath(parent_dir)
collection_base_dir = os.path.abspath(base_dir) + '/files' + collection["source_path"]

if not collection_name:
    collection_name = collection['name']

print(f"\nCollection name:\t [bold]{collection_name}")
print(f"Collection id:\t\t {collection['collection_id']}")
print(f"Collection type:\t {collection['source_type']}")
print (f"Base directory:\t\t {base_dir}")
print(f"Media directory:\t {collection_base_dir}")
print(f"Chunks in collection:\t {collection['num_chunks']}\n")

def setup_logger(name, log_file, format='%(message)s'):
    if very_verbose:
        print(f"Setting up logfile: {log_file}")
    if os.path.exists(os.path.dirname(log_file)) == False:
        print (cross, f"Directory does not exist: {os.path.dirname(log_file)}")
        try:
            os.mkdir(os.path.dirname(log_file))
        except OSError as error:
            print (f"Creation of the directory {os.path.dirname(log_file)} failed: {error}")
            sys.exit(1)
        else:
            if very_verbose: print (checkmark, f"Successfully created the directory {os.path.dirname(log_file)}")
    else:
        if very_verbose: print(checkmark, f"Loaded directory {os.path.dirname(log_file)}")

    formatter = logging.Formatter(format)
    try:
        handler = logging.FileHandler(log_file, mode='w')
    except OSError as error:
        print (f"Could not open logfile: {error}")
        sys.exit(1)
    else:
        if verbose: print (checkmark, f"Successfully opened logfile for {name}")
        handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.addHandler(handler)
        return logger

path_accepted_entities = collection_base_dir + '/entities/' + 'accepted.json'
path_rejected_entities = collection_base_dir + '/entities/' + 'rejected.json'
path_ambiguous_entities = collection_base_dir + '/entities/' + 'ambiguous.json'

accepted_entity_logger = setup_logger('accepted_entity_logger',
                                      path_accepted_entities,
                                      format='%(message)s'
                                      )
rejected_entity_logger = setup_logger('rejected_entity_logger',
                                      path_rejected_entities,
                                      format='%(message)s'
                                      )
ambiguous_entity_logger = setup_logger('ambiguous_entity_logger',
                                      path_ambiguous_entities,
                                      format='%(message)s'
                                      )



error_logger = setup_logger('error_logger', collection_base_dir + "/error.log", format='%(message)s')

def removeDuplicateDictFromList(list):
    return [dict(t) for t in {tuple(d.items()) for d in list}]


def sortDictByValues(dict):
    return {k: v for k, v in sorted(dict.items(), key=lambda item: item[1])}

def update_task(current_task):
    progress.update(task_progress, visible=True, description=f"Task: {current_task}")


def update_progress(chunk):
    progress.update(chunk_progress, description=f"Chunk [bold]#{chunk+1}[/bold] / {num_chunks} ")

    progress.reset(task_progress)
    progress.update(task_progress, visible=True)

    progress.reset(request_progress)
    progress.update(request_progress, visible=True)


def raise_error(error):
    if(error):
        print("Error: " + str(error))
        argument_parser.print_help()
    sys.exit(1)


def generate_summaries(text):
    update_task('Generating summaries')
    endpoint = '/text/summarize'
    query = {
        "text": text,
        "aim": 30,
        "deviation": 20,
        "num_summaries": 3
    }
    response = request(endpoint, query)
    num_summaries = len(response["summaries"])
    if num_summaries >= 1:
        chunk_summaries = response["summaries"]
        progress.console.print(checkmark, f"Successfully extracted {num_summaries} summaries")
    else:
        progress.console.print(cross, f"No summaries created.")
        chunk_summaries = ''
    progress.advance(task_progress)
    return chunk_summaries


def extract_entities(chunk, *adjacent_chunk_texts):
    update_task('Extracting entities')
    endpoint = '/text/extract/entities'
    query = {
        "text": chunk["text"]
    }
    response = request(endpoint, query)
    num_entities = len(response["entities"])
    processed_chunk_entities = []
    resolved_coreferences = 0
    if num_entities >= 1:
        chunk_entities = response["entities"]
        progress.console.print(checkmark, f"Successfully extracted {num_entities} entities")
        if very_verbose:
            progress.console.print(chunk_entities)
        if very_verbose:
            progress.console.print(eye, f"Resolving coreferences...")
        adjacent_chunk_entities = []
        adjacent_chunk_entities_as_string = ""
        for adjacent_chunk_text in adjacent_chunk_texts:
            # extract entities from adjacent chunks and store them in a list
            query = {
                "text": adjacent_chunk_text
            }
            response_adjacent_chunk_entities = request(endpoint, query)
            for entity in response_adjacent_chunk_entities["entities"]:
                adjacent_chunk_entities.append({
                    "entity_name": entity["entity_name"],
                    "entity_label": entity["entity_label"]
                })
                adjacent_chunk_entities_as_string += " " + entity["entity_name"]

        for entity in chunk_entities:
            adjacent_chunk_entities_as_string += " " + entity["entity_name"]

        for entity in chunk_entities:
            entity_name = entity["entity_name"]
            updated_entity = {
                "entity_name": "",
                "entity_label": ""
            }

            # compare entity to other chunks
            selected_entity = match.extractOne(entity_name, adjacent_chunk_entities, score_cutoff=0.2)
            if selected_entity != None:
                # similar entity appears in other chunks
                ambiguous_entity = selected_entity[0]
                if entity != ambiguous_entity:
                    # entry is similar but not the same
                    # > count occurances
                    ambiguous_entities.append(entity)
                    ambiguous_entities.append(ambiguous_entity)

                    occurances_ambiguous_entity = adjacent_chunk_entities_as_string.count(ambiguous_entity["entity_name"])
                    occurances_entity = adjacent_chunk_entities_as_string.count(entity["entity_name"])
                    if occurances_ambiguous_entity > occurances_entity:
                        # > select entity with higher occurances
                        selected_entity_name = ambiguous_entity["entity_name"]
                        updated_entity = {
                            "entity_name": ambiguous_entity["entity_name"],
                            "entity_label": ambiguous_entity["entity_label"]
                        }
                        if verbose: progress.console.print(checkmark, f"resolved coreference: [white]{entity_name}[/white] \u2192 [bold]{selected_entity_name}[/bold].")
                        processed_chunk_entities.append(updated_entity)
                        resolved_coreferences += 1
                    else:
                        processed_chunk_entities.append(entity)
                else:
                    # entities are the same
                    processed_chunk_entities.append(entity)
            else:
                # entity is unambiguous
                # > accept
                processed_chunk_entities.append(entity)

            for (entity_index, entity) in enumerate(processed_chunk_entities):
                if entity["entity_name"] in matched_entities:
                    # entity is in configuration
                    # > select configuration value > accept
                    updated_entity = {
                        "entity_name": matched_entities[entity["entity_name"]],
                        "entity_label": 'HARDCODED'
                    }
                    selected_entity_name = matched_entities[entity["entity_name"]]
                    if verbose: progress.console.print(checkmark, f"Resolved coreference from configuration: [white]{entity_name}[/white] \u2192 [bold]{selected_entity_name}[/bold].")
                    resolved_coreferences += 1
                    processed_chunk_entities[entity_index] = updated_entity

        if resolved_coreferences >= 1:
            progress.console.print(checkmark, f"Resolved {resolved_coreferences} coreferences.")

        for entity in removeDuplicateDictFromList(processed_chunk_entities):
            accepted_entities.append(entity)
            progress.console.print(eye, entity["entity_name"] + ':', entity["entity_label"])

        if verbose:
            progress.console.print(removeDuplicateDictFromList(processed_chunk_entities), highlight=True)
    else:
        progress.console.print(cross, f"No entities found.")

    progress.advance(task_progress)
    return removeDuplicateDictFromList(processed_chunk_entities)


def find_similar_chunks(chunk):
    update_task('Finding similar chunks')
    progress.console.print(f"Finding similar chunks.")
    endpoint = '/text/similarities'
    query = {
        "text": chunk["text"],
        "minimum_score": 80,
        "chunks": 5
    }
    response = request(endpoint, query)
    if len(response["similar_chunks"]) >= 1:
        similar_chunks = response["similar_chunks"]
        progress.console.print(checkmark)
    else:
        progress.console.print(cross, f"[red]No similar chunks found.")
        similar_chunks = ''
    progress.advance(task_progress)
    return similar_chunks


def request(endpoint, query):
    progress.update(reconnect_progress, visible=False)

    url = api_base_url + endpoint
    if very_verbose:
        progress.console.print(arrow, f'Sending request to {url}')
    progress.advance(request_progress)

    while True:
        try:
            response = requests.post(
                url,
                data=json.dumps(query),
                headers={
                    'Content-type': 'application/json'
                }
            )
            if response.status_code == requests.codes.ok:
                if very_verbose:
                    progress.console.print(checkmark, f'Request successful: {response.status_code}')
                if very_verbose:
                    progress.console.print(response.json(), highlight=False)
                return response.json()
            else:
                if verbose:
                    progress.console.print(cross)
                raise_error(f"Error {response.status_code}")
                progress.console.print(cross, f"[red]Failed at chunk {chunk}")
        except (
            requests.exceptions.RequestException
        ) as e:
            failed_inserts_chunks.append(query)
            if verbose:
                progress.console.print(cross, f'[red]Error sending request')
            if very_verbose:
                progress.console.print('\n[red]', e)
            if verbose:
                progress.console.print('\nReconnecting.')
            progress.reset(reconnect_progress)
            progress.update(reconnect_progress, visible=True)
            reconnect_tries = 0
            for seconds in range(timeout_for_reconnect):
                time.sleep(1)
                reconnect_tries += 1
                progress.advance(reconnect_progress)
            # request(endpoint, query)
            if reconnect_tries <= max_reconnect_tries:
                failed_inserts_chunks.remove(query)
                continue
            else:
                progress.console.print(f"\n[red bold]API is not responding. Aborting.")
                dump_failed_inserts()
                sys.exit(2)
        else:
            break


def check_media(chunk):
    if collection["source_type"] == "audio":
        source_file = chunk["source_file"]
        source_file_path = '/'.join([
            collection_base_dir,
            'mp3/Chunks',
            chunk["source_file"]
        ])
        if very_verbose:
            progress.console.print(arrow, f'Checking for associated audio file: {source_file}')
        try:
            f = open(source_file_path)
            progress.console.print(checkmark, f"Associated audio file exists: {source_file} ")
        except IOError:
            progress.console.print(cross, f"Associated audio file not found: {source_file} ")
            progress.console.print(f"\n[red bold]Aborting.")
            dump_failed_inserts()
            sys.exit(2)
        finally:
            f.close()


def insert_chunk_into_database(chunk):
    update_task('Uploading chunks')

    endpoint = '/graph/find/chunk'
    query = {"id": chunk_id}
    response = request(endpoint, query)

    if response["status"] == "failed":
        # chunk does not exist in database, so we continue to process
        if not skip_media_check:
            check_media(chunk)
        if verbose:
            progress.console.print(f"Inserting chunk into database...")
        if very_verbose:
            progress.console.print(chunk)

        endpoint = '/graph/add/chunk'
        if not dry_run:
            response = request(endpoint, chunk)
            if response["status"] == "success":
                progress.console.print(checkmark, f"Successfully inserted chunk.")
                failed_inserts_chunks.remove(chunk)
            else:
                progress.console.print(cross, f"[red]Error inserting chunk.")
        else:
            if verbose:
                progress.console.print(f"[black on #FF9900]\nDry-run. Skipping database update.\n")
            failed_inserts_chunks.remove(chunk)
        progress.advance(request_progress)

    elif response["status"] == "success":
        if verbose:
            progress.console.print(eye, f"[white]Chunk already exists.")
        stored_chunk = response["instance"]
        if stored_chunk != chunk:
            if verbose:
                progress.console.print(cross, f"[white]Updating chunk...")
            endpoint = '/graph/update/chunk'
            query = chunk
            if not dry_run:
                response_update = request(endpoint, query)
                if response_update["status"] == "success":
                    progress.console.print(checkmark, f"Successfully updated chunk.")
                else:
                    progress.console.print(cross, f"[red]Error updating chunk.")


def insert_entities_into_database(entities, chunk):
    # TODO:
    # filesystem read entities-accepted.json into list of dicts
    # iterate through entities
    # if entity in accepted_entities:
    # > upload entity
    # if not:
    # > add to rejected log

    try:
        with open(input_file) as f:
            collection = json.load(f)
        print("done.")
    except (OSError, IOError) as error:
        raise_error(error)

    for (entity, entity_type) in entities.items():
        endpoint = '/graph/find/entity'
        query = {
            "name": entity
        }
        response = request(endpoint, query)
        if response["status"] == "success":
            progress.console.print(eye, f"Entity already exists: [bold]{entity}[/bold]")
        else:
            if verbose:
                progress.console.print(f"Inserting entity into database: [bold]{entity}[/bold] ({entity_type})")
            hash = hashlib.md5(entity.encode("utf-8"))
            entity_id = hash.hexdigest()

            endpoint = '/graph/add/entity'
            query = {
                "name": entity,
                "entity_category": entity_type,
                "entity_id": entity_id,
                "chunk_id": chunk["chunk_id"]
            }
            failed_inserts_entities.append(query)
            if very_verbose:
                progress.console.print(f"{query}")
            if not dry_run:
                response = request(endpoint, query)
                if response["status"] == "success":
                    if verbose:
                        progress.console.print(checkmark, f"Successfully inserted entity: [bold]{entity}[/bold] ({entity_type})")
                    failed_inserts_entities.remove(query)
                else:
                    progress.console.print(cross, f"Could not insert entity into database.")
            else:
                if verbose:
                    progress.console.print(f"[black on #FF9900]\nDry-run. Skipping database update.\n")
            connect_entity_to_chunk(query, chunk)


def connect_entity_to_chunk(entity, chunk):
    if very_verbose:
        progress.console.print(f"Connecting entity {entity['name']}.")
    endpoint = '/graph/find/entity'
    entity_name = entity['name']
    query = {
        "name": entity_name
    }
    response = request(endpoint, query)
    if response["status"] == "success":
        if verbose:
            if verbose:
                progress.console.print(checkmark, f"Entity found. Connecting to chunk.")
            endpoint = '/graph/connect/entity'
            query = {
                "connect": entity["entity_id"],
                "with": {
                    "id": chunk["chunk_id"]
                }
            }
            if very_verbose:
                progress.console.print(f"{query}")
            if not dry_run:
                response = request(endpoint, query)
                if response["status"] == "success":
                    if verbose:
                        progress.console.print(checkmark, f"Successfully connected entity: [bold]{entity_name}[/bold]")
                else:
                    progress.console.print(cross, f"Could not connect entity (request error): [bold]{entity_name}[/bold]")
            else:
                if verbose:
                    progress.console.print(f"[black on #FF9900]\nDry-run. Skipping database update.\n")
    else:
        progress.console.print(cross, f"Could not find entity: [bold]{entity_name}[/bold]")


def dump_failed_inserts():
    if very_verbose:
        progress.console.print(f"[red]Insertion failed for the following objects:\n")
    for chunk in failed_inserts_chunks:
        if very_verbose:
            progress.console.print(f"{chunk}\n")
        error_logger.error(f"\n{chunk}\n")
    for entity in failed_inserts_entities:
        if very_verbose:
            progress.console.print(f"{entity}\n")
        error_logger.error(f"\n{entity}\n")
    progress.console.print(f"[red bold]Saved failed insertions to error.log\n")


if end_chunk:
    num_chunks = end_chunk - start_chunk +1
else:
    num_chunks = len(collection["chunk_sequence"])

with Progress(
    SpinnerColumn(),
    "[progress.percentage]{task.percentage:>3.0f}%",
    TimeElapsedColumn(),
    TimeRemainingColumn(),
    BarColumn(),
    "[progress.description]{task.description}",
) as progress:
    progress.console.print(f"Chunks to process:\t {num_chunks}")

    chunk_progress = progress.add_task(f"Chunk {start_chunk-1} / {end_chunk} \t\t", total=num_chunks)

    task_progress = progress.add_task(f"Task: {current_task}\t\t", total=num_tasks, visible=False)
    request_progress = progress.add_task(f"API requests\t\t", total=(num_tasks * 2), visible=False)
    reconnect_progress = progress.add_task(
        "[red]Waiting to reconnect... \t\t", total=timeout_for_reconnect, visible=False)

    updated_chunk_sequence = []

    if not end_chunk:
        end_chunk = num_chunks

    for task_number, task in enumerate(tasks, start=1):
        progress.console.print(f'\n[black on #FF9900][bold]Starting task {task_number}[/bold]: {task}')
        update_task(task)

        if task not in recognized_tasks:
            raise_error(f"Task not recognized: {task}")
        else:
            updated_chunk_index = 0
            for chunk_number in range(start_chunk, end_chunk + 1):
                chunk_index = chunk_number - 1
                update_progress(chunk_index)

                chunk = collection["chunk_sequence"][chunk_index]
                previous_chunk = collection["chunk_sequence"][chunk_index - 1]
                chunk_id = chunk["chunk_id"]
                progress.console.print(f'\n[bold]Processing chunk {chunk_number}[/bold]:\t {chunk_id}')
                if verbose: progress.console.print(f'\n{chunk["text"]}\n')

                updated_chunk = {
                    "chunk_id": chunk["chunk_id"],
                    "text": chunk["text"],
                    "source_file": chunk["source_file"],
                    "start_time": chunk["start_time"],
                    "end_time": chunk["end_time"]
                }

                if task_number == 1:
                    updated_chunk_sequence.append(updated_chunk)
                    failed_inserts_chunks.append(updated_chunk)

                if task == 'extract_entities':
                    if chunk_index < (num_chunks - 3):
                        chunk_entities = extract_entities(
                            chunk,
                            collection["chunk_sequence"][chunk_index - 1]["text"],
                            collection["chunk_sequence"][chunk_index - 2]["text"],
                            collection["chunk_sequence"][chunk_index - 3]["text"],
                            collection["chunk_sequence"][chunk_index + 1]["text"],
                            collection["chunk_sequence"][chunk_index + 2]["text"],
                            collection["chunk_sequence"][chunk_index + 3]["text"]
                        )
                    else:
                        chunk_entities = extract_entities(
                        chunk,
                        collection["chunk_sequence"][chunk_index - 1]["text"],
                        collection["chunk_sequence"][chunk_index - 2]["text"],
                        collection["chunk_sequence"][chunk_index - 3]["text"],
                        )

                    updated_chunk.update(entities=chunk_entities)
                elif task == 'store_entities':
                    insert_entities_into_database()
                elif task == 'generate_summaries':
                    chunk_summaries = generate_summaries(chunk['text'])
                    updated_chunk.update(summaries=chunk_summaries)
                elif task == 'find_similar_chunks':
                    similar_chunks = find_similar_chunks(chunk)
                    updated_chunk.update(similarity=similar_chunks)

                updated_chunk_sequence[updated_chunk_index].update(updated_chunk)
                failed_inserts_chunks[updated_chunk_index].update(updated_chunk)
                updated_chunk_index += 1
                progress.advance(chunk_progress)

                if chunk_number != end_chunk:
                    progress.update(chunk_progress, advance=1)

            if task == 'store_chunks':
                for i, chunk in enumerate(updated_chunk_sequence, start=1):
                    # insert_chunk_into_database(chunk)
                    # connect_chunk_to_collection(chunk)
                    time.sleep(1)
            elif task == 'extract_entities':
                for entity in removeDuplicateDictFromList(accepted_entities):
                    accepted_entity_logger.warning(entity)
                for entity in removeDuplicateDictFromList(rejected_entities):
                    rejected_entity_logger.warning(entity)
                for index, entity in enumerate(ambiguous_entities):
                    if (index % 2 == 0) and (index != 0):
                        ambiguous_entity_logger.warning("\n")
                    ambiguous_entity_logger.warning(entity)


            if task_number == num_tasks:
                progress.update(task_progress, visible=False)
                progress.update(request_progress, visible=False)
                progress.update(reconnect_progress, visible=False)
                progress.update(chunk_progress, visible=False)
                progress.console.print(f"\n\n[bold white]Done processing.")
                if len(failed_inserts_chunks) > 1:
                    dump_failed_inserts()
                    sys.exit(1)
                else:
                    progress.console.print(f"{checkmark}No errors encountered.")
                    sys.exit(0)


#
# Extracts additional information for a collection of chunks and writes it to the graph database.
# Queries: collection.json
# Updates/inserts into: graph database
#
# Foreach chunk in chunk_sequence (collection.json):
# Post to /api/graph/find/chunk
# If chunk !exists:
# Post to /api/graph/add/chunk
# Post to /api/text/extract/entities
# Foreach entity in response:
# Post to /api/graph/find/entity
# If entity !exists:
# Post to /api/graph/add/entity
# Post entity, chunk to /api/graph/connect/entity
# Post to /api/text/summarize/short
# Post response, chunk to /api/graph/add/summary/short
# Post to /api/text/summarize/medium
# Post response, chunk to /api/graph/add/summary/medium
# Run similarity detection for chunk (tbd)
# Foreach similar chunk:
# Post chunk, similar_chunk, similarity to /api/graph/connect/chunks
# If chunk exists:
# Update graph database
