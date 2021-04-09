#!/usr/bin/env python3

import sys
import logging
import argparse
import json
import hashlib
import time
import requests
from rich.progress import *
from rich import print

version_number = '0.2'

recognized_tasks = [
    'extract_entities',
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
    version="%(prog)s "+version_number
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
tasks.append('store_chunks')

timeout_for_reconnect = 15
max_reconnect_tries = 5
request_queue = []
num_tasks = int(len(tasks))

if start_chunk and end_chunk > 1:
    num_chunks = end_chunk - start_chunk + 1
else:
    num_chunks = len(collection["chunk_sequence"])

if very_verbose:
    verbose = True

checkmark = f"[green]"u'\u2713 '
cross = f"[red]"u'\u00D7 '
arrow = f"[grey]"u'\u21B3 '

if very_verbose: print (arguments)


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
    processed_chunk_entities = {}
    if num_entities >= 1:
        chunk_entities = response["entities"]
        processed_chunk_entities = chunk_entities.copy()
        progress.console.print(checkmark, f"Successfully extracted {num_entities} entities")
        if verbose: progress.console.print(f"Resolving coreferences...")
        resolved_coreferences = 0
        adjacent_chunk_entities = []
        for adjacent_chunk_text in adjacent_chunk_texts:
            query = {
                "text": adjacent_chunk_text
            }
            response_adjacent_chunk_entities = request(endpoint, query)
            adjacent_chunk_entities.append(response_adjacent_chunk_entities["entities"])
        for (entity, entity_type) in chunk_entities.items():
            for (index, adjacent_chunk) in enumerate(adjacent_chunk_entities):
                for (adjacent_entity, adjacent_entity_type) in adjacent_chunk.items():
                    if entity in adjacent_entity:
                        if len(adjacent_entity) > len(entity):
                            selected_entity = adjacent_entity
                            selected_entity_type = adjacent_entity_type
                            del processed_chunk_entities[entity]
                        else:
                            selected_entity = entity
                            selected_entity_type = entity_type
                        if verbose: progress.console.print(f"Entity found in adjacent chunk: {entity} >>> {adjacent_entity}. \nSelecting {selected_entity} ({selected_entity_type}).")
                        processed_chunk_entities.update({
                            selected_entity: selected_entity_type
                        })
                        resolved_coreferences += 1
        if verbose: progress.console.print(checkmark, f"Successfully resolved {resolved_coreferences} coreferences.")
        if verbose: progress.console.print(f"Extracted entities:\n {processed_chunk_entities}")
    else:
        progress.console.print(cross, f"No entities found.")
    progress.advance(task_progress)
    return processed_chunk_entities


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
    if verbose: progress.console.print(arrow, f'Sending request to {url}')
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
                if verbose: progress.console.print(checkmark, f'Request successful: {response.status_code}')
                if very_verbose: progress.console.print(response.json(), highlight=False)
                return response.json()
            else:
                if verbose: progress.console.print(cross)
                raise_error(f"Error {response.status_code}")
                progress.console.print(cross, f"[red]Failed at chunk {chunk}")
        except (
            requests.exceptions.RequestException
        ) as e:
            request_queue.append(query)
            if verbose: progress.console.print(cross, f'[red]Error sending request')
            if very_verbose: progress.console.print('\n[red]', e)
            if verbose: progress.console.print('\nReconnecting.')
            progress.reset(reconnect_progress)
            progress.update(reconnect_progress, visible=True)
            reconnect_tries = 0
            for seconds in range(timeout_for_reconnect):
                time.sleep(1)
                reconnect_tries += 1
                progress.advance(reconnect_progress)
            # request(endpoint, query)
            if reconnect_tries <= max_reconnect_tries:
                request_queue.remove(query)
                continue
            else:
                progress.console.print(f"\n[red bold]API is not responding. Aborting.")
                dump_request_queue()
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
        if very_verbose: progress.console.print(arrow, f'Checking for associated audio file: {source_file}')
        try:
            f = open(source_file_path)
            progress.console.print(checkmark, f"Associated audio file exists: {source_file} ")
        except IOError:
            progress.console.print(cross, f"Associated audio file not found: {source_file} ")
            progress.console.print(f"\n[red bold]Aborting.")
            dump_request_queue()
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
        if verbose: progress.console.print(f"Inserting chunk into database...")
        if very_verbose: progress.console.print(chunk)

        endpoint = '/graph/add/chunk'
        if not dry_run:
            response = request(endpoint, chunk)
            if response["status"] != "failed":
                progress.console.print(checkmark, f"Successfully inserted chunk.")
                request_queue.remove(chunk)
            else:
                progress.console.print(cross, f"[red]Error inserting chunk.")
        else:
            progress.console.print(f"[black on #FF9900]\nDry-run. Skipping database update.\n")
            request_queue.remove(chunk)
        progress.advance(request_progress)

    elif response["status"] == "success":
        if verbose: progress.console.print(cross, f"[white]Chunk already exists.")
        stored_chunk = response["instance"]
        if stored_chunk != chunk:
            if verbose: progress.console.print(cross, f"[white]Updating chunk...")
            endpoint = '/graph/update/chunk'
            query = chunk
            if not dry_run:
                response_update = request(endpoint, query)
                if response_update["status"] == "success":
                    progress.console.print(checkmark, f"Successfully updated chunk.")
                else:
                    progress.console.print(cross, f"[red]Error updating chunk.")

def insert_entities_into_database(entities):
    for (entity, entity_type) in entities.items():
        endpoint = '/graph/find/entity'
        query = {
            "name": entity
        }
        response = request(endpoint, query)
        if response["status"] == "success":
            progress.console.print(f"Entity {entity} already exists.")
        else:
            progress.console.print(f"Inserting {entity} into database.")
            hash = hashlib.md5(entity.encode("utf-8"))
            entity_id = hash.hexdigest()

            endpoint = '/graph/add/entity'
            query = {
                "name": entity,
                "entity_category": entity_type,
                "entity_id": entity_id,
                "chunk_id": chunk["chunk_id"]
            }
            if very_verbose:
                progress.console.print(f"{query}")
            if not dry_run:
                response = request(endpoint, query)
            else:
                progress.console.print(f"[black on #FF9900]\nDry-run. Skipping database update.\n")

def dump_request_queue():
    if very_verbose:
        progress.console.print(f"[red]Insertion failed for the following chunks:\n")
    logging.basicConfig(filename='error.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    for chunk in request_queue:
        if very_verbose:
            progress.console.print(f"{chunk}\n")
        logging.error(f"\n{chunk}\n")
    progress.console.print(f"[red bold]Saved failed insertions to error.log\n")


input_file = arguments.collection

print ("Loading " + input_file + ' ... ', end='')
try:
    with open(input_file) as f:
        collection = json.load(f)
    print("done.")
except (OSError, IOError) as error:
    raise_error(error)

with Progress(
    SpinnerColumn(),
    "[progress.percentage]{task.percentage:>3.0f}%",
    TimeElapsedColumn(),
    TimeRemainingColumn(),
    BarColumn(),
    "[progress.description]{task.description}",
) as progress:
    if not collection_name:
        collection_name = collection['name']
    progress.console.print(f"\nCollection name:\t [bold]{collection_name}")
    progress.console.print(f"Collection id:\t\t {collection['collection_id']}")
    progress.console.print(f"Collection type:\t {collection['source_type']}")
    progress.console.print(f"Chunks in collection:\t {collection['num_chunks']}")
    progress.console.print(f"Chunks to process:\t {num_chunks}")

    collection_base_dir = media_directory + '/'.join([
        collection["source_path"]
    ])

    if very_verbose: progress.console.print(f"Media directory:\t {collection_base_dir}")

    chunk_progress = progress.add_task(f"Chunk {start_chunk-1} / {end_chunk} \t\t", total=num_chunks)

    task_progress = progress.add_task(f"Task: {current_task}\t\t", total=num_tasks, visible=False)
    request_progress = progress.add_task(f"API requests\t\t", total=(num_tasks * 2), visible=False)
    reconnect_progress = progress.add_task("[red]Waiting to reconnect... \t\t", total=timeout_for_reconnect, visible=False)


    updated_chunk_sequence = []

    if not end_chunk:
        end_chunk = num_chunks

    for task_number, task in enumerate(tasks, start=1):
        if very_verbose: progress.console.print(f'\n[black on #FF9900][bold]Starting task {task_number}[/bold]:\t {task}')
        update_task(task)

        if task not in recognized_tasks:
            raise_error(f"Task not recognized: {task}")
        else:
            for chunk_number in range(start_chunk, end_chunk+1):
                chunk_index = chunk_number -1
                update_progress(chunk_index)

                chunk = collection["chunk_sequence"][chunk_index]
                previous_chunk = collection["chunk_sequence"][chunk_index-1]
                next_chunk = collection["chunk_sequence"][chunk_index+1]
                chunk_id = chunk["chunk_id"]
                progress.console.print(f'\n[bold]Processing chunk {chunk_number}[/bold]:\t {chunk_id}')

                updated_chunk = {
                    "chunk_id": chunk["chunk_id"],
                    "text": chunk["text"],
                    "source_file": chunk["source_file"],
                    "start_time": chunk["start_time"],
                    "end_time": chunk["end_time"],
                    "entities": [],
                    "summaries": [],
                    "similarity": []
                }

                if task_number == 1:
                    updated_chunk_sequence.append(updated_chunk)
                    request_queue.append(updated_chunk)

                if task == 'extract_entities':
                    chunk_entities = extract_entities(chunk, previous_chunk['text'], next_chunk['text'])
                    updated_chunk.update(entities = chunk_entities)
                    insert_entities_into_database(chunk_entities)
                    print(f"XXXX {updated_chunk}")
                elif task == 'generate_summaries':
                    chunk_summaries = generate_summaries(chunk['text'])
                    updated_chunk.update(summaries = chunk_summaries)
                elif task == 'find_similar_chunks':
                    similar_chunks = find_similar_chunks(chunk)
                    updated_chunk.update(similarity = similar_chunks)

                updated_chunk_sequence[chunk_index].update(updated_chunk)
                request_queue[chunk_index].update(updated_chunk)

                progress.advance(chunk_progress)

                if chunk_number != end_chunk:
                    progress.update(chunk_progress, advance=1)

            if task == 'store_chunks':
                for i, chunk in enumerate(updated_chunk_sequence, start=1):
                    insert_chunk_into_database(chunk)
                    time.sleep(1)

            if task_number == num_tasks:
                progress.update(task_progress, visible=False)
                progress.update(request_progress, visible=False)
                progress.update(reconnect_progress, visible=False)
                progress.update(chunk_progress, visible=False)
                progress.console.print(f"\n\n[bold white]Done processing.")
                if len(request_queue) > 1:
                    dump_request_queue()
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