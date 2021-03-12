#!/usr/bin/env python3

import sys
import argparse
import json
import hashlib
import time
import requests
from rich.progress import *
from rich import print

checkmark = f"[green]"u'\u2713 '
cross = f"[red]"u'\u00D7 '
arrow = f"[grey]"u'\u21B3 '

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
argument_parser.add_argument('-m', '--media-directory',
    help='base location of media files',
    default="files"
)
argument_parser.add_argument('-a', '--api-address',
    help='url of the metasphere api to connect to',
    default='http://ecchr.metasphere.xyz:2342'
)
argument_parser.add_argument('-d', '--dry-run',
    help='do not write to graph database, show verbose output only',
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
    version='%(prog)s 0.1'
)

arguments = argument_parser.parse_args()

current_task = 'Starting'
api_base_url = vars(arguments)['api_address']
media_directory = vars(arguments)['media_directory']
collection_name = vars(arguments)['collection_name']
start_chunk = vars(arguments)['start_chunk']
end_chunk = vars(arguments)['end_chunk']
verbose = vars(arguments)['verbose']
very_verbose = vars(arguments)['vv']
dry_run = vars(arguments)['dry_run']
timeout_for_reconnect = 15
num_tasks = 2

if very_verbose:
    verbose = True

if verbose: print (arguments)


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


def extract_summaries(text):
    update_task('Extracting summaries')
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


def extract_entities(text):
    update_task('Extracting entities')
    endpoint = '/text/extract/entities'
    query = {
        "text": text
    }
    response = request(endpoint, query)
    num_entities = len(response["entities"])
    if num_entities >= 1:
        chunk_entities = response["entities"]
        progress.console.print(checkmark, f"Successfully extracted {num_entities} entities")
    else:
        progress.console.print(cross, f"No entities found.")
        chunk_entities = ''
    progress.advance(task_progress)
    return chunk_entities


def find_similar_chunks(text):
    update_task('Finding similar chunks')
    progress.console.print(f"Finding similar chunks.")
    endpoint = '/text/similarities'
    query = {
        "text": data["text"],
        "minimum_score": 20,
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
            if verbose: progress.console.print(checkmark, f'Request successful: {response.status_code}')
            if very_verbose: progress.console.print(response.json(), highlight=False)
        except (
            requests.exceptions.RequestException
        ) as e:
            if verbose: progress.console.print(cross, f'[red]Error sending request')
            if very_verbose: progress.console.print('\n[red]', e)
            if verbose: progress.console.print('\nReconnecting.')
            progress.reset(reconnect_progress)
            progress.update(reconnect_progress, visible=True)
            for seconds in range(timeout_for_reconnect):
                time.sleep(1)
                progress.advance(reconnect_progress)
            # request(endpoint, query)
            continue
        else:
            break

    time.sleep(0.2)
    if response.status_code == requests.codes.ok:
        return response.json()
    else:
        if verbose: progress.console.print(cross)
        raise_error(f"Error {response.status_code}")
        progress.console.print(cross, f"[red]Failed at chunk {chunk}")

input_file = arguments.collection

print ("Loading " + input_file + ' ... ', end='')
try:
    with open(input_file) as f:
        collection = json.load(f)
    print("done.")
except (OSError, IOError) as error:
    raise_error(error)

num_chunks = len(collection["chunk_sequence"])


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

    collection_base_dir = media_directory + '/'.join([
        collection["source_path"]
    ])

    if very_verbose: progress.console.print(f"Media directory:\t {collection_base_dir}")

    chunk_progress = progress.add_task(f"Chunk {start_chunk-1} / {end_chunk} \t\t", total=num_chunks)

    task_progress = progress.add_task(f"Task: {current_task}\t\t", total=num_tasks, visible=False)
    request_progress = progress.add_task(f"API requests\t\t", total=(num_tasks * 2), visible=False)
    reconnect_progress = progress.add_task("[red]Waiting to reconnect... \t\t", total=timeout_for_reconnect, visible=False)

    if not end_chunk:
        end_chunk = num_chunks
    for chunk in range((start_chunk-1), end_chunk):
        update_progress(chunk)

        data = collection["chunk_sequence"][chunk]
        chunk_id = data["chunk_id"]
        progress.console.print(f'\n[bold]Processing chunk {chunk}[/bold]:\t {chunk_id}')

        endpoint = '/graph/find/chunk'
        query = {"id": chunk_id}
        response = request(endpoint, query)

        if response["status"] == "failed":
            if collection["source_type"] == "audio":
                source_file = data["source_file"]
                source_file_path = '/'.join([
                    collection_base_dir,
                    'mp3/Chunks',
                    data["source_file"]
                ])
                if very_verbose: progress.console.print(arrow, f'Checking for associated audio file: {source_file}')
                try:
                    f = open(source_file_path)
                    progress.console.print(checkmark, f"Associated audio file exists: {source_file} ")
                except IOError:
                    progress.console.print(cross, f"Associated audio file not found: {source_file} ")
                    progress.console.print(f"\n[red bold]Aborting.")
                    sys.exit(2)
                finally:
                    f.close()

            chunk_entities = extract_entities(data['text'])
            # chunk_summaries = extract_summaries(data['text'])
            # similar_chunks = find_similar_chunks(data['text'])
            chunk_summaries = ''
            similar_chunks = ''

            if verbose: progress.console.print(f"Inserting chunk into database...")
            endpoint = '/graph/add/chunk'
            query = {
                'chunk_id': data["chunk_id"],
                'text': data["text"],
                'source_file': data["source_file"],
                'start_time': data["start_time"],
                'end_time': data["end_time"],
                'summaries': [chunk_summaries],
                'entities': [chunk_entities],
                'similarity': [similar_chunks]
            }
            if verbose: progress.console.print(query, highlight=True)

            if not dry_run:
                response = request(endpoint, query)
            else:
                response["status"] = query
            progress.advance(request_progress)

            if response["status"] != "failed":
                progress.console.print(checkmark, f"Successfully inserted chunk.")
            else:
                progress.console.print(cross, f"[red]Error inserting chunk.")
        else:
            progress.console.print(cross, f"[white]Chunk already exists. Skipping.")

        progress.advance(chunk_progress)


        if chunk != end_chunk:
            progress.update(chunk_progress, advance=1)
        else:
            progress.update(task_progress, visible=False)
            progress.update(request_progress, visible=False)
            progress.update(reconnect_progress, visible=False)
            progress.console.print(checkmark, f"\n\n[bold]Done processing.")
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