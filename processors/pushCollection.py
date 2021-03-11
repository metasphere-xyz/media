#!/usr/bin/env python3

import sys
import argparse
import json
import hashlib
import time
import requests
from rich.progress import Progress, track
from rich import print

checkmark = f"[green]"u'\u2713'
cross = f"[red]"u'\u00D7'

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
argument_parser.add_argument('-V', '--version', action='version',
    version='%(prog)s 0.1'
)

arguments = argument_parser.parse_args()


api_base_url = vars(arguments)['api_address']
media_directory = vars(arguments)['media_directory']
collection_name = vars(arguments)['collection_name']
start_chunk = vars(arguments)['start_chunk']
end_chunk = vars(arguments)['end_chunk']
verbose = vars(arguments)['verbose']
dry_run = vars(arguments)['dry_run']
timeout_for_reconnect = 15

if verbose: print (arguments)

def raise_error(error):
    if(error):
        print("Error: " + str(error))
        argument_parser.print_help()
    sys.exit(1)

def request(endpoint, query):
    progress.update(reconnect_progress, visible=False)

    url = api_base_url + endpoint
    if verbose:
        progress.console.print(f'Sending request to {url}')
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
            if verbose: progress.console.print(checkmark)
        except (
            requests.exceptions.RequestException
        ) as e:
            progress.console.print(cross)
            if verbose: progress.console.print(e)
            progress.reset(reconnect_progress)
            progress.update(reconnect_progress, visible=True)
            for seconds in range(timeout_for_reconnect):
                time.sleep(1)
                progress.advance(reconnect_progress)
            # request(endpoint, query)
            continue
        else:
            break

    time.sleep(2)
    if response.status_code == requests.codes.ok:
        return response.json()
    else:
        if verbose: progress.console.print(cross)
        raise_error(f"Error {response.status_code}: {response.text}")

input_file = arguments.collection

print ("Loading " + input_file + ' ... ', end='')
try:
    with open(input_file) as f:
        collection = json.load(f)
    print("done.")
except (OSError, IOError) as error:
    raise_error(error)

# Get collection meta data

# collection["name"]
# collection["collection_id"]
# collection["source_type"]
# collection["source_path"]

num_chunks = len(collection["chunk_sequence"])

with Progress() as progress:
    if not collection_name:
        collection_name = collection['name']
    progress.console.print(f"\nCollection name:\t {collection_name}")
    progress.console.print(f"Collection id:\t\t {collection['collection_id']}")
    progress.console.print(f"Collection type:\t {collection['source_type']}")

    chunk_progress = progress.add_task("Progress \t", total=num_chunks)
    request_progress = progress.add_task("API request \t", total=3)
    reconnect_progress = progress.add_task("[red]Timeout until reconnect \t", total=timeout_for_reconnect, visible=False)

    if not end_chunk:
        end_chunk = num_chunks
    for chunk in range((start_chunk-1), end_chunk):
        progress.reset(request_progress)
        data = collection["chunk_sequence"][chunk]
        chunk_id = data["chunk_id"]
        progress.console.print(f"\n[bold]Processing chunk [regular]#{chunk+1}")
        progress.console.print(f"Chunk id: {chunk_id}")

        endpoint = '/graph/find/chunk'
        query = {"id": chunk_id}
        response = request(endpoint, query)

        if response["status"] == "failed":
            if collection["source_type"] == "audio":
                source_file = data["source_file"]
                source_file_path = '/'.join([
                    media_directory,
                    collection["source_path"],
                    'mp3/Chunks',
                    data["source_file"]
                ])
                try:
                    f = open(source_file_path)
                    progress.console.print(checkmark, f"Associated audio file exists: {source_file} ")
                except IOError:
                    progress.console.print(cross, f"Associated audio file not found: {source_file} ")
                    progress.console.print(f"\n[red bold]Aborting.")
                    sys.exit(2)
                finally:
                    f.close()

            progress.console.print(f"Extracting entities.")
            endpoint = '/text/extract/entities'
            query = {
                "text": data["text"]
            }
            response = request(endpoint, query)
            if verbose: progress.console.print(response)
            if len(response["entities"]) >= 1:
                chunk_entities = response["entities"]
                progress.console.print(checkmark)
            else:
                progress.console.print(cross, f"[red]No entities found.")
                chunk_entities = ''


            progress.console.print(f"Extracting summaries.")
            endpoint = '/text/summarize'
            query = {
                "text": data["text"],
                "aim": 30,
                "deviation": 20,
                "num_summaries": 3
            }
            response = request(endpoint, query)
            if verbose: progress.console.print(response)
            if len(response["summaries"]) >= 1:
                chunk_summaries = response["summaries"]
                progress.console.print(checkmark)
            else:
                progress.console.print(cross, f"[red]No summaries created.")
                chunk_summaries = ''


            progress.console.print(f"Finding similar chunks.")
            endpoint = '/text/similarities'
            query = {
                "text": data["text"],
                "minimum_score": 20,
                "chunks": 5
            }
            response = request(endpoint, query)
            if verbose: progress.console.print(response)
            if len(response["similar_chunks"]) >= 1:
                similar_chunks = response["similar_chunks"]
                progress.console.print(checkmark)
            else:
                progress.console.print(cross, f"[red]No similar chunks found.")
                similar_chunks = ''


            if verbose: progress.console.print(f"Inserting chunk into database.")
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
            if verbose: progress.console.print(query)

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

    while not progress.finished:
        progress.update(chunk_progress, advance=0.5)

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