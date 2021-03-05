#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib
import time
import requests
from rich.progress import Progress, track
from rich import print

api_base_url = 'http://ecchr.metasphere.xyz:2342'
media_directory = 'files'

help_message = """pushCollection.py:
Processes a metasphere collection.json and pushes it into the graph database
usage: pushCollection.py [-i] <collection.json>
-i --input-file <collection.json>: location of metasphere collection.json
"""

checkmark = f"[green]"u'\u2713'
cross = f"[red]"u'\u00D7'

def request(endpoint, query):
    url = api_base_url + endpoint
    try:
        response = requests.post(
            url,
            data=json.dumps(query),
            headers={
                'Content-type': 'application/json'
            }
        )
    except error as e:
        raise_error(e)
    if response.status_code == requests.codes.ok:
        return response.json()
    else:
        raise_error(f"Error {response.status_code}: {response.text}")

def raise_error(error):
    print(help_message)
    if(error):
        print("Error: " + str(error))
    sys.exit(2)

def main():
    input_file = ""
    verbose = False

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "vhi:", ["input-file="]
        )
    except getopt.GetoptError as error:
        raise_error(error)
    for opt, arg in opts:
        if opt == '-h':
            print(help_message)
            sys.exit()
        elif opt in ("-i", "--input-file"):
            input_file = arg
        elif opt == '-v':
            verbose = True

    if not input_file:
        raise_error("Please specify the location of collection.json")

    print ("Loading " + input_file + ' ... ', end='')
    try:
        with open(input_file) as f:
            collection = json.load(f)
        print("done.")
    except (OSError, IOError) as error:
        raise_error(error)

    # Get collection meta data

    collection["name"]
    collection["collection_id"]
    collection["source_type"]
    collection["source_path"]

    num_chunks = len(collection["chunk_sequence"])

    with Progress() as progress:
        progress.console.print(f"\nCollection name:\t {collection['name']}")
        progress.console.print(f"Collection id:\t\t {collection['collection_id']}")
        progress.console.print(f"Collection type:\t {collection['source_type']}")

        chunk_progress = progress.add_task("Progress \t", total=num_chunks)
        request_progress = progress.add_task("API request \t", total=2)

        for chunk in range(num_chunks):
            data = collection["chunk_sequence"][chunk]
            chunk_id = data["chunk_id"]
            progress.console.print(f"\n[bold]Processing chunk [regular]#{chunk}")
            progress.console.print(f"Chunk id: {chunk_id}")

            endpoint = '/graph/find'
            query = {"name": chunk_id}
            response = request(endpoint, query)
            # response = {'status':'success'}

            progress.advance(request_progress)
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

                if verbose: progress.console.print(f"Inserting chunk into database.")
                endpoint = '/graph/add/chunk'
                query = {
                    'chunk_id': data["chunk_id"],
                    'text': data["text"],
                    'source_file': data["source_file"],
                    'start_time': data["start_time"],
                    'end_time': data["end_time"],
                    'summaries': [],
                    'entities': [],
                    'similarity': []
                }

                # UNCOMMENT response = request(endpoint, query)
                response["status"] = query
                progress.advance(request_progress)

                if response["status"] != "failed":
                    progress.console.print(checkmark, f"Successfully inserted chunk.")
                else:
                    progress.console.print(cross, f"[red]Error inserting chunk.")
            else:
                progress.console.print(cross, f"[white]Chunk already exists. Skipping.")

            if verbose: progress.console.print(f"Extracting entities.")
            time.sleep(.3)
            if verbose: progress.console.print(f"Extracting summaries.")
            time.sleep(.3)

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

if __name__ == "__main__":
    main()