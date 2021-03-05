#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib
import progress

help_message = """pushCollection.py:
Processes a metasphere collection.json and pushes it into the graph database
usage: pushCollection.py [-i] <collection.json>
-i --input-file <collection.json>: location of metasphere collection.json
"""

def raise_error(error):
    print(help_message)
    if(error):
        print("Error: " + str(error))
    sys.exit(2)


def main():
    input_file = ""

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hi:", ["input-file="]
        )
    except getopt.GetoptError as error:
        raise_error(error)
    for opt, arg in opts:
        if opt == '-h':
            print(help_message)
            sys.exit()
        elif opt in ("-i", "--input-file"):
            input_file = arg

    if not input_file:
        raise_error("Please specify the location of collection.json")

    print ("Loading " + input_file)
    try:
        with open(input_file) as f:
            data = json.load(f)
        print("done.")
    except (OSError, IOError) as error:
        raise_error(error)


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