#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib

help_message = """
dumpCollection.py:
Queries the graph database and compiles a JSON object of the collection.
usage: dumpCollection.py [-i] <collection id> [-o <collection.json>]
-i --id <collection id>: id of the collection to be compiled
-o --output-file <collection.json>: location of output file (uses md5 id of collection as default)
"""

def raise_error(error):
    print(help_message)
    if(error):
        print("Error: " + str(error))
    sys.exit(2)


def main():
    collection_id = ""
    output_file = ""

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hi:o:", ["id=", "output-file="]
        )
    except getopt.GetoptError as error:
        raise_error(error)
    for opt, arg in opts:
        if opt == '-h':
            print(help_message)
            sys.exit()
        elif opt in ("-i", "--id"):
            collection_id = arg
        elif opt in ("-o", "--output-file"):
            output_file = arg

    if not collection_id:
        raise_error("No collection id specified. Please specify which collection should be dumped.")
    if not output_file:
        output_file = 'collection-' + collection_id + '.json'
        print("No output file specified. Using " + output_file)
#
# Pulls current state of the graph of a collection from graph database and generates collection.json.
# Queries: graph database
# Generates: collection.json
#
# // All text elements with a strikethrough value of true should be deleted
#
# {
#   "name": "Wolfgang & Mark"
#   "collection_id": "3644a684f98ea8fe223c713b77189a77", // md5 hashsum total sequence
#   "source_type": "audio",
#   "source_path": "/episodes/2020-10-3/3644a684f98ea8fe223c713b77189a77/mp3/full-episode.mp3",
#   "date": "2021-02-16 15:04:28.539573", // python datetime object
#   // https://stackabuse.com/converting-strings-to-datetime-in-python/
#
#   "chunk_sequence": [
#       {
#           "chunk_id": "c4ca4238a0b923820dcc509a6f75849b", // md5 hash text
#           "text": "So, hello everybody, to the latest edition of our podcast by ECCHR, European Center for Constitutional and Human Rights in Berlin.",
#           "source_file": "1-wolfgang.mp3",
#           "start_time": 0.21,
#           "end_time": 12.33,
#           "annotations": {
#               "url": "https://...",
#               "image": "https://..."
#           },
#           "summaries": [
#               {
#                   "compression": 32,
#                   "text": "This is a short summary.",
#                   "aim": 35,
#                   "deviation": 3
#               },
#               {
#                   "compression": 40,
#                   "text": "This is a medium summary.",
#                   "aim": 39,
#                   "deviation": 3
#               },
#               {
#                   "compression": 45,
#                   "text": "This is a long summary.",
#                   "aim": 43,
#                   "deviation": 3
#               }
#           ],
#           "entities": [
#               {
#                   "name": "Berlin",
#                   "label": "LOCATION",
#                   "start": 0,
#                   "end": 6,
#                   "token_start": 0, // was soll das Prodigy / Ines?! -> wahrscheinlich löschen
#                   "token_end": 0,
#                   "annotations": {
#                       "description": "This is an entity description",
#                       "url": "https://..."
#                   }
#               }
#           ],
#           "connections": [
#               {
#                   "chunk_id":"md5",
#                   "score": 95,
#                   "text": "Chunk text" // summary or original chunk or entity
#               },
#               {
#                   "chunk_id":"md5",
#                   "score": 91,
#                   "text": "Chunk text"
#               },
#               {
#                   "chunk_id":"md5",
#                   "score": 88,
#                   "text": "Chunk text"
#               }
#           ]
#       }
#   ]
# }
#
# Write general information to header
# Foreach node connected to collection ID:
# if node type == chunk
# write to chunk_sequence
# if node type == summary
# write to summaries section
# if node type == connection
# write to connections section

## OR JUST DUMP THE WHOLE NODE???


if __name__ == "__main__":
    main()