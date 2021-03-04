#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib

help_message = """
processCollection.py:
Processes a metasphere collection.json and pass it into the graph database
usage: processCollection.py [-i] <collection.json>
-i --input-file <transcript.json>: location of the sonix transcript
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


if __name__ == "__main__":
    main()