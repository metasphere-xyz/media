#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib

help_message = """
sonix2metasphere.py:
Convert a sonix.ai JSON transcript into a metasphere collection.json
usage: sonix2metasphere.py [-i] <transcript.json> [-o output.json]
-i --input-file <transcript.json>: location of the sonix transcript
-o --output-file <output.json>: output file (default: collection.json)
"""

def raise_error(error):
    print(help_message)
    if(error):
        print("Error: " + str(error))
    sys.exit(2)


def preprocess(text):
    text = text.replace('\u00a0', ' ') # NBSP characters
    text = text.replace(' , ', ', ') # whitespace before punction
    text = text.replace(' . ', '. ') # whitespace before punction
    return text


def main():
    input_file = ""
    output_file = ""

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hi:o:", ["input-file=", "output-file="]
        )
    except getopt.GetoptError as error:
        raise_error(error)
    for opt, arg in opts:
        if opt == '-h':
            print(help_message)
            sys.exit()
        elif opt in ("-i", "--input-file"):
            input_file = arg
        elif opt in ("-o", "--output-file"):
            output_file = arg

    if not input_file:
        raise_error("Please specify the location of transcript.json")

    try:
        with open(input_file) as f:
            data = json.load(f)
            print ("Processing " + input_file)
    except (OSError, IOError) as error:
        raise_error(error)


    hash = hashlib.md5(data['name'].encode("utf-8"))
    collection_id = hash.hexdigest()

    if not output_file:
        output_file = 'collection-' + collection_id + '.json'

    file_format = data['transcript'][0]['speaker'][-3:]
    if file_format == "mp3" or "wav":
        source_type = "audio"
        source_path = '/podcasts/episodes/' + collection_id

    final_transcript = {
        'collection_id': collection_id,
        'name': data['name'],
        'source_type': source_type,
        'source_path': source_path,
        'chunk_sequence': [
        ]
    }

    chunk_number = 0
    for chunk in data['transcript']:
        text_complete = []

        for word in chunk['words']:
            if word.get('strikethrough') == False:
                text = text_complete.append(word.get('text'))

        text_transcript = ''.join(map(str, text_complete))

        hash = hashlib.md5(text_transcript.encode("utf-8"))
        chunk_id = hash.hexdigest()

        if text_transcript != "":
            chunk_number = chunk_number + 1
            speaker_name = str(chunk.get('speaker')[13:]).capitalize()

            final_transcript['chunk_sequence'].append({
                "chunk_id": chunk_id,
                "text": text_transcript,
                "source_file": str(chunk_number) + "-" + speaker_name,
                "start_time": chunk.get('start_time'),
                "end_time": chunk.get('end_time')
            })

    with open(output_file, 'w', encoding='utf8') as json_file:
        json.dump(final_transcript, json_file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()
