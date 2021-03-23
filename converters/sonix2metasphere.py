#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib

# TODO: change getopt to argparse

help_message = """
sonix2metasphere.py:
Convert a sonix.ai JSON transcript into a metasphere collection.json
usage: sonix2metasphere.py [-i] <transcript.json> [-o output.json]
-i --input-file <transcript.json>: location of the sonix transcript
-o --output-file <output.json>: output file (default: collection.json)
-n --episode-name <name>: episode name (overwrites name of sonix session)
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
    episode_name = ""

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hi:o:n:", ["input-file=", "output-file=", "episode-name="]
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
        elif opt in ("-n", "--episode-name"):
            episode_name = arg


    if not input_file:
        raise_error("Please specify the location of transcript.json")

    try:
        with open(input_file) as f:
            data = json.load(f)
            print ("Processing " + input_file)
    except (OSError, IOError) as error:
        raise_error(error)

    if not episode_name:
        episode_name = data['name']

    hash = hashlib.md5(episode_name.encode("utf-8"))
    collection_id = hash.hexdigest()

    if not output_file:
        output_file = 'collection-' + collection_id + '.json'

    file_format = data['transcript'][0]['speaker'][-3:]
    if file_format == "mp3" or "wav":
        source_type = "audio"
        source_path = '/podcasts/episodes/' + collection_id

# TODO: add speakers to episode header

    chunk_number = 0

    final_transcript = {
        'collection_id': collection_id,
        'name': episode_name,
        'source_type': source_type,
        'source_path': source_path,
        'num_chunks': chunk_number,
        'chunk_sequence': [
        ]
    }

    for chunk in data['transcript']:

        # TODO: add speaker, duration
        text_complete = []

        for word in chunk['words']:
            if word.get('strikethrough') == False:
                text = text_complete.append(word.get('text'))

        text_transcript = ''.join(map(str, text_complete))

        hash = hashlib.md5(text_transcript.encode("utf-8"))
        chunk_id = hash.hexdigest()

        if text_transcript != "":
            chunk_number = chunk_number + 1
            speaker_name = str(chunk.get('speaker')[13:]).capitalize().replace('.mp3', '')

            duration = round(chunk.get('end_time') - chunk.get('start_time'), 2)

            final_transcript['chunk_sequence'].append({
                "chunk_id": chunk_id,
                "speaker": speaker_name,
                "text": text_transcript,
                "source_file": str(chunk_number) + "-" + speaker_name + '.mp3',
                "start_time": chunk.get('start_time'),
                "end_time": chunk.get('end_time'),
                "duration": duration
            })

            final_transcript['num_chunks'] = chunk_number

    with open(output_file, 'w', encoding='utf8') as json_file:
        json.dump(final_transcript, json_file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()
