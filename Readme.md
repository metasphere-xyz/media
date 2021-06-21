# metasphere media helpers
This repository is a collection of helper scripts to deal with media file conversion, cleanup and storage. They are intended to be run as shell scripts.




## downloaders
Downloaders download content from a specific site or service.


#### downloadFromSonix.py
Downloads a JSON file of a transcript from sonix.ai

Usage:
```
downloadFromSonix.py [-i] <sonix media id>

-i --id <sonix media id>: media id of the transcript location


## converters
Converters convert different data formats into metasphere compatible JSON structures.

#### sonix2metasphere.py
Converts a sonix.ai JSON transcript into a metasphere collection.json

Usage:
```
sonix2metasphere.py [-i] <transcript.json> [-o output.json]

-i --input-file <transcript.json>: location of the sonix transcript
-o --output-file <output.json>: output file (default: collection-<md5>.json)
```


## processors
Processors process metasphere compatible JSON structures and load them into the graph database.

#### pushCollection.py
Processes a metasphere collection.json and passes it into the graph database.

Usage:
```
pushCollection.py <COLLECTION>
    [-n COLLECTION_NAME]
    [-s START_CHUNK] [-e END_CHUNK]
    [-m MEDIA_DIRECTORY] [--skip-media-check]
    [--api-address API_ADDRESS]
    [--task TASK]
    [-o OUTPUT_FILE]
    [--dry-run]
    [-h -v -vv -V]

positional arguments:
  <COLLECTION>                              path to metasphere collection.json to process

optional arguments:
  -n --collection-name <COLLECTION_NAME>    name of the collection (overwrites name specified in input file)
  -s --start-chunk <START_CHUNK>            start processing at chunk
  -e --end-chunk <END_CHUNK>                end processing at chunk
  -o --output-file <OUTPUT_FILE>            write collection.json to output file
  -m --media-directory <MEDIA_DIRECTORY>    base location of media files
  --skip-media-check                        skip filesystem check for associated media files
  --api-address <API_ADDRESS>               url of the metasphere api to connect to
  --task <TASK>                             task to execute [extract_entities generate_summaries find_similar_chunks]
  --dry-run                                 do not write to graph database
  -v, --verbose                             verbose output for debugging
  -vv                                       very verbose output for debugging
  -V, --version                             show version number and exit
  -h, --help                                show this help message and exit
```

##### Suggested Usage
- run task 'extract_entities' to extract entities
- check entities/accepted.json and entities/ambiguous.json, resolve conflicts
- run task 'store_chunks' to insert chunks into database
- run task 'insert_entities' to insert entities into database


#### pullCollection.py
Queries the graph database and compiles a JSON object of the collection.

Usage:
```bash
pullCollection.py [-i] <collection id> [-o <collection.json>]

-i --id <collection id>: id of the collection to be compiled
-o --output-file <filename>: location of output file (default: collection-<md5>.json)
```

