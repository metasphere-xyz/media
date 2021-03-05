# metasphere media helpers
This repository is a collection of helper scripts to deal with media file conversion, cleanup and storage. They are intended to be run as shell scripts.




## downloaders
Downloaders download content from a specific site or service.


#### downloadFromSonix.py
Downloads a JSON file of a transcript from sonix.ai

Usage:
```bash
downloadFromSonix.py [-i] <sonix media id>

-i --id <sonix media id>: media id of the transcript location
```


## converters
Converters convert different data formats into metasphere compatible JSON structures.

#### sonix2metasphere.py
Converts a sonix.ai JSON transcript into a metasphere collection.json

Usage:
```bash
sonix2metasphere.py [-i] <transcript.json> [-o output.json]

-i --input-file <transcript.json>: location of the sonix transcript
-o --output-file <output.json>: output file (default: collection-<md5>.json)
```


## processors
Processors process metasphere compatible JSON structures and load them into the graph database.

#### pushCollection.py
Processes a metasphere collection.json and passes it into the graph database.

Usage:
```bash
pushCollection.py [-i] <collection.json>

-i --input-file <filename>: location of metasphere collection.json
```

#### pullCollection.py
Queries the graph database and compiles a JSON object of the collection.

Usage:
```bash
pullCollection.py [-i] <collection id> [-o <collection.json>]

-i --id <collection id>: id of the collection to be compiled
-o --output-file <filename>: location of output file (default: collection-<md5>.json)
```

