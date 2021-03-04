# metasphere media helpers
This repository is a collection of helper scripts to deal with media file conversion, cleanup and storage. They are intended to be run as shell scripts.

## downloader
#### downloadFromSonix.py
Downloads a JSON file of a transcript from sonix.ai

Usage: ```downloadFromSonix.py [-i] <sonix media id>```


## converter
#### sonix2metasphere.py
Converts a sonix.ai JSON transcript into a metasphere collection.json

usage: ```sonix2metasphere.py [-i] <transcript.json> [-o output.json]```


## processors
#### processCollection.py
Processes a metasphere collection.json and passes it into the graph database

Usage: ```processCollection.py [-i] <collection.json>```

##