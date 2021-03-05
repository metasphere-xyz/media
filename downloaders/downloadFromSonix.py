#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib

help_message = """
downloadFromSonix.py:
Downloads a transcript from sonix.ai
usage: downloadFromSonix.py [[-i] <sonix media id>] [-l -a] <sonix api authentication token>
-i --id <sonix media id>: media id of the transcript location
-l --list: list sonix media ids associated with account
-a --all: download all transcripts from sonix account
"""

