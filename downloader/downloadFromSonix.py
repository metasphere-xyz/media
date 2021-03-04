#!/usr/bin/env python3

import sys
import getopt
import json
import hashlib

help_message = """
downloadFromSonix.py:
Downloads a transcript from sonix.ai
usage: downloadFromSonix.py [-i] <sonix media id>
-i --id <sonix media id>: media id of the transcript location
"""

