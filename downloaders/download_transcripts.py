# %% Import and Authentication
import json
import requests
import os

#%% Create Directory for Transcripts
path = os.getcwd()
path = path + '/transcript_storage'

try:
    os.mkdir(path)
except OSError:
    print ("Creation of the directory %s failed" % path)
else:
    print ("Successfully created the directory %s " % path)

# %% GET Media IDs from SONIX AI
headers = {
    'Authorization': 'Bearer HDx7ZSQ9CIzbwJZ90OurHAtt',
}

response_full = requests.get(
    'https://api.sonix.ai/v1/media/', headers=headers)
r_media = response_full.json()
media_ids = json.loads(json.dumps(r_media))

# %% LIST with all Media IDs
media_amount = len(media_ids['media'])
lst = []
for i in range(media_amount):
    lst.append(media_ids['media'][i]['id'])

# %% WRITE transcripts to JSON files
for i in lst:
    response = requests.get(
        'https://api.sonix.ai/v1/media/{id}/transcript.json'.format(id=i), headers=headers)
    filename = response.json()

    with open('./transcript_storage/{id}.json'.format(id=i), 'w') as outfile:
        json.dump(filename, outfile)
