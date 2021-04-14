#!/bin/bash

source env/bin/activate

# Wolfgang & Christina
./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/102d3a312808a19439b8a7b6f9917dbe/full_episode.json
./processors/pushCollection.py --task store_entities files/podcasts/episodes/102d3a312808a19439b8a7b6f9917dbe/full_episode.json

# Wolfgang & Mark
./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/275b1e9261bbfb863c0e62cfa2aa9d73/full_episode.json
./processors/pushCollection.py --task store_entities files/podcasts/episodes/275b1e9261bbfb863c0e62cfa2aa9d73/full_episode.json

# Susan Meiselas and Wolfgang Kaleck
# ./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/2792e8ea6aa32398937276841d9b2414/full_episode.json
# ./processors/pushCollection.py --task store_entities files/podcasts/episodes/2792e8ea6aa32398937276841d9b2414/full_episode.json

# Ixmucané Aguilar and Wolfgang Kaleck
./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/445b28f26e7c2d54a6cbc6deeb57bc49/full_episode.json
./processors/pushCollection.py --task store_entities files/podcasts/episodes/445b28f26e7c2d54a6cbc6deeb57bc49/full_episode.json

# Claudia Salazar Jimenéz and Karina Theurer
./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/455f1001b9fcfda09b2b471e941853e4/full_episode.json
./processors/pushCollection.py --task store_entities files/podcasts/episodes/455f1001b9fcfda09b2b471e941853e4/full_episode.json

# Fred Ritchin and Wolfgang Kaleck
./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/7cd79ca6d4999a07e969b647a82057eb/full_episode.json
./processors/pushCollection.py --task store_entities files/podcasts/episodes/7cd79ca6d4999a07e969b647a82057eb/full_episode.json

# Rabih Mroué and Wolfgang Kaleck
./processors/pushCollection.py --task extract_entities --task store_collection files/podcasts/episodes/f8487321ec78207ffa6a25f9fbff7079/full_episode.json
./processors/pushCollection.py --task store_entities files/podcasts/episodes/f8487321ec78207ffa6a25f9fbff7079/full_episode.json



# After all chunks are processed, find similar chunks:
./processors/pushCollection.py --task find_similar_chunks --task store_entities files/podcasts/episodes/102d3a312808a19439b8a7b6f9917dbe/full_episode.json
./processors/pushCollection.py --task find_similar_chunks --task store_entities files/podcasts/episodes/275b1e9261bbfb863c0e62cfa2aa9d73/full_episode.json
# ./processors/pushCollection.py --task find_similar_chunks --task store_entities files/podcasts/episodes/2792e8ea6aa32398937276841d9b2414/full_episode.json
./processors/pushCollection.py --task find_similar_chunks files/podcasts/episodes/445b28f26e7c2d54a6cbc6deeb57bc49/full_episode.json
./processors/pushCollection.py --task find_similar_chunks files/podcasts/episodes/455f1001b9fcfda09b2b471e941853e4/full_episode.json
./processors/pushCollection.py --task find_similar_chunks files/podcasts/episodes/7cd79ca6d4999a07e969b647a82057eb/full_episode.json
./processors/pushCollection.py --task find_similar_chunks files/podcasts/episodes/f8487321ec78207ffa6a25f9fbff7079/full_episode.json

