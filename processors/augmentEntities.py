#!/usr/bin/env python3

version_number = '0.1'

from configuration.ecchr import matched_entities
from functions import *

augmented_entities = [
    'PERSON',
    'ORG'
]

def searchDictInList(list, key, value):
    for item in list:
        if item[key] == value:
            return item

argument_parser = argparse.ArgumentParser(
    description='Fetches entities from metasphere graph database and lets you augment their properties'
)

argument_parser.add_argument('--update-all',
                             help='update all entities',
                             action="store_true", default=False
                             )
argument_parser.add_argument('--create-resource-nodes',
                             help='create resource nodes for entities with links',
                             action="store_true", default=False
                             )
<<<<<<< HEAD
argument_parser.add_argument('--entity-type',
=======
argument_parser.add_argument('--entity-types',
>>>>>>> 0b3a8f4b0fe2c3fdf3ebe73087ab1600f6c0edf9
                             help='entity types to update',
                             default=augmented_entities
                             )
argument_parser.add_argument('--api-address',
                             help='url of the metasphere api to connect to',
                             default='http://ecchr.metasphere.xyz:2342'
                             )
argument_parser.add_argument('--db-address',
                             help='url of the metasphere graph database to connect to',
                             default='bolt://ecchr.metasphere.xyz:7687/'
                             )
argument_parser.add_argument('--db-username',
                             help='username for graph database',
                             default='neo4j'
                             )
argument_parser.add_argument('--db-password',
                             help='password for graph database',
                             default='burr-query-duel-cherry'
                             )
argument_parser.add_argument('--dry-run',
                             help='do not write to graph database',
                             action="store_true", default=False
                             )
argument_parser.add_argument('-v', '--verbose',
                             help='verbose output for debugging',
                             action="store_true", default=False
                             )
argument_parser.add_argument('-vv',
                             help='very verbose output for debugging',
                             action="store_true", default=False
                             )
argument_parser.add_argument('-V', '--version', action='version',
                             version="%(prog)s " + version_number
                             )

arguments = argument_parser.parse_args()

current_task = 'Starting'
update_all = vars(arguments)['update_all']
task_create_resource_nodes = vars(arguments)['create_resource_nodes']
db_address = vars(arguments)['db_address']
db_username = vars(arguments)['db_username']
db_password = vars(arguments)['db_password']
api_base_url = vars(arguments)['api_address']
verbose = vars(arguments)['verbose']
very_verbose = vars(arguments)['vv']
dry_run = vars(arguments)['dry_run']

<<<<<<< HEAD
entity_type = vars(arguments)['entity_type']
print (entity_type)
search_pattern = 'e:' + entity_type + ' '
# for accepted_type_index in range(1, len(entity_type)):
#     search_pattern += 'OR e:' + entity_type[accepted_type_index] + ' '
=======
entity_types = vars(arguments)['entity_types']
search_pattern = 'e:' + entity_types[0] + ' '
for accepted_type_index in range(1, len(entity_types)):
    search_pattern += 'OR e:' + entity_types[accepted_type_index] + ' '
>>>>>>> 0b3a8f4b0fe2c3fdf3ebe73087ab1600f6c0edf9

print(search_pattern)

checkmark = f"[green]"u'\u2713 '
cross = f"[red]"u'\u00D7 '
eye = f"[white]"u'\u2022 '
arrow = f"[grey]"u'\u21B3 '
database = f"[white]DATABASE[/white]:"

if very_verbose:
    print (arguments)


def connect_to_graph_database():
    print (eye, f"[bold]Connecting to[/bold] graph database.")
    try:
        graph = Graph(
            db_address,
            auth=(db_username, db_password)
        )
    except:
        print (cross, f"Can't connect to graph database. Exiting.")
        sys.exit(1)
    finally:
        print (checkmark, f"Connected to graph database.")
    return graph
graph = connect_to_graph_database()

failed_requests = []
def request(endpoint, query):
    url = api_base_url + endpoint
    if very_verbose:
        print(arrow, f'Sending request to {url}')

    while True:
        try:
            response = requests.post(
                url,
                data=json.dumps(query),
                headers={
                    'Content-type': 'application/json'
                }
            )
            if response.status_code == requests.codes.ok:
                if very_verbose:
                    print(checkmark, f'Request successful: {response.status_code}')
                if very_verbose:
                    print(response.json(), highlight=False)
                return response.json()
            else:
                if verbose:
                    print(cross)
                raise_error(f"Error {response.status_code}")
        except (
            requests.exceptions.RequestException
        ) as e:
            failed_requests.append(query)
            if verbose:
                print(cross, f'[red]Error sending request')
            if very_verbose:
                print('\n[red]', e)
            if verbose:
                print('\nReconnecting.')
            reconnect_tries = 0
            for seconds in range(timeout_for_reconnect):
                time.sleep(1)
                reconnect_tries += 1
            # request(endpoint, query)
            if reconnect_tries <= max_reconnect_tries:
                failed_requests.remove(query)
                continue
            else:
                print(f"\n[red bold]API is not responding. Aborting.")
                dump_failed_inserts()
                sys.exit(2)
        else:
            break


entities_to_process = []
def load_entities_from_database():
    print(database, f"Loading entities from database.")

    query = f'MATCH (e:Entity) WHERE {search_pattern} RETURN e'
    parameters = {}
    results = [record for record in graph.run(query).data()]
    entities = []
    if verbose: print(results)
    for record in results:
        entities.append({
            "entity_id": record['e']["entity_id"],
            "name": record['e']["name"],
            "text": record['e']["text"],
            "url": record['e']["url"]
        })
    return entities


def save_entity_to_database(entity):
    entity_id = entity["entity_id"]
    entity_name = entity["name"]
    print(database, f"Saving entity to database: [bold]{entity_name}[/bold] {entity_id}")
    if verbose: print (entity)
    endpoint = '/graph/update/entity'
    query = {
        "entity_id": entity["entity_id"],
        "name": entity["name"],
        "text": entity["text"],
        "url": entity["url"]
    }
    response = request(endpoint, query)
    if response["status"] == "success":
        print(checkmark, database, f"Updated entity.")
        return True
    else:
        print(cross, database, f"Could not update entity.")
        return False


def create_resource_node_for_entity(entity, resource):
    entity_id = entity["entity_id"]
    entity_name = entity["name"]
    hash = hashlib.md5(entity["url"].encode("utf-8"))
    resource_id = hash.hexdigest()
    resource["resource_id"] = resource_id
    print(eye, database, f"Creating resource node for: [bold]{entity_name}[/bold] {entity_id}")
    endpoint = '/graph/add/resource'
    query = {
        "resource_type": resource["resource_type"],
        "url": resource["url"],
        "resource_id": resource_id,
        "name": resource["name"],
        "description": resource["description"]
    }
    if verbose: print(query)
    response = request(endpoint, query)
    if response["status"] == "success":
        print(checkmark, database, f"Created resource node.")
        return resource
    else:
        print(cross, database, f"Could not create resource node.")
        return False


def connect_resource_node_to_entity(entity, resource):
    entity_id = entity["entity_id"]
    print(eye, database, f"Connecting resource node to entity")
    endpoint = '/graph/connect/resource'
    query = {
        "connect": resource["resource_id"],
        "with": {
          "id": entity["entity_id"],
        }
    }
    if verbose: print(query)
    response = request(endpoint, query)
    if response["status"] == "success":
        print(checkmark, database, f"Successfully connected resource node.")
        return True
    else:
        print(cross, database, f"Could not connect resource node.")
        return False


def create_resource_nodes():
    for entity in entities:
        if entity["url"] != '':
            resource = {
                "resource_type": 'url',
                "url": entity["url"],
                "name": entity["url"],
                "description": entity["text"]
            }
            created_resource = create_resource_node_for_entity(entity, resource)
            if created_resource:
                print ("XXXXX")
                print(created_resource)
                connect_resource_node_to_entity(entity, created_resource)


def clean(text):
    text_clean = text \
        .replace("\n", ' ') \
        .replace('; ', '') \
        .replace("\s\s+", " ") \
        .strip()
    text_clean = re.sub(r'(\(([^\)])+\))\W?', '', text)
    return text_clean


questions = []
def inquire_list(message, list):
    questions = [
        inquirer.List(
            "list",
            message=message,
            choices=list,
        ),
    ]
    return inquirer.prompt(questions, theme=GreenPassion())


def inquire_text(message, default):
    questions = [
        inquirer.Text('text',
                      message=message,
                      default=default)
    ]
    return inquirer.prompt(questions, theme=GreenPassion())


def inquire_editor(message, text):
    questions = [
        inquirer.Editor('editor', message=message, default=text)
    ]
    return inquirer.prompt(questions)

def get_wikipedia_summary(entity_name):
    entries = wikipedia.search(entity_name)
    description = ''
    if verbose: print(entries)

    if len(entries) > 1:
        answers = inquire_list("Please choose entry:", entries)
        entry_name = str(answers["list"])
    else:
        entry_name = entity_name
    descriptions = []
    if verbose: print(eye, f"fetching summary for {entry_name}")
    try:
        descriptions = wikipedia.summary(entry_name, sentences=2).split("\n")
    except:
        print (cross, f"Could not fetch description for {entry_name}\n")
        # The suggest() method returns suggestions related to the search query entered as a parameter to it, or it will return "None" if no suggestions were found.
        # > use for entity disambiguation
        # suggestion = wikipedia.suggest(entity_name)
        # if verbose: print(suggestion)
        # answer = inquire_text("Correct entity name:", suggestion)

        failed_requests.append(entity)
    finally:
        if len(descriptions) >= 1:
            description = clean(descriptions[0])
        if description:
            return description

def inquire_submit_text(variable):
    answers = ['yes', 'edit', 'delete']
    answer = "no"
    while answer == "no":
        print (variable)
        answer = inquire_list("Submit?", answers)["list"]
    if answer == "edit":
        variable = inquire_editor("", variable)["editor"]
        inquire_submit_text(variable)
    if answer == "delete":
        variable = ''
    return variable


def update_entity(entity):
    print (f"Updating [bold]" + entity["name"] + "[/bold]")
    answer = inquire_text("Name:", entity["name"])
    entity_name = answer["text"]
    entity["name"] = answer["text"]
    print (checkmark, f"Setting name to [bold]{entity_name}[/bold]")
    print (f"Description:\n" + entity["text"])
    answer = inquire_list("Accept?", ["yes", "no"])
    if answer["list"] == "yes":
        print (checkmark, f"Accepted description:")
        print (entity["text"])
    else:
        print (f"Fetching description for {entity_name}")
        description = get_wikipedia_summary(entity_name)
        print(f"Summary for {entity_name}:")
        entity["text"] = inquire_submit_text(description)
    answer = inquire_text("URL:", entity["url"])
    entity["url"] = answer["text"]
    print (checkmark, database, "Updating entity")
    if save_entity_to_database(entity):
        print (checkmark, database, "Successfully updated entity.")


entities = load_entities_from_database()

if update_all:
    for entity in entities:
        update_entity(entity)
else:
    if task_create_resource_nodes:
        print ("Creating resource nodes.")
        create_resource_nodes()
    else:
        entity_names = [entity["name"] for entity in entities]
        answer = inquire_list("Please choose the entity you want to update:", entity_names)
        entity_to_update = answer["list"]
        entity = searchDictInList(entities, "name", entity_to_update)
        update_entity(entity)
        print (entity)





if len(failed_requests) > 1:
    print("\n", cross, f"The following entities had request issues:")
    for entity in failed_requests:
        entity_name = entity["name"]
        print (f"[red]{entity_name}[/red]")