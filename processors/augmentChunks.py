#!/usr/bin/env python3

version_number = '0.1'

from configuration.ecchr import matched_entities
from functions import *


def searchDictInList(list, key, value):
    for item in list:
        if item[key] == value:
            return item

def searchDictInListFuzzy(list, key, value):
    items = []
    for item in list:
        if value.lower() in item[key].lower():
            items.append(item)
    return items


argument_parser = argparse.ArgumentParser(
    description='Augments chunks in a metasphere graph database with additional resources'
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
db_address = vars(arguments)['db_address']
db_username = vars(arguments)['db_username']
db_password = vars(arguments)['db_password']
api_base_url = vars(arguments)['api_address']
verbose = vars(arguments)['verbose']
very_verbose = vars(arguments)['vv']
dry_run = vars(arguments)['dry_run']

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
            request(endpoint, query)
            if reconnect_tries <= max_reconnect_tries:
                failed_requests.remove(query)
                continue
            else:
                print(f"\n[red bold]API is not responding. Aborting.")
                dump_failed_inserts()
                sys.exit(2)
        else:
            break


chunks = []
def load_chunks_from_database():
    print(database, f"Loading chunks from database.")

    query = f'MATCH (c:Chunk) RETURN c'
    parameters = {}
    results = [record for record in graph.run(query).data()]
    if verbose: print(results)
    num_chunks = 0
    for record in results:
        num_chunks += 1
        chunks.append(dict(record['c']))
    print(checkmark, database, f"Successfully loaded {num_chunks} chunks from database.")
    return chunks


def create_resource_node_for_chunk(chunk, resource):
    chunk_id = chunk["chunk_id"]
    hash = hashlib.md5(resource["url"].encode("utf-8"))
    resource_id = hash.hexdigest()
    resource["resource_id"] = resource_id
    print(eye, database, f"Creating resource node for: [bold]{chunk_id}[/bold]")
    endpoint = '/graph/add/resource'
    query = {
        "resource_type": resource["resource_type"],
        "url": resource["url"],
        "resource_id": resource_id,
        "name": resource["name"],
        "description": resource["description"]
    }
    if verbose: print(query)
    if not dry_run:
        response = request(endpoint, query)
        if response["status"] == "success":
            print(checkmark, database, f"Created resource node.")
            return resource
        else:
            print(cross, database, f"Could not create resource node.")
            return False


def connect_resource_node_to_chunk(chunk, resource):
    print(eye, database, f"Connecting resource node to chunk")
    endpoint = '/graph/connect/resource'
    query = {
        "connect": resource["resource_id"],
        "with": {
          "id": chunk["chunk_id"],
        }
    }
    if verbose: print(query)
    if not dry_run:
        response = request(endpoint, query)
        if response["status"] == "success":
            print(checkmark, database, f"Successfully connected resource node.")
            return True
        else:
            print(cross, database, f"Could not connect resource node.")
            return False


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


def inquire_text(message, default=""):
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


chunks = load_chunks_from_database()
answer = inquire_text("Search chunk")
search_pattern = answer["text"]
print (f"Searching for chunks containing {search_pattern}")
chunks_to_process = searchDictInListFuzzy(chunks, "text", search_pattern)
chunk_ids = []
if len(chunks_to_process) >= 1:
    for chunk in chunks_to_process:
        chunk_id = chunk["chunk_id"]
        print(f"Found chunk: [bold]{chunk_id}[/bold]:")
        print(chunk["text"] + "\n")
        chunk_ids.append(chunk_id)
    answer = inquire_list("Choose chunk to augment", chunk_ids)
    chunk_id = answer["list"]
    chunk = searchDictInList(chunks_to_process, "chunk_id", chunk_id)
    print (f"Processing {chunk}")
    keep_processing = "yes"
    resources_to_process = []
    while keep_processing == "yes":
        resource = {
            "resource_type": "",
            "url": "",
            "resource_id": "",
            "name": "",
            "description": ""
        }
        questions = [
            inquirer.List('resource_type',
                          message="Resource type",
                          choices=["url", "image"]),
            inquirer.Text('name',
                          message="Name"),
            inquirer.Text('description',
                          message="Description"),
            inquirer.Text('url',
                          message="Link")
        ]
        answers = inquirer.prompt(questions, theme=GreenPassion())
        resource["resource_type"] = answers["resource_type"]
        resource["url"] = answers["url"]
        resource["name"] = answers["name"]
        resource["description"] = answers["description"]
        questions = [
            inquirer.List('continue',
                          message="Add another resource?",
                          choices=["yes", "no"])
        ]
        answer = inquirer.prompt(questions, theme=GreenPassion())
        keep_processing = answer["continue"]
        resources_to_process.append(resource)
        print (keep_processing)
        continue
    if len(resources_to_process) >= 1:
        for resource in resources_to_process:
            create_resource_node_for_chunk(chunk, resource)
            connect_resource_node_to_chunk(chunk, resource)

