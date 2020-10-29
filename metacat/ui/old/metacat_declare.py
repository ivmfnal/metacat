import sys, getopt, os, json
#from urllib.request import urlopen, Request
#from urllib.error import HTTPError
from urllib.parse import quote_plus, unquote_plus
from py3 import to_bytes, to_str
from token_lib import TokenLib


import requests


Usage = """
Usage: 
    metacat declare <options> <json file> <dataset>
    
    Create new files with metadata, add them to the dataset

    Options:
        -n|--namespace=<default namespace>              - default namespace for files and datasets

"""

def do_declare(config, server_url, args):
    opts, args = getopt.getopt(args, "n:", ["namespace="])
    opts = dict(opts)

    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)
    
    url = server_url + "/data/declare"
    metadata = json.load(open(args[0], "r"))       # parse to validate JSON
    
    params = []
    namespace = opts.get("-n") or opts.get("--namespace")
    if namespace:
        params.append("namespace=%s" % (namespace,))
    params.append("dataset=%s" % (args[1],))
    
    url += "?" + "&".join(params)

    tl = TokenLib()
    token = tl.get(server_url)
    if not token:
        print("No valid token found. Please obtain new token")
        sys.exit(1)
        
    response = requests.post(url, data=to_bytes(json.dumps(metadata)), headers={"X-Authentication-Token": token.encode()})
    
    status = response.status_code
    if status/100 != 2:
        print("Error: ", status, "\n", response.text)
        sys.exit(1)

    body = response.text
    print(body)
                
    
    
    
