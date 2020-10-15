import sys, getopt, os, json, fnmatch
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient

Usage = """
Usage: 
    metacat namespce <command> [<options>] ...
    
    Commands and options:
    
        list [<options>] [<namespace pattern>]
            -v -- verbose
            
        create [<options>] <name>
            -o <role> -- owner role
        
        show <name>
"""

def do_list(config, client, args):
    opts, args = getopt.getopt(args, "v", ["--verbose"])
    
    pattern = None if not args else args[0]
    
    opts = dict(opts)
    verbose = "-v" in opts or "--verbose" in opts
    output = client.list_namespaces(pattern)
    for item in output:
        name = item["name"]
        print("%(name)s\towner:%(owner)s" % item)
                
    
def do_show(config, client, args):
    print(client.get_namespace(args[0]))
    
def do_create(config, client, args):
    opts, args = getopt.getopt(args, "o:", ["--owner="])
    opts = dict(opts)
    
    name = args[0]
    
    output = client.create_namespace(name, owner_role=opts.get("-o", opts.get("--owner")))
    print(output)
    
def do_namespace(config, server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)
        
    command = args[0]
    client = MetaCatClient(server_url)
    return {
        "list":     do_list,
        "create":   do_create,
        "show":     do_show
    }[command](config, client, args[1:])
    
    
 
