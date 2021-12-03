import sys, getopt, os, json, fnmatch, pprint
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

def do_list(client, args):
    opts, args = getopt.getopt(args, "v", ["--verbose"])
    
    pattern = None if not args else args[0]
    
    opts = dict(opts)
    verbose = "-v" in opts or "--verbose" in opts
    output = client.list_namespaces(pattern)
    for item in output:
        name = item["name"]
        owner = ""
        owner_user = item.get("owner_user")
        if owner_user:
            owner="u:"+owner_user
        else:
            owner_role = item.get("owner_role")
            if owner_role:
                owner = "r:"+owner_role
        print("%-30s\t%-20s\t%s" % (name, owner, item.get("descrition") or ""))
                
    
def do_show(client, args):
    pprint.pprint(client.get_namespace(args[0]))
    
def do_create(client, args):
    opts, args = getopt.getopt(args, "o:", ["--owner="])
    opts = dict(opts)
    
    name = args[0]
    
    output = client.create_namespace(name, owner_role=opts.get("-o", opts.get("--owner")))
    print(output)
    
def do_namespace(server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)
        
    command = args[0]
    client = MetaCatClient(server_url)
    try:
        method = {
            "list":     do_list,
            "create":   do_create,
            "show":     do_show
        }[command]
    except KeyError:
        print("Unknown subcommand:", command)
        sys.exit(2)
    return method(client, args[1:])
    
    
 
