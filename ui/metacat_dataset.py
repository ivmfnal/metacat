import sys, getopt, os, json, fnmatch
#from urllib.request import urlopen, Request
from metacat.util import to_bytes, to_str, TokenLib
from metacat.webapi import MetaCatClient

Usage = """
Usage: 
    metacat dataset <command> [<options>] ...
    
    Commands and options:
    
        list [<options>] [[<namespace pattern>:]<name pattern>]
            -v|--verbose
            
        create [<options>] <namespace>:<name>
            -p|--parent <parent namespace>:<parent name>
        
        show [<options>] <namespace>:<name>
"""

def do_list(config, client, args):
    opts, args = getopt.getopt(args, "v", ["--verbose"])
    if args:
        patterns = args
    else:
        patterns = ["*"]
    opts = dict(opts)
    verbose = "-v" in opts or "--verbose" in opts
    output = client.list_datasets(with_file_counts=verbose)
    for item in output:
        match = False
        for p in patterns:
            pns = None
            if ":" in p:
                pns, pn = p.split(":", 1)
            else:
                pn = p
            namespace, name = item["namespace"], item["name"]
            if fnmatch.fnmatch(name, pn) and (pns is None or fnmatch.fnmatch(namespace, pns)):
                match = True
                break
        if match:
            print("%s:%s" % (namespace, name))
            if verbose:
                print("    Parent:     %s:%s" % (item.get("parent_namespace") or "", item.get("parent_name") or ""))
                print("    File count: %d" % (item["file_count"],))
                    
                
    
def do_show(config, client, args):
    print(client.get_dataset(args[0]))
    
def do_create(config, client, args):
    opts, args = getopt.getopt(args, "p:", ["--parent="])
    opts = dict(opts)
    dataset_spec = args[0]    
    parent_spec = opts.get("-p") or opts.get("--parent")
    
    out = client.create_dataset(dataset_spec, parent = parent_spec)
    print(out)
    
def do_dataset(config, server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)

    client = MetaCatClient(server_url)

    command = args[0]
    return {
        "list":     do_list,
        "create":   do_create,
        "show":     do_show
    }[command](config, client, args[1:])
    
    
 
