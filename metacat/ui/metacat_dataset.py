import sys, getopt, os, json, fnmatch, pprint
#from urllib.request import urlopen, Request
from metacat.util import to_bytes, to_str, TokenLib, epoch
from metacat.webapi import MetaCatClient

import datetime


Usage = """
Usage: 
    metacat dataset <command> [<options>] ...
    
    Commands and options:
    
        list [<options>] [[<namespace pattern>:]<name pattern>]
            -l|--long -- detailed output
            
        create [<options>] <namespace>:<name>
            -p|--parent <parent namespace>:<parent name>
        
        show [<options>] <namespace>:<name>
        
        update <options> @<JSON file with metadata> <namespace>:<name>
        update <options> '<JSON expression>' <namespace>:<name>
            -r|--replace          - replace metadata, otherwise update
"""

def do_list(client, args):
    opts, args = getopt.getopt(args, "l", ["--long"])
    if args:
        patterns = args
    else:
        patterns = ["*"]
    opts = dict(opts)
    verbose = "-l" in opts or "--long" in opts
    output = client.list_datasets(with_file_counts=verbose)
    
    verbose_format = "%-16s %-19s %s"
    
    if verbose:
        print(verbose_format % (
            "creator", "created", "name/parent"
        ))
        print("-"*16, "-"*19, "-"*40)
    
    for item in output:
        match = False
        namespace, name = item["namespace"], item["name"]
        for p in patterns:
            pns = None
            if ":" in p:
                pns, pn = p.split(":", 1)
            else:
                pn = p
            if fnmatch.fnmatch(name, pn) and (pns is None or fnmatch.fnmatch(namespace, pns)):
                match = True
                break
        if match:
            if verbose:
                parent = "" if not item.get("parent_namespace") else ("/" + item["parent_namespace"] + ":" + item["parent_name"])
                ct = datetime.datetime.fromtimestamp(item.get("created_timestamp", 0))
                print(verbose_format % (
                    item.get("creator",""),
                    ct.strftime("%Y-%m-%d %H:%M:%S"),
                    namespace + ":" + name + parent
                ))
            else:
                print("%s:%s" % (namespace, name))
                    
                
    
def do_show(client, args):
    pprint.pprint(client.get_dataset_info(args[0]))
    
def do_create(client, args):
    opts, args = getopt.getopt(args, "p:", ["--parent="])
    opts = dict(opts)
    dataset_spec = args[0]    
    parent_spec = opts.get("-p") or opts.get("--parent")
    
    out = client.create_dataset(dataset_spec, parent = parent_spec)
    print(out)
    
def do_update(client, args):
    opts, args = getopt.getopt(args, "r", ["replace"])
    opts = dict(opts)

    mode = "replace" if ("-r" in opts or "--replace" in opts) else "update"

    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)
    
    meta = args[0]
    if meta.startswith('@'):
        meta = json.load(open(meta[1:], "r"))
    else:
        meta = json.loads(meta)

    dataset = args[1]

    response = client.update_dataset_meta(meta, dataset, mode=mode)
    print(response)

def do_dataset(server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)

    client = MetaCatClient(server_url)

    command = args[0]
    return {
        "list":     do_list,
        "update":   do_update,
        "create":   do_create,
        "show":     do_show
    }[command](client, args[1:])
    
    
 
