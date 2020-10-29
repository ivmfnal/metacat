import sys, getopt, os, json, pprint
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient

Usage = """
Usage: 
    metacat query <options> "<MQL query>"
    metacat query <options> -f <MQL query file>

    Options:
        -j|--json                           - print raw JSON output
        -p|--pretty                         - pretty-print metadata
        -i|--ids                            - print file ids instead of names
        -s|--summary                        - print only summary information
        -m|--metadata=[<field>,...]         - print metadata fields
                                              overrides --summary
        -m|--metadata=all                   - print all metadata fields
                                              overrides --summary
        -N|--namespace=<default namespace>  - default namespace for the query
        -S|--save-as=<namespace>:<name>     - save results as a new datset
"""

def do_query(config, server_url, args):
    opts, args = getopt.getopt(args, "jism:N:pf:S:", ["json", "ids","summary","metadata=","namespace=","pretty",
                "save-as="])
    opts = dict(opts)

    #print("opts:", opts,"    args:", args)
    
    namespace = opts.get("-N") or opts.get("--namespace")
    with_meta = not "--summary" in opts and not "-s" in opts
    with_meta = with_meta or "-m" in opts or "--metadata" in opts
    keys = opts.get("-m") or opts.get("--metadata") or []
    if keys and keys != "all":    keys = keys.split(",")
    save_as = opts.get("-S") or opts.get("--saves-as")

    #print("url:", url)
    client = MetaCatClient(server_url)
    if args:
        query_text = " ".join(args)
    else:
        query_file = opts.get("-f")
        if not query_file:
            print(Usage)
            sys.exit(2)
        query_text = to_str(open(query_file, "r").read())
        
    results = client.run_query(query_text, namespace=namespace, with_metadata = with_meta, save_as=save_as)

    if "--json" in opts or "-j" in opts:
        print(json.dumps(results, sort_keys=True, indent=4, separators=(',', ': ')))
        sys.exit(0)

    if "--pretty" in opts or "-p" in opts:
        meta = sorted(results, key=lambda x: x["name"])
        pprint.pprint(meta)
        sys.exit(0)

    #print("response data:", out)
    
    if "-s" in opts or "--summary" in opts and not with_meta:
        print("%d files" % (len(out),))
    else:
        for f in results:
            meta_lst = []
            meta_out = ""
            if with_meta:
                meta = f["metadata"]
                klist = sorted(list(meta.keys())) if keys == "all" else keys
                for k in klist:
                    if k in meta:
                        meta_lst.append("%s=%s" % (k, repr(meta[k])))
            if meta_lst:
                meta_out = "\t"+"\t".join(meta_lst)
            if "--ids" in opts or "-i" in opts:
                print("%s%s" % (f["fid"],meta_out))
            else:
                print("%s%s" % (f["name"],meta_out))
        
                
    
    
    
