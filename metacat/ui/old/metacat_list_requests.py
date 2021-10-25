import sys, getopt, os, json
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from py3 import to_bytes, to_str

import requests


Usage = """
Usage: 
    metacat [-c <config file>] list <options> "<MQL query>"
    metacat [-c <config file>] list <options> -q|--query <MQL query file>

    Options:
        -j|--json                           - print raw JSON output
        -i|--ids                            - print file ids instead of names
        -s|--summary                        - print only summary information
        -m|--metadata=[<field>,...]         - print metadata fields
                                              overrides --summary
        -m|--metadata=all                   - print all metadata fields
                                              overrides --summary
        -n|--namespace=<default namespace>  - default namespace for the query
"""

def do_list(config, args):
    opts, args = getopt.getopt(args, "jism:n:", ["json", "ids","summary","metadata=","namespace="])
    opts = dict(opts)

    #print("opts:", opts,"    args:", args)
    
    url = config["Server"]["URL"] + "/data/query"
    params = []
    namespace = opts.get("-n") or opts.get("--namespace")
    if namespace:
        params.append("namespace=%s" % (namespace,))
    with_meta = not "--summary" in opts and not "-s" in opts
    with_meta = with_meta or "-m" in opts or "--metadata" in opts
    keys = opts.get("-m") or opts.get("--metadata") or []
    if keys and keys != "all":    keys = keys.split(",")
    params.append("with_meta=%s" % ("yes" if with_meta else "no"))
    if params:
        url += "?" + "&".join(params)

    #print("url:", url)

    if args:
        query_text = args[0]
    else:
        query_file = opts.get("-q") or opts.get("--query")
        if not query_file:
            print(Usage)
            sys.exit(2)
        query_text = to_str(open(query_file, "r").read())

    #print("query_text: %s" % (query_text,))
    response = requests.post(url, data=query_text)
    #print(response)
    
    status = response.status_code
    if status/100 != 2:
        print("Error: ", status, "\n", response.read())
        sys.exit(1)

    if "--json" in opts or "-j" in opts:
        print(response.text)
        sys.exit(0)

    out = response.json()

    #print("response data:", out)
    
    if "-s" in opts or "--summary" in opts and not with_meta:
        print("%d files" % (len(out),))
    else:
        for f in out:
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
                print("%s:%s%s" % (f["namespace"],f["name"],meta_out))
        
                
    
    
    
