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
        -l|--line                           - print all metadata on single line (good for grepping, ignored with -j and -p)
        -i|--ids                            - print file ids instead of names
        -s|--summary                        - print only summary information
        -m|--metadata=[<field>,...]         - print metadata fields
                                              overrides --summary
        -m|--metadata=all                   - print all metadata fields
                                              overrides --summary
        -P|--with-provenance                - include provenance information
        -N|--namespace=<default namespace>  - default namespace for the query
        -S|--save-as=<namespace>:<name>     - save files as a new datset
        -A|--add-to=<namespace>:<name>      - add files to an existing dataset
"""

def do_query(server_url, args):
    opts, args = getopt.getopt(args, "jism:N:pf:S:A:lP", ["line","json", "ids","summary","metadata=","namespace=","pretty",
                "with-provenance","save-as=","add-to="])
    opts = dict(opts)

    #print("opts:", opts,"    args:", args)
    
    namespace = opts.get("-N") or opts.get("--namespace")
    #with_meta = not "--summary" in opts and not "-s" in opts
    with_meta = "-m" in opts or "--metadata" in opts
    with_provenance = "-P" in opts or "--with-provenance" in opts
    keys = opts.get("-m") or opts.get("--metadata") or []
    if keys and keys != "all":    keys = keys.split(",")
    save_as = opts.get("-S") or opts.get("--saves-as")
    add_to = opts.get("-A") or opts.get("--add-to")

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
        
    #print("with_meta=", with_meta)
        
    results = client.query(query_text, namespace=namespace, with_metadata = with_meta, 
            save_as=save_as, add_to=add_to,
            with_provenance=with_provenance)

    if "--json" in opts or "-j" in opts:
        print(json.dumps(results, sort_keys=True, indent=4, separators=(',', ': ')))
        sys.exit(0)

    if "--pretty" in opts or "-p" in opts:
        meta = sorted(results, key=lambda x: x["name"])
        pprint.pprint(meta)
        sys.exit(0)

    in_line = "-l" in opts or "--line" in opts

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
                if in_line:
                    for k in klist:
                        if k in meta:
                            meta_lst.append("%s=%s" % (k, repr(meta[k])))
                else:
                    for k in klist:
                        if k in meta:
                            meta_lst.append("%s\t=\t%s" % (k, repr(meta[k])))
                if meta_lst:
                    if in_line:
                        meta_out = "\t"+"\t".join(meta_lst)
                    else:
                        meta_out += "\n    "+"\n    ".join(meta_lst)
            if "--ids" in opts or "-i" in opts:
                print("%s%s" % (f["fid"],meta_out))
            else:
                print("%s:%s%s" % (f["namespace"], f["name"], meta_out))
        
                
    
    
    
