import sys, getopt, os, json, pprint
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient, MCServerError, MCError
from metacat.ui.cli import CLICommand, InvalidArguments, InvalidOptions
from metacat.mql import MQLQuery

Usage = """
Usage: --
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

class QueryCommand(CLICommand):

    GNUStyle = False    
    Opts = (
        "jism:N:pq:S:A:lPx", 
        ["line", "json", "ids", "summary", "metadata=", "namespace=", "pretty",
            "with-provenance", "save-as=", "add-to=", "explain"
        ]
    )
    Usage = """[<options>] (-q <MQL query file>|"<MQL query>")

        options:
            -j|--json                           - print raw JSON output
            -p|--pretty                         - pretty-print metadata
            -l|--line                           - print all metadata on single line (good for grepping, ignored with -j and -p)
            -i|--ids                            - print file ids instead of names
            -s|--summary                        - print only summary information
            -m|--metadata [<field>,...]         - print metadata fields
                                                  overrides --summary
            -m|--metadata all                   - print all metadata fields
                                                  overrides --summary
            -P|--with-provenance                - include provenance information
            -N|--namespace=<default namespace>  - default namespace for the query
            -S|--save-as=<namespace>:<name>     - save files as a new datset
            -A|--add-to=<namespace>:<name>      - add files to an existing dataset
            
            -x|--explain                        - dp not run the query, show resulting SQL only
    """
    
    def __call__(self, command, client, opts, args):
        namespace = opts.get("-N") or opts.get("--namespace")
        #with_meta = not "--summary" in opts and not "-s" in opts
        with_meta = "-m" in opts or "--metadata" in opts
        with_provenance = "-P" in opts or "--with-provenance" in opts
        keys = opts.get("-m") or opts.get("--metadata") or []
        if keys and keys != "all":    keys = keys.split(",")
        save_as = opts.get("-S") or opts.get("--saves-as")
        add_to = opts.get("-A") or opts.get("--add-to")

        #print("url:", url)
        if args:
            query_text = " ".join(args)
        else:
            query_file = opts.get("-q")
            if not query_file:
                raise InvalidArguments("Query must be specified")
            query_text = to_str(open(query_file, "r").read())
            
        if "-x" in opts or "--explain" in opts:
            print("---- Query text ----\n%s\n" % (query_text,))
            q = MQLQuery.parse(query_text, loader=client)

            print("---- Parsed ----")
            print(q.Parsed.pretty())
            print("")

            print("---- Converted ----")
            print(q.Tree.pretty("    "))
            print("")
        
            #q.optimize(False)
            #print("---- Optimized ----")
            #print(q.Optimized.pretty("    "))
            #print("")
    
            compiled = q.compile()
            print("---- Compiled ----")
            print(compiled.pretty("    "))
        else:
            results = client.query(query_text, namespace=namespace, with_metadata = with_meta, 
                        save_as=save_as, add_to=add_to,
                        with_provenance=with_provenance)

            if "--json" in opts or "-j" in opts:
                print(json.dumps(list(results), sort_keys=True, indent=4, separators=(',', ': ')))
                sys.exit(0)

            if "--pretty" in opts or "-p" in opts:
                meta = sorted(results, key=lambda x: x["name"])
                pprint.pprint(meta)
                sys.exit(0)

            in_line = "-l" in opts or "--line" in opts

            #print("response results:", results)
    
            if "-s" in opts or "--summary" in opts and not with_meta:
                print("%d files" % (len(results),))
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

QueryInterpreter = QueryCommand()