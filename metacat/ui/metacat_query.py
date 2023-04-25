import sys, getopt, os, json, pprint
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient, MCServerError, MCWebAPIError, MCError
from metacat.ui.cli import CLICommand, InvalidArguments, InvalidOptions

class QueryCommand(CLICommand):

    GNUStyle = False    
    Opts = (
        "jism:N:pq:S:A:lPxrL:U:S:R:Q:", 
        ["line", "json", "ids", "summary", "metadata=", "namespace=", "pretty",
            "with-provenance", "save-as=", "add-to=", "explain", "include-retired-files",
            "list=", "source=", "create=", "update=", "run="
        ]
    )
    Usage = """[<options>] (-q <MQL query file>|"<MQL query>")

        Options:
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
            -r|--include-retired-files          - include retired files into the query results

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
        include_retired = "-r" in opts or "--include-retired-files" in opts
        
        if args:
            query_text = " ".join(args)
        else:
            query_file = opts.get("-q")
            if not query_file:
                raise InvalidArguments("Query must be specified")
            query_text = to_str(open(query_file, "r").read())
            
        if "-x" in opts or "--explain" in opts:
            from metacat.mql import MQLQuery
            print("---- Query text ----\n%s\n" % (query_text,))
            q = MQLQuery.parse(query_text, loader=client, include_retired_files=include_retired)

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
    
            compiled = q.compile(with_meta=with_meta, with_provenance=with_provenance)
            print("---- Compiled ----")
            print(compiled.pretty("    "))
        else:
            results = client.query(query_text, 
                        namespace=namespace, with_metadata = with_meta, 
                        save_as=save_as, add_to=add_to,
                        with_provenance=with_provenance,
                        include_retired_files=include_retired
            )
            if "--json" in opts or "-j" in opts:
                results = list(results)
                print(json.dumps(results, sort_keys=True, indent=4, separators=(',', ': ')))
                sys.exit(0)

            if "--pretty" in opts or "-p" in opts:
                results = list(results)
                meta = sorted(results, key=lambda x: x["name"])
                pprint.pprint(meta)
                sys.exit(0)

            in_line = "-l" in opts or "--line" in opts

            #print("response results:", results)
    
            if "-s" in opts or "--summary" in opts and not with_meta:
                nfiles = total_size = 0
                for f in results:
                    nfiles += 1
                    total_size += f.get("size", 0)
                print("Files:       ", nfiles)
                if total_size >= 1024*1024*1024*1024:
                    unit = "TB"
                    n = total_size / (1024*1024*1024*1024)
                elif total_size >= 1024*1024*1024:
                    unit = "GB"
                    n = total_size / (1024*1024*1024)
                elif total_size >= 1024*1024:
                    unit = "MB"
                    n = total_size / (1024*1024)
                elif total_size >= 1024:
                    unit = "KB"
                    n = total_size / 1024
                else:
                    unit = "B"
                    n = total_size
                print("Total size:  ", "%d (%.3f %s)" % (total_size, n, unit))
                    
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