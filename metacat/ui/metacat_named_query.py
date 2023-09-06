import sys, getopt, os, json, fnmatch, pprint
from datetime import datetime, timezone
from textwrap import indent
#from urllib.request import urlopen, Request
from metacat.util import to_bytes, to_str, epoch
from metacat.webapi import MetaCatClient, MCError

from metacat.ui.cli import CLI, CLICommand, InvalidArguments


class ListNamedQueriesCommand(CLICommand):

    Opts = "n:j namespace= json"
    Usage = """[options]                     -- list named queries
        -n|--namespace                          - include queries from the namespace only
        -j|--json                               - as JSON
    """

    def __call__(self, command, client, opts, args):
        as_json = "-j" in opts or "--json" in opts
        namespace = opts.get("-n") or opts.get("--namespace")
        queries = client.list_named_queries(namespace=namespace)
        if as_json:
            print(json.dumps(queries, indent=2, sort_keys=True))
        else:
            for query in queries:
                print("Query:      %(namespace)s:%(name)s" % query)
                print("Created by: %(creator)s" % query)
                print("Created on: %s UTC" % (datetime.utcfromtimestamp(query["created_timestamp"]),))
                print("Source:")
                print(indent(query["source"], "  "))
                print()


class ShowNamedQueryCommand(CLICommand):

    Opts = "vj verbose json"
    Usage = """[options] <namespace>:<name>  -- show named query
        -j|--json                               - as JSON
        -v|--verbose                            - verbose outout. Otherwise - source only
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        as_json = "-j" in opts or "--json" in opts
        verbose = "-v" in opts or "--verbose" in opts
        
        if ':' not in args[0]:
            raise InvalidArguments()
        namespace, name = args[0].split(':', 1)
        query = client.get_named_query(namespace, name)
        if query is None:
            print("Not found")
            sys.exit(1)
        if as_json:
            print(json.dumps(query, indent=2, sort_keys=True))
        elif not verbose:
            print(query["source"])
        else:
            print("Query:      %(namespace)s:%(name)s" % query)
            print("Created by: %(creator)s" % query)
            print("Created on: %s UTC" % (datetime.utcfromtimestamp(query["created_timestamp"]),))
            print("Source:")
            print(indent(query["source"], "  "))


class CreateNamedQueryCommand(CLICommand):

    Opts = "f:u update file="
    Usage = """                                                -- create named query
        create [options] <namespace>:<name> <MQL query>         - inline query
        create [options] -f|--file <file> <namespace>:<name>    - read query from file
        create [options] <namespace>:<name>                     - read query from stdin
        
        Options:
            -u|--update                -- update if the named query if exists
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        if ':' not in args[0]:
            raise InvalidArguments()

        if len(args) > 1:
            query_text = " ".join(args[1:])
        else:
            input_file = opts.get("-f", opts.get("--file", "-"))
            if input_file == "-":
                query_text = sys.stdin.read()
            else:
                query_text = open(input_file, "r").read()

        update = "-u" in opts or "--update" in opts
        namespace, name = args[0].split(':', 1)
        query = client.create_named_query(namespace, name, query_text, update=update)

class SearchCommand(CLICommand):

    Opts = "f:q: format= query-file="
    Usage = """                                                -- search for named queries
        search <inline MQL query>                               - inline query
        search -q|--query <MQL query file>                      - read query from file
        search -q|--query -                                     - read query from stdin
        
        Options:
            -f|--format (json|pretty|names)                     - output format
    """
    
    def __call__(self, command, client, opts, args):
        query = None
        if "-q" in opts or "--query-file" in opts:
            query = load_text(opts.get("-q") or opts.get("--query")) or None
        elif args:
            query = " ".join(args)
        
        if query is None:
            raise InvalidArguments()
        
        fmt = opts.get("-f") or opts.get("--format") or "names"
        results = list(client.search_named_queries(query))
        if fmt == "json":
            print(json.dumps(results, indent=4, sort_keys=True))
        elif fmt == "pretty":
            pprint.pprint(results)
        else:
            for item in results:
                print(item["namespace"] + ":" + item["name"])
            


NamedQueriesCLI = CLI(
    "create",       CreateNamedQueryCommand(),
    "show",         ShowNamedQueryCommand(),
    "list",         ListNamedQueriesCommand(),
    "search",       SearchCommand()
)
    
 
