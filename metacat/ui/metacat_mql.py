import sys, getopt, os, json, pprint
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient, MCServerError, MCWebAPIError, MCError
from metacat.ui.cli import CLICommand, InvalidArguments, InvalidOptions
from metacat.mql import MQLQuery

class CompileCommand(CLICommand):
    
    GNUStyle = False    
    Opts = "q:"
    Usage = """(-q <MQL query file>|"<MQL query>")"""

    def __call__(self, command, client, opts, args):
        query_text = " ".join(args)
        if args:
            query_text = " ".join(args)
        else:
            query_file = opts.get("-q")
            if not query_file:
                raise InvalidArguments("Query must be specified")
            query_text = to_str(open(query_file, "r").read())
        
        print("---- Query text ----\n%s\n" % (query_text,))
        q = MQLQuery.parse(query_text, loader=client)

        print("---- Parsed ----")
        print(q.Parsed.pretty())
        print("")

        print("---- Converted ----")
        print(q.Tree.pretty("    "))
        print("")
        
        q.skip_assembly()
        q.optimize(False)
        print("---- Optimized ----")
        print(q.Optimized.pretty("    "))
        print("")
    
        compiled = q.compile()
        print("---- Compiled ----")
        print(compiled.pretty("    "))

MQLInterpreter = CompileCommand()