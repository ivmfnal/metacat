import sys, getopt, os, json, fnmatch, pprint
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient
from metacat.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments

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

class ListCommand(CLICommand):
    
    Opts = "v verbose"
    Usage = """[-v|--verbose] [<namespace pattern>]
    """

    def __call__(self, command, client, opts, args):
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
                
class ShowCommand(CLICommand):
    
    Opts = "j json"
    MinArgs = 1
    Usage = """[-j|--json] <namespace>
        -j|--json           - print as JSON
    """
    
    def __call__(self, command, client, opts, args):
        data = client.get_namespace(args[0])
        if "-j" in opts or "--json" in opts:
            print(json.dumps(data, indent=4, sort_keys=True))
        else:
            pprint.pprint(data)

class CreateCommand(CLICommand):

    Opts = "oj json owner"
    MinArgs = 1
    Usage = """[options] <namespace>
        -o <owner>|--owner <owner>              - namespace owner
        -j|--json                               - print as JSON 
    """
    
    def __call__(self, command, client, opts, args):
        name = args[0]
        data = client.create_namespace(name, owner_role=opts.get("-o", opts.get("--owner")))
        if "-j" in opts or "--json" in opts:
            print(json.dumps(data, indent=4, sort_keys=True))
        else:
            pprint.pprint(data)
    
NamespaceCLI = CLI(
    "create",   CreateCommand(),
    "list",     ListCommand(),
    "show",     ShowCommand()
)
 
