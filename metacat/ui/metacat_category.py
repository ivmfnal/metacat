import sys, getopt, os, json, fnmatch, pprint
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient, MCError
from metacat.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments

class ListCommand(CLICommand):

    GNUStyle = True
    Opts = "j json"
    Usage = """[options] [<root category>]
        -j|--json           - print as JSON
    """

    def __call__(self, command, client, opts, args):
        root = None if not args else args[0]
        as_json = "-j" in opts or "--json" in opts
        lst = client.list_categories(root)
        if as_json:
            print(json.dumps(lst, indent=4, sort_keys=True))
        else:
            for c in lst:
                print(c["path"])
                
class ShowCommand(CLICommand):
    
    GNUStyle = True
    Opts = "j json"
    MinArgs = 1
    Usage = """[-j|--json] <category>
        -j|--json           - print as JSON
    """
    
    def __call__(self, command, client, opts, args):
        data = client.get_category(args[0])
        if data:
            if "-j" in opts or "--json" in opts:
                print(json.dumps(data, indent=4, sort_keys=True))
            else:
                pprint.pprint(data)

CategoryCLI = CLI(
    "list",     ListCommand(),
    "show",     ShowCommand()
)
 
