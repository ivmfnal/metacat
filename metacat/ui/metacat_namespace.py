import sys, getopt, os, json, fnmatch, pprint
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.webapi import MetaCatClient, MCError
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

    GNUStyle = True
    Opts = "dvu:r: verbose user= role= directly"
    Usage = """[options] [<pattern>]
        <pattern> is a UNIX shell style pattern (*?[]), optional
        -u|--user <username>        - list namespaces owned by the user
        -d                          - exclude namespaces owned by the user via a role
        -r|--role <role>            - list namespaces owned by the role
    """

    
    def __call__(self, command, client, opts, args):
        pattern = None if not args else args[0]
    
        opts = dict(opts)
        verbose = "-v" in opts or "--verbose" in opts
        match_owner_user = opts.get("-u", opts.get("--user"))
        match_owner_role = opts.get("-r", opts.get("--role"))
        if match_owner_user and match_owner_role:
            raise InvalidOptions("Owner user and owner role can not be used together")
        output = client.list_namespaces(pattern=pattern, owner_user=match_owner_user, owner_role=match_owner_role, directly="-d" in opts)
        for item in output:
            name = item["name"]
            owner_user = item.get("owner_user")
            owner_role = item.get("owner_role")
            if owner_user:
                owner = "u:"+owner_user
            else:
                owner = "r:"+owner_role
            print("%-30s\t%-20s\t%s" % (name, owner, item.get("descrition") or ""))
                
class ShowCommand(CLICommand):
    
    GNUStyle = True
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

    GNUStyle = True
    Opts = "o:j json owner="
    MinArgs = 1
    Usage = """[options] <namespace> [<description>]
        -o <owner>|--owner <owner>              - namespace owner
        -j|--json                               - print as JSON 
    """
    
    
    def __call__(self, command, client, opts, args):
        name = args[0]
        description = (" ".join(args[1:])).strip() or None
        data = client.create_namespace(name, owner_role=opts.get("-o", opts.get("--owner")), description=description)

        if "-j" in opts or "--json" in opts:
            print(json.dumps(data, indent=4, sort_keys=True))
    
NamespaceCLI = CLI(
    "create",   CreateCommand(),
    "list",     ListCommand(),
    "show",     ShowCommand()
)
 
