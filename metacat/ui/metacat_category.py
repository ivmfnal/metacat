import sys, getopt, os, json, fnmatch, pprint
from datetime import datetime, timezone
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
                print("Path:            ", data["path"])
                print("Description:     ", data.get("description") or "")
                print("Owner user:      ", data.get("owner_user", "") or "")
                print("Owner role:      ", data.get("owner_role", "") or "")
                print("Creator:         ", data.get("creator", ""))
                ct = data.get("created_timestamp") or ""
                if ct:
                    ct = datetime.fromtimestamp(ct, timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                print("Created at:      ", ct)
                print("Restricted:      ", "yes" if data.get("restricted", False) else "no")
                print("Constraints:")
                for name, constraint in sorted(data.get("definitions", {}).items()):
                    line = "  %-40s %10s" % (name, constraint.get("type", "any"))
                    if "values" in constraint:
                        line += " %s" % (tuple(constraint["values"]),)
                    rng = None
                    if "min" in constraint:
                        rng = [repr(constraint["min"]), ""]
                    if "max" in constraint:
                        if rng is None: rng = ["", ""]
                        rng[1] = repr(constraint["max"])
                    if rng is not None:
                        line += " [%s - %s]" % tuple(rng)
                    if "pattern" in constraint:
                        line += " ~ '%s'" % (constraint["pattern"])
                    print(line)



CategoryCLI = CLI(
    "list",     ListCommand(),
    "show",     ShowCommand()
)
 
