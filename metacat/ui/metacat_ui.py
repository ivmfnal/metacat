from metacat.webapi import MCWebAPIError, MetaCatClient, MCError, AuthenticationError
from metacat import Version
import sys, getopt, os, json
from .cli import CLI, CLICommand

from .metacat_file import FileCLI
from .metacat_dataset import DatasetCLI
from .metacat_namespace import NamespaceCLI
from .metacat_auth import AuthCLI
from .metacat_admin import AdminCLI
from .metacat_query import QueryInterpreter
from .metacat_category import CategoryCLI
from .metacat_named_query import NamedQueriesCLI
from metacat.util import validate_metadata

import warnings
warnings.simplefilter("ignore")

Usage = f"""
MetaCat version {Version}

Usage: 
    metacat [-s <server URL>] [-a <auth server URL>] command arguments
    metacat help

    Server host:port can also be specified using environment variables METACAT_SERVER_URL and METACAT_AUTH_SERVER_URL
    
    Commands are:
        auth       login, whoami, list, mydn, export, import
        dataset    create, update, list, show
        namespace  create, list, show
        file       declare, update, show, add
        query
"""

class MetaCatCLI(CLI):
    
    Opts="s:a:"    
    Usage = """[-s <server URL>] [-a <auth server URL>] <command> ...
        
            Both server and auth server URLs must be specified either using -s and -a or 
            via environment variables METACAT_SERVER_URL and METACAT_AUTH_SERVER_URL
        """
    
    def update_context(self, context, command, opts, args):

        if context is None and command != "admin":      # admin does not need to connect to the server
            server_url = opts.get("-s") or os.environ.get("METACAT_SERVER_URL")
    
            if not server_url:
                print("Server address must be specified either using -s option or using environment variable METACAT_SERVER_URL", file=sys.stderr)
                sys.exit(2)

            auth_server_url = opts.get("-a") or os.environ.get("METACAT_AUTH_SERVER_URL")
            if not auth_server_url:
                #print("Warning: MetaCat authentication server URL is not set. Using default:", server_url+"/auth", file=sys.stderr)
                auth_server_url = server_url+"/auth"
        
            context = MetaCatClient(server_url, auth_server_url)       # return the client as context
        return context

class VersionCommand(CLICommand):
    
    Usage = "print server and client versions"
    
    def __call__(self, command, client, opts, args):
        print("MetaCat Server URL:        ", client.ServerURL)
        print("Authentication server URL: ", client.AuthURL)
        print("Server version:            ", client.get_version())
        print("Client version:            ", Version)
        
class Simulate503Command(CLICommand):
    
    Usage = ""
    Hidden = True
    
    def __call__(self, command, client, opts, args):
        print(client.simulate_503())
        
class ValidateMetadataCommand(CLICommand):
    
    MinArgs = 1
    Opts = "d:"
    Usage = """[options] <JSON file with metadata>
        -d <dataset namespace>:<dataset name>           - if specified, validate the file metadata against the dataset requirements
        -q                                              - quiet - do not print anything, just use exit status to signal results
    """

    def __init__(self):
        self.Categories = None

    def find_category(self, name):
        # assume name has at least 1 dot in it
        cat_name, pname = name.rsplit('.', 1)
        cat = self.Categories.get(cat_name)
        immediate = True
        while cat_name != '.' and not cat:
            immediate = False
            words = cat_name.rsplit('.', 1)
            if len(words) == 1:
                cat_name = "."
            else:
                cat_name = words[0]
            cat = self.Categories.get(cat_name)
        return cat, immediate

    def __call__(self, command, client, opts, args):
        dataset = None
        dataset_did = opts.get("-d")
        if dataset_did:
            dataset = client.get_dataset(dataset_did)
            if dataset is None:
                print(f"Dataset {dataset_did} not found", file=sys.stderr)
                sys.exit(1)

        if self.Categories is None:
            categories = self.Categories = {c["path"]:c for c in client.list_categories()}

        meta = json.load(open(args[0], "r"))
        if not isinstance(meta, dict):
            raise InvalidArguments("Metadata must be a dictionary")
        errors = []
        for name, value in sorted(meta.items()):
            if not "." in name:
                errors.append(f"Invalid metadata parameter name: {name} - must be <category>.<name>")
                continue
            cat, immediate = self.find_category(name)
            if not cat:
                continue            # no category found, not even root
            if immediate:
                pname = name.rsplit('.', 1)[-1]
                cat_path = cat["path"]
                perrors = validate_metadata(cat["definitions"], cat["restricted"], name=pname, value=value)
                errors += [(cat_path + "." + pname, error) for (_, error) in perrors]
            elif cat["restricted"]:
                cat_path = cat["path"]
                errors.append((name, f"Undefined parameter {name} in restricted category {cat_path}"))

        if dataset is not None:
            errors += validate_metadata(dataset.get("file_meta_requirements", {}), False, meta)

        if errors:
            if "-q" not in opts:
                for name, error in errors:
                    print("%-40s: %s" % (name, error))
            sys.exit(1)
        else:
            sys.exit(0)

Commands = ["admin","auth","dataset","query","namespace","file"]

def main():

    cli = MetaCatCLI(
        "admin", AdminCLI,
        "auth", AuthCLI,
        "dataset", DatasetCLI,
        "namespace", NamespaceCLI,
        "category", CategoryCLI,
        "file", FileCLI,
        "query", QueryInterpreter,
        "named_query", NamedQueriesCLI,
        "version", VersionCommand(),
        "503", Simulate503Command(),
        "validate", ValidateMetadataCommand()
    )
    try:
        cli.run(sys.argv, argv0="metacat")
    except (AuthenticationError, MCError) as e:
        print(e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
    
