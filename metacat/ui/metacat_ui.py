from metacat.webapi import MCWebAPIError, MetaCatClient, MCError, AuthenticationError
from metacat import Version
import sys, getopt, os
from .cli import CLI, CLICommand

from .metacat_file import FileCLI
from .metacat_dataset import DatasetCLI
from .metacat_namespace import NamespaceCLI
from .metacat_auth import AuthCLI
from .metacat_admin import AdminCLI
from .metacat_query import QueryInterpreter
from .metacat_category import CategoryCLI

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
        "version", VersionCommand(),
        "503", Simulate503Command()
    )
    try:
        cli.run(sys.argv, argv0="metacat")
    except (AuthenticationError, MCError) as e:
        print(e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
    
