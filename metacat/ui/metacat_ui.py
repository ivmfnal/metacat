from metacat.webapi import MCAuthenticationError, MCWebAPIError
from metacat import Version
import sys, getopt, os

import warnings
warnings.simplefilter("ignore")


Usage = f"""
MetaCat version {Version}

Usage: 
    metacat [-s <server URL>] [-a <auth server URL>] command argsuments
    metacat help

    Server host:port can also be specified using environment variables METACAT_SERVER_URL and METACAT_AUTH_SERVER_URL
    
    Commands are:
        auth       login, whoami, list, mydn, export, import
        dataset    create, update, list, show
        namespace  create, list, show
        file       declare, update, show, add
        query
"""

Commands = ["admin","auth","dataset","query","namespace","file"]

def main():

    opts, args = getopt.getopt(sys.argv[1:], "s:a:")
    opts = dict(opts)

    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)
        
    cmd, args = args[0], args[1:]
    server_url = opts.get("-s") or os.environ.get("METACAT_SERVER_URL")
    
    if not server_url:
        print("Server address must be specified either using -s option or using environment variable METACAT_SERVER_URL", file=sys.stderr)
        sys.exit(2)

    auth_server_url = opts.get("-a") or os.environ.get("METACAT_AUTH_SERVER_URL")
    if not auth_server_url:
        #print("Warning: MetaCat authentication server URL is not set. Using default:", server_url+"/auth", file=sys.stderr)
        auth_server_url = server_url+"/auth"
        
    if not cmd in Commands:
        print("Unrecognized command", cmd, file=sys.stderr)
        print(Usage)
        sys.exit(2)

    if cmd == "admin":
        # does not require server configuration
        from .metacat_admin import do_admin
        do_admin(args)
        sys.exit(0)

    try:
        if cmd == "query":
            from metacat.ui.metacat_query import do_query
            do_query(server_url, args)
        elif cmd == "auth":
            from metacat.ui.metacat_auth import do_auth
            do_auth(server_url, auth_server_url, args)
        elif cmd == "dataset":
            from metacat.ui.metacat_dataset import do_dataset
            do_dataset(server_url, args)
        elif cmd == "file":
            from metacat.ui.metacat_file import do_file
            do_file(server_url, args)
        elif cmd == "namespace":
            from metacat.ui.metacat_namespace import do_namespace
            do_namespace(server_url, args)
    except MCWebAPIError as e:
        sys.stderr.write(str(e))
        sys.exit(1)
    
    
if __name__ == "__main__":
    main()
    
