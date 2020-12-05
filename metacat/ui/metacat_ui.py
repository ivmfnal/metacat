from metacat.webapi import MCAuthenticationError, MCWebAPIError
import sys, getopt, os

Usage = """
Usage: 
    metacat [-s <server URL>] command argsuments
    metacat help

    Server host:port can also be specified using environment variable METACAT_SERVER_URL
    
    Commands are:
        auth       login, whoami, list
        dataset    create, update, list, show
        namespace  create, list, show
        file       declare, update, show, add
        query
"""

Commands = ["admin","auth","dataset","query","namespace","file"]

def main():

    opts, args = getopt.getopt(sys.argv[1:], "s:")
    opts = dict(opts)

    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)
        
    cmd, args = args[0], args[1:]
    server_url = opts.get("-s", os.environ.get("METACAT_SERVER_URL"))
    if not server_url or not cmd in Commands:
        print("Server address must be specified either using -s option or using environment variable METACAT_SERVER_URL")
        sys.exit(2)

    if cmd == "admin":
        # does not require server configuration
        from .metacat_admin import do_admin
        do_admin(args)
        sys.exit(0)

    try:
        if cmd == "query":
            from .metacat_query import do_query
            do_query(server_url, args)
        elif cmd == "auth":
            from .metacat_auth import do_auth
            do_auth(server_url, args)
        elif cmd == "dataset":
            from .metacat_dataset import do_dataset
            do_dataset(server_url, args)
        elif cmd == "file":
            from .metacat_file import do_file
            do_file(server_url, args)
        elif cmd == "namespace":
            from .metacat_namespace import do_namespace
            do_namespace(server_url, args)
    except MCWebAPIError as e:
        sys.stderr.write(str(e))
        sys.exit(1)
    
    
if __name__ == "__main__":
    main()
    
