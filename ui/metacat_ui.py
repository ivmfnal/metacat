from metacat_config import MetaCatConfig
import sys, getopt, os

Usage = """
Usage: 
    metacat [-c <config file>] [-s <server host>:<server port>] command argsuments
    metacat help

    Configuration file can also be specified using environment variable METACAT_CONFIG
    Server host:port can also be specified using environment variable METACAT_SERVER
    
    Commands are:
        admin      create, password, add, remove, list     -- requires direct connection to the database 
        auth       login, whoami, list
        dataset    create, list, show
        namespace  create, list, show
        file       declare, update, show, add
        query
"""

opts, args = getopt.getopt(sys.argv[1:], "c:s:")
opts = dict(opts)

server_addr = opts.get("-s", os.environ.get("METACAT_SERVER"))
if server_addr:
    host, port = server_addr.split(":", 1)
    server_addr = (host, int(port))

cfg_file = opts.get("-c", os.environ.get("METACAT_CONFIG"))    
if not cfg_file or not args or args[0] == "help":
    print (Usage)
    sys.exit(2)
    
config = MetaCatConfig(cfg_file)
cmd, args = args[0], args[1:]

if cmd == "admin":
    # does not require server configuration
    from metacat_admin import do_admin
    do_admin(config, args)
    sys.exit(0)



if server_addr:
    server_url = "http://%s:%s" % server_addr
else:
    server_url = config["Server"]["URL"]

if cmd == "query":
    from metacat_query import do_query
    do_query(config, server_url, args)
elif cmd == "auth":
    from metacat_auth import do_auth
    do_auth(config, server_url, args)
elif cmd == "dataset":
    from metacat_dataset import do_dataset
    do_dataset(config, server_url, args)
elif cmd == "file":
    from metacat_file import do_file
    do_file(config, server_url, args)
elif cmd == "namespace":
    from metacat_namespace import do_namespace
    do_namespace(config, server_url, args)
    
else:
    print(Usage)
    sys.exit(2)
    
    
