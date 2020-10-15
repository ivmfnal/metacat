import sys, traceback, yaml, os
import psycopg2
from metacat.mql import parse_query
from getopt import getopt

Usage = """
python query_test.py parse [-v] <query>
python query_test.py run [-v] [-c <config_file>] <query>

To run a query, the config file must be provided either using -c, or by defining the environent variable METACAT_SERVER_CFG

The config file must be a YAML file with the following definition:

database:
    user: ...
    password: ...
    host: ...
    port: ...
    dbname: ...
"""

if not sys.argv[1:] or sys.argv[1] in ("help", "--help", "-?"):
    print(Usage)
    sys.exit(2)

cmd = sys.argv[1]
opts, args = getopt(sys.argv[2:], "vc:o")
opts = dict(opts)

if cmd == "parse":
    qtext = " ".join(args)
    print("Query text:'%s'" % (qtext,))
    q = parse_query(qtext)
    print("Converted:---------------")
    print(q.Tree.pretty("    "))
    if "-o" in opts:
        q.skip_assembly()
        optimized = q.optimize()
        print("Optimized:---------------")
        print(optimized.pretty("    "))
elif cmd == "run":
    
    def connect(dbcfg):
        connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % dbcfg
        return psycopg2.connect(connstr)
    
    config = opts.get("-c", os.environ.get("METACAT_SERVER_CFG"))
    if not config:
        print("Database configuration not found")
        print(Usage)
        sys.exit(2)
    
    qtext = " ".join(args)
    config = yaml.load(open(config, "r").read(), Loader=yaml.SafeLoader)["database"]
    print("Query text:'%s'" % (qtext,))
    q = parse_query(qtext, debug=True)
    #print("connecting to db...")
    db = connect(config)
    #print("connected to db")
    results = q.run(db, debug=True)
    print("Query results:")
    for r in results:
        print(r)
else:
    print(Usage)
    sys.exit(2)
    
    
