import sys, traceback, yaml, os
import psycopg2
from metacat.mql import parse_query
from getopt import getopt

Usage = """
python query_test.py parse [-d] <query>
python query_test.py run [-d] [-c <config_file>] <query>

To run a query, the config file must be provided either using -c, or by defining the environent variable METACAT_SERVER_CFG

The config file must be a YAML file with the following definition:

database:
    user: ...
    password: ...
    host: ...
    port: ...
    dbname: ...
"""

def sample(inputs, params):
    inp = inputs[0]
    fraction = params[0]
    #print("sample: inp:", inp, "  fraction:", fraction)
    x = 0.0
    for f in inp:
        x += fraction
        if x >= 1.0:
            x -= 1.0
            yield f
        #else:
        #    print("sample: skipping:", f)
            
def limit(inputs, params):
    inp = inputs[0]
    n = params[0]
    if n is None:
        yield from inp
    elif n > 0:
        for f in inp:
            yield f
            n -= 1
            if n <= 0:
                break
                
def print_meta(inputs, params):
    for f in inputs[0]:
        meta = f.metadata()
        lst = ["%s=%s" % (name, repr(meta.get(name))) for name in params]
        print(f"{f.FID} {f.Namespace}:{f.Name}  " + " ".join(lst))
        yield f

from metacat.filters import standard_filters as filters



if not sys.argv[1:] or sys.argv[1] in ("help", "--help", "-?"):
    print(Usage)
    sys.exit(2)

cmd = sys.argv[1]
opts, args = getopt(sys.argv[2:], "dc:o")
opts = dict(opts)

debug = "-d" in opts

if cmd == "parse":
    qtext = " ".join(args)
    print("Query text:'%s'" % (qtext,))
    q = parse_query(qtext)
    print("Converted:---------------")
    print(q.Tree.pretty("    "))
    if "-o" in opts:
        q.skip_assembly()
        optimized = q.optimize(debug=debug)
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
    q = parse_query(qtext, debug=debug)
    #print("connecting to db...")
    db = connect(config)
    #print("connected to db")
    results = q.run(db, debug=True, filters=filters, with_meta=True, with_provenance=False)
    print("Query results:")
    for r in results:
        print(r)
else:
    print(Usage)
    sys.exit(2)
    
    
