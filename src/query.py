from expressions5 import Query
import psycopg2, sys, getopt, time
import random

def sample(inputs, params):
    inp = inputs[0]
    fraction = params[0]
    x = 0.0
    for f in inp:
        x += fraction
        if x >= 1.0:
            x -= 1.0
            yield f
            
filters = dict(sample = sample)

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

queries = [
        ("minus",    
        """
            with namespace="test"
                (dataset A - dataset B) where b == true
        """), 
        ("mult",    """
		with namespace="test"
		(
                    dataset test:A			# comment
                    * dataset test:B
		) where b = true and f < 100 and i < 30
        """),
        
        ("meta_filter, intersection", """
            {   
                dataset test:A, 
                dataset test:B
            } 
            where i > 10
        """),
    
        ("sample",   
        """
                filter sample(0.1) (dataset test:K where b == true)
        """
        ),
    
        ("meta int", """
                dataset test:C where i < 10
        """),
        
        
        ("named", """
            union (
                query test:C_true,
                dataset test:A
            ) where b=true

        """)

]

queries = []

qtext = """
    union(
        dataset test:M where b = true and i < 10 and f < 100.0,
        dataset test:M where b=false and i < 100 and f < 1000.0
    )
"""

exp = Query(qtext, default_namespace = "t")
t0 = time.time()
exp.parse()
dt_parse = time.time() - t0
print("-- parsed --")
print(exp.parse().pretty())
exp.assemble(conn)
print("-- optimized --")
print(exp.optimize().pretty())
t1 = time.time()
out = list(exp.run(conn, filters))
dt_run = time.time() - t1
#print (qn,f"parse: {dt_parse:.3}, run:{dt_run:.3}","\n    -> ",sorted([f.Name for f in out]))
for f in out:
    print(f)

for qn, qtext in queries:
    print("Query '%s': %s" % (qn, qtext))
    exp = Query(qtext, default_namespace = "t")
    t0 = time.time()
    exp.parse()
    dt_parse = time.time() - t0
    print("-- parsed --")
    print(exp.parse().pretty())
    exp.assemble(conn)
    print("-- optimized --")
    print(exp.optimize().pretty())
    t1 = time.time()
    out = list(exp.run(conn, filters))
    dt_run = time.time() - t1
    print (qn,f"parse: {dt_parse:.3}, run:{dt_run:.3}","\n    -> ",sorted([f.Name for f in out]))


        
