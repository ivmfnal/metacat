from expressions3 import Query
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
                (dataset A - dataset B)
        """), 
        ("union",    """
                [
                    dataset test:A, 
                    dataset test:B
                ]
        """),
        
        ("meta_filter, interaection", """
            {   
                dataset test:A, 
                dataset test:B
            } 
            where i > 10
        """),
    
        ("sample",   
        """
                filter sample(0.5) (dataset test:C where b == true)
        """
        ),
    
    
        ("meta int", """
                dataset test:C where i=5
        """)
]

for qn, qtext in queries:
    exp = Query(conn, qtext, default_namespace = "t")
    t0 = time.time()
    out = list(exp.run(filters))
    dt = time.time() - t0
    print (qn,dt," ->   ",sorted([f.Name for f in out]))


        
