from expressions4 import Query
import psycopg2, sys, getopt, time
import random

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

q = """
    union (
        query test:C_true,
        dataset test:A
    ) where b=true
    
"""

q = Query(q)

#print (q.code)

q.parse()

print("--- assembling ---")
a = q.assemble(conn)
print ("assembled:\n", a.pretty())
o = q.optimize()
print ("optomized:\n", o.pretty())
