from expressions4 import Query
import psycopg2, sys, getopt, time
import random
from dbobjects import DBNamedQuery

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

q = """
    dataset test:C where b = true
"""

q = Query(q)
q.to_db(conn, "test", "C_true")
