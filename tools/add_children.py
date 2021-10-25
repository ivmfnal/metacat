import psycopg2, sys, getopt
from dbobjects2 import DBDataset, DBFile, AlreadyExistsError

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

namespace = "test"

files_A = DBDataset(conn, namespace, "A").list_files()
files_B = DBDataset(conn, namespace, "B").list_files()

j = 0

for i, f in enumerate(files_A):
    n = i%4
    for _ in range(n):
        name = "c_%03d_%03d.dat" % (i,j)
        c = DBFile(conn, namespace, name)
        try:    
            c.save()
        except AlreadyExistsError:  
            c = DBFile.get(namespace=namespace, name=name)
        j += 1
        f.add_child(c)
    print("children of %s: %s" % (f, list(f.children())))
    
j = 0
for i, f in enumerate(files_B):
    n = i%4
    for _ in range(n):
        name = "p_%03d_%03d.dat" % (i,j)
        p = DBFile(conn, namespace, name)
        try:    p.save()
        except AlreadyExistsError:
            p = DBFile.get(namespace=namespace, name=name)
        j += 1
        f.add_parent(p)
    print("parents of %s: %s" % (f, list(f.parents())))
    
