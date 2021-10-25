import psycopg2, sys, getopt, math
from dbobjects import DBDataset, DBFile

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

namespace = "test"

dataset = DBDataset(conn, namespace, "M").save()


for i in range(1000000):
    fn = "m%06d" % (i,)
    f = DBFile(conn, namespace, f"{fn}.dat")
    meta = {
        "i":    i,
        "s":    fn,
        "f":    math.log(float(i+1)),
        "b":    i%2 == 0
    }
    f.save()
    f.save_metadata(meta)
    dataset.add_file(f)

