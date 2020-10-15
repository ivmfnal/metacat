import psycopg2, sys, getopt
from dbobjects import DBDataset, DBFile

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

namespace = "test"

dataset = DBDataset(conn, namespace, "K").save()


for i in range(1000):
    fn = "%03d" % (i,)
    f = DBFile(conn, namespace, f"{fn}.dat")
    meta = {
        "i":    i,
        "s":    fn,
        "f":    float(i*i),
        "b":    i%2 == 0
    }
    f.save()
    f.save_metadata(meta)
    dataset.add_file(f)

