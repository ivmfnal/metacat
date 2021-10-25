import sys, psycopg2, yaml
from dbobjects import DBDataset

cfg = yaml.load(open("cfg.yaml", "r"))

in_conn_str = cfg["in"]
out_conn_str = cfg["out"]

in_conn = psycopg2.connect(in_conn_str)
out_conn = psycopg2.connect(out_conn_str)

namespace = "dune"

cin = in_conn.cursor()
cin.execute("select distinct file_type_desc from file_types")
for (dsname,) in cin.fetchall():
    DBDataset(out_conn, namespace, dsname).save()
    print("dataset %s:%s created" % (namespace, dsname))

