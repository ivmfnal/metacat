import sys, psycopg2, yaml
from dbobjects import DBDataset, DBFile

def cursor_fetch(c):
    tup = c.fetchone()
    while tup is not None:
        yield tup
        tup = c.fetchone()


cfg = yaml.load(open("cfg.yaml", "r"))

in_conn_str = cfg["in"]
out_conn_str = cfg["out"]

in_conn = psycopg2.connect(in_conn_str)
out_conn = psycopg2.connect(out_conn_str)

namespace = "dune"

cin = in_conn.cursor()
cin.execute("""
    select f.file_id, f.file_name, t.file_type_desc
        from data_files f
            inner join file_types t on t.file_type_id = f.file_type_id
        order by t.file_type_desc
    """)

ds = None
i = 0
for fid, fn, dsn in cursor_fetch(cin):
    if ds is None or ds.Name != dsn:
        ds = DBDataset(out_conn, namespace, dsn)
    if not DBFile.exists(out_conn, namespace=namespace, name=fn):
        f = DBFile(out_conn, namespace=namespace, name=fn, fid=str(fid)).save(do_commit = False)
        ds.add_file(f, do_commit = False)
        i += 1
        if i % 1000 == 0:
            out_conn.commit()
            print("Files created:", i)

if i % 1000 != 0:
        out_conn.commit()
    

