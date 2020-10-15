from dbobjects import DBFile
import psycopg2, sys, json

connstr = sys.argv[1]

conn = psycopg2.connect(connstr)

files = DBFile.list(conn)

c = conn.cursor()
c.execute("""
    create table if not exists file_metadata
    ( 
        file_id   text    primary key,
        metadata    jsonb
    );
    
    create index if not exists
        meta_meta_index on file_metadata using gin (metadata);
""")

n = 0
buf = []
for f in files:
    #print(f.FID, f.metadata())
    buf.append((f.FID, json.dumps(f.metadata())))
    if len(buf) > 1000:
        c.executemany("""
            insert into file_metadata(file_id, metadata)
            values (%s, %s)""", buf)
        c.execute("commit")
        buf = []
    n += 1
    if n % 1000 == 0:   print(n)

if buf:
        c.executemany("""
            insert into file_metadata(file_id, metadata)
            values (%s, %s)""", buf)
        c.execute("commit")
        buf = []
print(n)    
    
