from psycopg2 import IntegrityError
import itertools, io, csv

Debug = False

def debug(*parts):
    if Debug:
        print(*parts)
        
Aliases = {}
def alias(prefix="t"):
    global Aliases
    i = Aliases.get(prefix, 1)
    Aliases[prefix] = i+1
    return f"{prefix}_{i}"

class AlreadyExistsError(Exception):
    pass

class DatasetCircularDependencyDetected(Exception):
    pass

class NotFoundError(Exception):
    def __init__(self, msg):
        self.Message = msg

    def __str__(self):
        return "Not found error: %s" % (self.Message,)
        
def parse_name(name, default_namespace):
    words = name.split(":", 1)
    if len(words) < 2 or not words[0]:
        assert not not default_namespace, "Null default namespace"
        ns = default_namespace
        name = words[-1]
    else:
        ns, name = words
    return ns, name
                

def fetch_generator(c):
    while True:
        tup = c.fetchone()
        if tup is None: break
        yield tup
        
def first_not_empty(lst):
    val = None
    for v in lst:
        val = v
        if v is not None and not (isinstance(v, list) and len(v) == 0):
            return v
    else:
        return val
        
def limited(iterable, n):
    for f in iterable:
        if n is None:
            yield f
        else:
            if n or n > 0:
                yield f
            else:
                break
            n -= 1
            
def strided(iterable, n, i=0):
    for j, f in enumerate(iterable):
        if j%n == i:
            yield f
            
def skipped(iterable, n):
    for f in iterable:
        if n > 0:
            n -= 1
        else:
            yield f
            
def chunked(iterable, n):
    lst = []
    for item in iterable:
        if len(lst) >= n:
            yield lst
            lst = []
        lst.append(item)
    if lst:
        yield lst
            
class MetaValidationError(Exception):
    
    def __init__(self, message, errors):
        self.Errors = errors
        self.Message = message
        
    def as_json(self):
        return json.dumps(
            {
                "message":self.Message,
                "metadata_errors":self.Errors
            }
        )
        
class DBObject(object):
    
    @classmethod
    def columns(cls, table_name=None, as_text=True, exclude=[]):
        if isinstance(exclude, str):
            exclude = [exclude]
        clist = [c for c in cls.Columns if c not in exclude]
        if table_name:
            clist = [table_name+"."+cn for cn in clist]
        if as_text:
            return ",".join(clist)
        else:
            return clist

def make_list_if_short(iterable, limit):
    # convert iterable to list if it is short. otherwise return another iterable with the same elements
    
    if isinstance(iterable, (list, tuple)):
        return iterable, None
    
    head = []
    if len(head) < limit:
        for x in iterable:
            head.append(x)
            if len(head) > limit:
                return None, itertools.chain(head, iterable)
        else:
            return head, None
    else:
        return None, iterable

def insert_bulk(cursor, table, column_names, tuples, do_commit=True, copy_threshold = 100):

    # if the tuples list or iterable is short enough, do it as multiple inserts
    tuples_lst, tuples = make_list_if_short(tuples, copy_threshold)
    if tuples_lst is not None and len(tuples_lst) <= copy_threshold:
        columns = ",". join(column_names)
        placeholders = ",".join(["%s"]*len(column_names))
        try:
            cursor.executemany(f"""
                insert into parent_child({columns}) values({placeholders})
            """, tuples_lst)
            if do_commit:   cursor.execute("commit")
        except Exception as e:
            cursor.execute("rollback")
            raise
    else:
        
        csv_file = io.StringIO()
        writer = csv.writer(csv_file, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

        for tup in tuples:
            assert len(tup) == len(column_names)
            tup = ["\\N" if x is None else x for x in tup]
            writer.writerow(tup)
        csv_file.seek(0,0)
        try:
            cursor.copy_from(csv_file, table, columns = column_names)
            if do_commit:   cursor.execute("commit")
        except Exception as e:
            cursor.execute("rollback")
            raise
        

class _DBManyToMany(object):
    
    def __init__(self, db, table, *reference_columns, **lookup_values):
        self.DB = db
        self.Table = table
        self.LookupValues = lookup_values
        self.Where = "where " + " and ".join(["%s = '%s'" % (name, value) for name, value in lookup_values.items()])
        assert len(reference_columns) >= 1
        self.ReferenceColumns = list(reference_columns)
        
    def list(self, c=None):
        columns = ",".join(self.ReferenceColumns) 
        if c is None: c = self.DB.cursor()
        c.execute(f"select {columns} from {self.Table} {self.Where}")
        if len(self.ReferenceColumns) == 1:
            return (x for (x,) in fetch_generator(c))
        else:
            return fetch_generator(c)
        
    def __iter__(self):
        return self.list()
        
    def add(self, *vals, c=None):
        assert len(vals) == len(self.ReferenceColumns)
        col_vals = list(zip(self.ReferenceColumns, vals)) + list(self.LookupValues.items())
        cols, vals = zip(*col_vals)
        cols = ",".join(cols)
        vals = ",".join([f"'{v}'" for v in vals])
        if c is None: c = self.DB.cursor()
        c.execute(f"""
            insert into {self.Table}({cols}) values({vals})
                on conflict({cols}) do nothing
        """)
        return self
        
    def contains(self, *vals, c=None):
        assert len(vals) == len(self.ReferenceColumns)
        col_vals = list(zip(self.ReferenceColumns, vals))
        where = self.Where + " and " + " and ".join(["%s='%s'" % (k,v) for k, v in col_vals])
        if c is None: c = self.DB.cursor()
        c.execute(f"""
            select exists(
                    select * from {self.Table} {where} limit 1
            )
        """)
        return c.fetchone()[0]
        
    def __contains__(self, v):
        if not isinstance(v, tuple): v = (v,)
        return self.contains(*v)

    def remove(self, *vals, c=None, all=False):
        assert all or len(vals) == len(self.VarColumns)
        if c is None: c = self.DB.cursor()
        where = self.Where
        if not all:
            col_vals = list(zip(self.ReferenceColumns, vals))
            where += " and " + " and ".join(["%s='%s'" % (k,v) for k, v in col_vals])
        c.execute(f"delete from {self.Table} {where}")
        return self
        
    def set(self, lst, c=None):
        if c is None: c = self.DB.cursor()
        c.execute("begin")
        self.remove(all=True, c=c)
        for tup in lst:
            if not isinstance(tup, tuple):  tup = (tup,)
            self.add(*tup, c=c)
        c.execute("commit")
        
