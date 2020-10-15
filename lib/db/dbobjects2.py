import uuid, json, hashlib, re
from metacat.util import to_bytes, to_str
from psycopg2 import IntegrityError

Debug = False

def debug(*parts):
    if Debug:
        print(*parts)

class AlreadyExistsError(Exception):
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
            
class DBFileSet(object):
    
    def __init__(self, db, files=[], limit=None):
        self.DB = db
        self.Files = files
        self.Limit = limit

    def limit(self, n):
        return DBFileSet(self.DB, self.Files, n)
        
    @staticmethod
    def from_shallow(db, g):
        # g is genetator of tuples (fid, namespace, name)
        return DBFileSet(db, (
            DBFile(db, namespace, name, fid=fid) for fid, namespace, name in g
        ))
        

    @staticmethod
    def from_tuples(db, g):
        return DBFileSet(db, 
            (
                DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta)
                for fid, namespace, name, meta in g
            )
        )
        
    @staticmethod
    def from_id_list(db, lst):
        c = db.cursor()
        c.execute("""
            select id, namespace, name, metadata from files
                where id = any(%s)""", (list(lst),))
        return DBFileSet.from_tuples(db, fetch_generator(c))
    
    @staticmethod
    def from_name_list(db, names, default_namespace=None):
        full_names = [parse_name(x, default_namespace) for x in names]
        just_names = [name for ns, name in full_names]
        joined = set("%s:%s" % t for t in full_names)
        c = db.cursor()
        c.execute("""
            select id, namespace, name, metadata from files
                where name = any(%s)""", (just_names,))
        selected = ((fid, namespace, name, metadata) 
                    for (fid, namespace, name, metadata) in fetch_generator(c)
                    if "%s:%s" % (namespace, name) in joined)
        return DBFileSet.from_tuples(db, selected)
    
    def __iter__(self):
        return limited(self.Files, self.Limit)
                        
    def as_list(self):
        # list(DBFileSet) should work too
        return list(self.Files)
            
    def parents(self, with_metadata = False):
        return self._relationship("parents", with_metadata)
            
    def children(self, with_metadata = False):
        return self._relationship("children", with_metadata)
            
    def _relationship(self, rel, with_metadata):
        if rel == "children":
            join = "f.id = pc.child_id and pc.parent_id = any (%s)"
        else:
            join = "f.id = pc.parent_id and pc.child_id = any (%s)"
            
        c = self.DB.cursor()
        file_ids = list(f.FID for f in self.Files)
        if with_metadata:
            sql = f"""select distinct f.id, f.namespace, f.name, f.metadata
                        from files f, parent_child pc
                        where {join}
                        """
        else:
            sql = f"""select distinct f.id, f.namespace, f.name, null
                        from files f, parent_child pc
                        where {join}
                        """
        c.execute(sql, (file_ids,))
        return DBFileSet.from_tuples(self.DB, fetch_generator(c))

    @staticmethod
    def join(db, file_sets):
        first = file_sets[0]
        if len(file_sets) == 1:
            return first
        file_list = list(first)
        file_ids = set(f.FID for f in file_list)
        for another in file_sets[1:]:
            another_ids = set(f.FID for f in another)
            file_ids &= another_ids
        return DBFileSet(db, (f for f in file_list if f.FID in file_ids))

    @staticmethod
    def union(db, file_sets):
        def union_generator(file_lists):
            file_ids = set()
            for lst in file_lists:
                #print("DBFileSet.union: lst:", lst)
                for f in lst:
                    if not f.FID in file_ids:
                        file_ids.add(f.FID)
                        yield f
        gen = union_generator(file_sets)
        #print("DBFileSet.union: returning:", gen)
        return DBFileSet(db, gen)

    def subtract(self, right):
        right_ids = set(f.FID for f in right)
        #print("DBFileSet: right_ids:", len(right_ids))
        return DBFileSet(self.DB, (f for f in self if not f.FID in right_ids))
        
    __sub__ = subtract
    
    @staticmethod
    def from_basic_query(db, basic_file_query, with_metadata, limit):
        
        if limit is None:
            limit = basic_file_query.Limit
        elif basic_file_query.Limit is not None:
            limit = min(limit, basic_file_query.Limit)
            
        dataset_selector = basic_file_query.DatasetSelector
        datasets = None
        if dataset_selector is not None:
            datasets = list(basic_file_query.DatasetSelector.datasets(db))
            if not datasets:
                return DBFileSet(db)      # empty File Set

        if dataset_selector is None:
            return DBFileSet.all_files(db, dnf, with_metadata, limit)
            
        elif len(datasets) == 1:
            return datasets[0].list_files(with_metadata = with_metadata, condition=basic_file_query.Wheres, limit=limit,
                        relationship = basic_file_query.Relationship)
        else:
            return DBFileSet.union(
                        ds.list_files(
                            with_metadata = with_metadata, condition=basic_file_query.Wheres,
                            relationship = basic_file_query.Relationship, limit=limit
                        )
                        for ds in datasets
            )
        
    @staticmethod
    def all_files(db, meta_exp, with_metadata, limit):
        #print("DBDataset.all_files: dnf:", dnf)
        meta_where_clause = MetaExpressionDNF(meta_exp).sql("files")
        limit = "" if limit is None else f"limit {limit}"   
        meta = "files.metadata" if with_metadata else "null"     
        sql = f"""select files.id, files.namespace, files.name, {meta}
                                        from files
                                        where {meta_where_clause}
                                        {limit}
                                        """

        #print("DBDataset.all_files: sql:", sql)
        c = db.cursor()
        c.execute(sql)
        return DBFileSet.from_tuples(db, fetch_generator(c))
        

        
class DBFile(object):

    def __init__(self, db, namespace = None, name = None, metadata = None, fid = None):
        assert (namespace is None) == (name is None)
        self.DB = db
        self.FID = fid or uuid.uuid4().hex
        self.FixedFID = (fid is not None)
        self.Namespace = namespace
        self.Name = name
        self.Metadata = metadata or {}
        self.Creator = None
        self.CreatedTimestamp = None
    
    ID_BITS = 64
    ID_NHEX = ID_BITS/4
    ID_FMT = f"%0{ID_NHEX}x"
    ID_MASK = (1<<ID_BITS) - 1
    
    def generate_id(self):          # not used. Use 128 bit uuid instead to guarantee uniqueness
        x = uuid.uuid4().int
        fid = 0
        while x:
            fid ^= (x & self.ID_MASK)
            x >>= self.ID_BITS
        return self.ID_FMT % fid
        
    def __str__(self):
        return "[DBFile %s %s:%s]" % (self.FID, self.Namespace, self.Name)
        
    __repr__ = __str__

    def create(self, do_commit = True):
        from psycopg2 import IntegrityError
        c = self.DB.cursor()
        try:
            meta = json.dumps(self.Metadata or {})
            c.execute("""
                insert into files(id, namespace, name, metadata) values(%s, %s, %s, %s)
                """,
                (self.FID, self.Namespace, self.Name, meta))
            if do_commit:   c.execute("commit")
        except IntegrityError:
            c.execute("rollback")
            raise AlreadyExistsError("%s:%s" % (self.Namespace, self.Name))
        except:
            c.execute("rollback")
            raise
        return self


    @staticmethod
    def create_many(db, files, do_commit=True):
        from psycopg2 import IntegrityError
        tuples = [
            (f.FID, f.Namespace, f.Name, json.dumps(f.Metadata or {}))
            for f in files
        ]
        #print("tuples:", tuples)
        c = db.cursor()
        try:
            c.executemany("""
                insert 
                    into files(id, namespace, name, metadata) 
                    values(%s, %s, %s, %s)
                """,
                tuples)
            if do_commit:   c.execute("commit")
        except IntegrityError:
            c.execute("rollback")
            raise AlreadyExistsError("multiple")
        except:
            c.execute("rollback")
            raise
            
        for f in files: f.DB = db

        
    def update(self, do_commit = True):
        from psycopg2 import IntegrityError
        c = self.DB.cursor()
        meta = json.dumps(self.Metadata or {})
        try:
            c.execute("""
                update files set namespace=%s, name=%s, metadata=%s where id = %s
                """, (self.Namespace, self.Name, meta, self.FID)
            )
            if do_commit:   c.execute("commit")
        except:
            c.execute("rollback")
            raise    
        return self
        
    @staticmethod
    def update_many(db, files, do_commit=True):
        from psycopg2 import IntegrityError
        tuples = [
            (f.Namespace, f.Name, json.dumps(f.Metadata or {}), f.FID)
            for f in files
        ]
        #print("tuples:", tuples)
        c = db.cursor()
        try:
            c.executemany("""
                update files
                    set namespace=%s, name=%s, metadata=%s
                    where id=%s
                """,
                tuples)
            if do_commit:   c.execute("commit")
        except:
            c.execute("rollback")
            raise
        for f in files: f.DB = db
        
    @staticmethod
    def get(db, fid = None, namespace = None, name = None, with_metadata = False):
        assert (fid is not None) != (namespace is not None or name is not None), "Can not specify both FID and namespace.name"
        assert (namespace is None) == (name is None)
        c = db.cursor()
        if fid is not None:
            c.execute("""select id, namespace, name, metadata from files
                    where id = %s""", (fid,))
        else:
            c.execute("""select id, namespace, name, metadata 
                    from files
                    where namespace = %s and name=%s""", (namespace, name))
        tup = c.fetchone()
        if not tup: return None
        fid, namespace, name, meta = tup
        meta = meta or {}
        return DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta)
            
    @staticmethod
    def exists(db, fid = None, namespace = None, name = None):
        print("DBFile.exists:", fid, namespace, name)
        if fid is not None:
            assert (namespace is None) and (name is None),  "If FID is specified, namespace and name must be null"
        else:
            assert (namespace is not None) and (name is not None), "Both namespace and name must be specified"
        c = db.cursor()
        if fid is not None:
            c.execute("""select namespace, name 
                    from files
                    where id = %s""", (fid,))
        else:
            c.execute("""select id 
                    from files
                    where namespace = %s and name=%s""", (namespace, name))
        return c.fetchone() != None
        
    def fetch_metadata(self):
        c = self.DB.cursor()
        c.execute("""
            select metadata
                from files
                where id=%s""", (self.FID,))
        meta = None
        tup = c.fetchone()
        if tup is not None:
            meta = tup[0] or {}
        return meta
        
    def with_metadata(self):
        if not self.Metadata:
            self.Metadata = self.fetch_metadata()
        return self
    
    def metadata(self):
        if not self.Metadata:
            self.Metadata = self.fetch_metadata()
        return self.Metadata
        
    @staticmethod
    def list(db, namespace=None):
        c = db.cursor()
        if namespace is None:
            c.execute("""select id, namespace, name from files""")
        else:
            c.execute("""select id, namespace, name from files
                where namespace=%s""", (namespace,))
        return DBFileSet.from_shallow(db, fetch_generator(c))
        
    def has_attribute(self, attrname):
        return attrname in self.Metadata
        
    def get_attribute(self, attrname, default=None):
        return self.Metadata.get(attrname, default)

    def to_jsonable(self, with_metadata = False, with_relations=False):
        data = dict(
            fid = self.FID,
            namespace = self.Namespace,
            name = self.Name,
            children = [c.FID for c in self.children()],
            parents = [p.FID for p in self.parents()]
        )
        if with_metadata:
            data["metadata"] = self.metadata()
        if with_relations:
            data["parents"] = [{
                "fid":p.FID,
                "namespace":p.Namespace,
                "name":p.Name
            } for p in self.parents()]
            data["children"] = [{
                "fid":c.FID,
                "namespace":c.Namespace,
                "name":c.Name
            } for c in self.children()]
            data["datasets"] = [{
                "namespace":ds.Namespace, "name":ds.Name
            } for ds in self.datasets()]
        return data

    def to_json(self, with_metadata = False, with_relations=False):
        return json.dumps(self.to_jsonable(with_metadata=with_metadata, with_relations=with_relations))
        
    def children(self, with_metadata = False):
        return DBFileSet(self.DB, [self]).children(with_metadata)
        
    def parents(self, with_metadata = False):
        return DBFileSet(self.DB, [self]).parents(with_metadata)
        
    def add_child(self, child, do_commit=True):
        child_fid = child if isinstance(child, str) else child.FID
        c = self.DB.cursor()
        c.execute("""
            insert into parent_child(parent_id, child_id)
                values(%s, %s)        
                on conflict(parent_id, child_id) do nothing;
            """, (self.FID, child_fid)
        )
        if do_commit:   c.execute("commit")
        
    def add_parents(self, parents, do_commit=True):
        parent_fids = [(p if isinstance(p, str) else p.FID,) for p in parents]
        c = self.DB.cursor()
        c.executemany(f"""
            insert into parent_child(parent_id, child_id)
                values(%s, '{self.FID}')        
                on conflict(parent_id, child_id) do nothing;
            """, parent_fids
        )
        if do_commit:   c.execute("commit")
        
    def set_parents(self, parents, do_commit=True):
        parent_fids = [(p if isinstance(p, str) else p.FID,) for p in parents]
        c = self.DB.cursor()
        #print("set_parents: fids:", parent_fids)
        c.execute(f"delete from parent_child where child_id='{self.FID}'")
        c.executemany(f"""
            insert into parent_child(parent_id, child_id)
                values(%s, '{self.FID}')        
                on conflict(parent_id, child_id) do nothing;
            """, parent_fids
        )
        if do_commit:   c.execute("commit")
        
    def remove_child(self, child, do_commit=True):
        child_fid = child if isinstance(child, str) else child.FID
        c = self.DB.cursor()
        c.execute("""
            delete from parent_child where
                parent_id = %s and child_id = %s;
            """, (self.FID, child_fid)
        )
        if do_commit:   c.execute("commit")

    def add_parent(self, parent, do_commit=True):
        parent_fid = parent if isinstance(parent, str) else parent.FID
        return DBFile(self.DB, fid=parent_fid).add_child(self, do_commit=do_commit)
        
    def remove_parent(self, parent, do_commit=True):
        parent_fid = parent if isinstance(parent, str) else parent.FID
        return DBFile(self.DB, fid=parent_fid).remove_child(self, do_commit=do_commit)
        
    def datasets(self):
        # list all datasets this file is found in
        c = self.DB.cursor()
        c.execute("""
            select fds.dataset_namespace, fds.dataset_name
                from files_datasets fds
                where fds.file_id = %s
                order by fds.dataset_namespace, fds.dataset_name""", (self.FID,))
        return (DBDataset(self.DB, namespace, name) for namespace, name in fetch_generator(c))
        
class MetaExpressionDNF(object):
    
    def __init__(self, exp):
        #
        # meta_exp is a nested list representing the query filter expression in DNF:
        #
        # meta_exp = [meta_or, ...]
        # meta_or = [meta_and, ...]
        # meta_and = [(op, aname, avalue), ...]
        #
        debug("===MetaExpressionDNF===")
        self.Exp = None
        self.DNF = None
        if exp is not None:
            #
            # converts canonic Node expression (meta_or of one or more meta_ands) into nested or-list or and-lists
            #
            #assert isinstance(self.Exp, Node)
            assert exp.T == "meta_or"
            for c in exp.C:
                assert c.T == "meta_and"
    
            or_list = []
            for and_item in exp.C:
                or_list.append(and_item.C)
            self.DNF = or_list

        #print("MetaExpressionDNF: exp:", self.DNF)
        #self.validate_exp(meta_exp)
        
    def __str__(self):
        return self.file_ids_sql()
        
    __repr__= __str__
    
    def sql_and(self, and_term, table_name):

        def sql_literal(v):
            if isinstance(v, str):       v = "'%s'" % (v,)
            elif isinstance(v, bool):    v = "true" if v else "false"
            elif v is None:              v = "null"
            else:   v = str(v)
            return v
            
        def json_literal(v):
            if isinstance(v, str):       v = '"%s"' % (v,)
            else:   v = sql_literal(v)
            return v
            
        def pg_type(v):
            if isinstance(v, bool):   pgtype='boolean'
            elif isinstance(v, str):   pgtype='text'
            elif isinstance(v, int):   pgtype='bigint'
            elif isinstance(v, float):   pgtype='double precision'
            else:
                raise ValueError("Unrecognized literal type: %s %s" % (v, type(v)))
            return pgtype
            
        contains_items = []
        parts = []
        for exp in and_term:
            debug("sql_and:")
            debug(exp.pretty("    "))

            op = exp.T
            args = exp.C

            if op == "present":
                aname = exp["name"]
                parts.append(f"{table_name}.metadata ? '{aname}'")

            elif op == "not_present":
                aname = exp["name"]
                parts.append(f"not ({table_name}.metadata ? '{aname}')")
            
            else:
                arg = args[0]
                
                if arg.T == "array_subscript":
                    # a[i] = x
                    aname, inx = arg["name"], arg["index"]
                    inx = json_literal(inx)
                    subscript = f"[{inx}]"
                elif arg.T == "array_any":
                    aname = arg["name"]
                    subscript = "[*]"
                else:
                    aname = arg["name"]
                    subscript = ""
            

                if op == "in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if arg.T == "array_subscript" or arg.T == "scalar":
                        # a[i] in x:y or a in x:y
                        parts.append(f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} >={low}'")
                        parts.append(f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} <={high}'")
                    elif arg.T == "array_any":
                        # a[*] in x:y
                        parts.append(f"{table_name}.metadata @? '$.\"{aname}\"[*] ? (@ >= {low} && @ <= {high})'")
                    elif arg.T == "array_length":
                        parts.append(f"jsonb_array_length({table_name}.metadata -> '{aname}') between {low} and {high}")
                        

                elif op == "in_set":
                    if arg.T == "array_length":
                        values = exp["set"]
                        or_parts = [f"(jsonb_array_length({table_name}.metadata -> '{aname}') = {v})" for v in values]
                    else:
                        values = [json_literal(x) for x in exp["set"]]
                        or_parts = [f"({table_name}.metadata @@ '$.\"{aname}\"{subscript} == {v}')" for v in values]
                    parts.append("(%s)" % (" or ".join(or_parts),))
                    
                elif op == "cmp_op":
                    cmp_op = exp["op"]
                    value = args[1]
                    value_type, value = value.T, value["value"]
                    if arg.T == "array_length":
                        parts.append(f"jsonb_array_length({table_name}.metadata -> '{aname}') {cmp_op} {value}")
                    else:
                        value = json_literal(value)
                        if cmp_op in (">", ">=", "<", "<=", "=", "==", "!="):
                            if cmp_op == '=': cmp_op = "=="
                            parts.append(f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} {cmp_op} {value}'")
                        elif cmp_op in ("~", "~*", "!~", "!~*"):
                            negated = cmp_op.startswith('!')
                            if negated: cmp_op = cmp_op[1:]
                            flags = ' flag "i"' if cmp_op.endswith("*") else ''
                            part = f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} like_regex {value}{flags}'"
                            if negated:
                                part = "not "+part
                            parts.append(part)

        if contains_items:
            parts.append("%s.metadata @> '{%s}'" % (table_name, ",".join(contains_items )))
            
        if Debug:
            print("sql_and():")
            print(" and_terms:")
            for t in and_term:
                print(t.pretty("    "))
            print("output parts:")
            for p in parts:
                print("      ", p)
            
        return " and ".join([f"({p})" for p in parts])
        
    def sql(self, table_name):
        if self.DNF:
            return " or ".join([self.sql_and(t, table_name) for t in self.DNF])
        else:
            return " true "
            
class DBDataset(object):

    def __init__(self, db, namespace, name, parent_namespace=None, parent_name=None, frozen=False, monotonic=False):
        assert namespace is not None and name is not None
        assert (parent_namespace is None) == (parent_name == None)
        self.DB = db
        self.Namespace = namespace
        self.Name = name
        self.ParentNamespace = parent_namespace
        self.ParentName = parent_name
        self.SQL = None
        self.Frozen = frozen
        self.Monotonic = monotonic
        self.Creator = None
        self.CreatedTimestamp = None
    
    def __str__(self):
        return "DBDataset(%s:%s)" % (self.Namespace, self.Name)
        
    def save(self, do_commit = True):
        c = self.DB.cursor()
        namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace
        parent_namespace = self.ParentNamespace.Name if isinstance(self.ParentNamespace, DBNamespace) else self.ParentNamespace
        c.execute("""
            insert into datasets(namespace, name, parent_namespace, parent_name, frozen, monotonic) values(%s, %s, %s, %s, %s, %s)
                on conflict(namespace, name) 
                    do update set parent_namespace=%s, parent_name=%s, frozen=%s, monotonic=%s
            """,
            (namespace, self.Name, parent_namespace, self.ParentName, self.Frozen, self.Monotonic, 
                    parent_namespace, self.ParentName, self.Frozen, self.Monotonic))
        if do_commit:   c.execute("commit")
        return self
            
    def add_file(self, f, do_commit = True):
        assert isinstance(f, DBFile)
        c = self.DB.cursor()
        c.execute("""
            insert into files_datasets(file_id, dataset_namespace, dataset_name) values(%s, %s, %s)
                on conflict do nothing""",
            (f.FID, self.Namespace, self.Name))
        if do_commit:   c.execute("commit")
        return self
        
    def add_files(self, files, do_commit=True):
        c = self.DB.cursor()
        c.executemany(f"""
            insert into files_datasets(file_id, dataset_namespace, dataset_name) values(%s, '{self.Namespace}', '{self.Name}')
                on conflict do nothing""", ((f.FID,) for f in files))
        if do_commit:
            c.execute("commit")
        return self
        
        
    @staticmethod
    def get(db, namespace, name):
        c = db.cursor()
        namespace = namespace.Name if isinstance(namespace, DBNamespace) else namespace
        #print(namespace, name)
        c.execute("""select parent_namespace, parent_name, frozen, monotonic
                        from datasets
                        where namespace=%s and name=%s""",
                (namespace, name))
        tup = c.fetchone()
        if tup is None: return None
        parent_namespace, parent_name, frozen, monotonic = tup
        dataset = DBDataset(db, namespace, name, parent_namespace, parent_name)
        dataset.Frozen = frozen
        dataset.Monotonic = monotonic
        return dataset

    @staticmethod
    def exists(db, namespace, name):
        return DBDataset.get(db, namespace, name) is not None

    @staticmethod
    def list(db, namespace=None, parent_namespace=None, parent_name=None):
        namespace = namespace.Name if isinstance(namespace, DBNamespace) else namespace
        parent_namespace = parent_namespace.Name if isinstance(parent_namespace, DBNamespace) else parent_namespace
        wheres = []
        if namespace is not None:
            wheres.append("namespace = '%s'" % (namespace,))
        if parent_namespace is not None:
            wheres.append("parent_namespace = '%s'" % (parent_namespace,))
        if parent_name is not None:
            wheres.append("parent_name = '%s'" % (parent_name,))
        wheres = "" if not wheres else "where " + " and ".join(wheres)
        c=db.cursor()
        c.execute("""select namespace, name, parent_namespace, parent_name, frozen, monotonic
                from datasets %s""" % (wheres,))
        return (DBDataset(db, namespace, name, parent_namespace, parent_name, frozen, monotonic) for
                namespace, name, parent_namespace, parent_name, frozen, monotonic in fetch_generator(c))




    def list_files(self, recursive=False, with_metadata = False, condition=None, relationship=None,
                limit=None):
        # condition is the filter condition in DNF nested list format

        limit = "" if limit is None else f"limit {limit}"        
        if relationship is None:
            meta = "f.metadata" if with_metadata else "null"
            meta_where_clause = MetaExpressionDNF(condition).sql("f")
            sql = f"""select f.id, f.namespace, f.name, {meta}
                        from files f
                        inner join files_datasets fd on fd.file_id = f.id
                        where fd.dataset_namespace='{self.Namespace}' and fd.dataset_name='{self.Name}' and
                            {meta_where_clause}
                        {limit}
                        """
        elif relationship == "children_of":
            meta = "c.metadata" if with_metadata else "null"
            meta_where_clause = MetaExpressionDNF(condition).sql("p")
            sql = f"""select c.id, c.namespace, c.name, {meta}
                        from files c
                            inner join parent_child pc on c.id = pc.child_id
                            inner join files p on p.id = pc.parent_id
                            inner join files_datasets fd on fd.file_id = p.id
                        where fd.dataset_namespace='{self.Namespace}' and fd.dataset_name='{self.Name}' and
                            {meta_where_clause}
                        {limit}
                        """
        elif relationship == "parents_of":
            meta = "p.metadata" if with_metadata else "null"
            meta_where_clause = MetaExpressionDNF(condition).sql("c")
            sql = f"""select p.id, p.namespace, p.name, {meta}
                        from files p
                            inner join parent_child pc on p.id = pc.parent_id
                            inner join files c on c.id = pc.child_id
                            inner join files_datasets fd on fd.file_id = c.id
                        where fd.dataset_namespace='{self.Namespace}' and fd.dataset_name='{self.Name}' and
                            {meta_where_clause}
                        {limit}
                        """
            
        if Debug:
            print("DBDataset.list_files: sql:", sql)
        self.SQL = sql 
        c = self.DB.cursor()
        c.execute(sql)
        return DBFileSet.from_tuples(self.DB, fetch_generator(c))



    @property
    def nfiles(self):
        c = self.DB.cursor()
        c.execute("""select count(*) 
                        from files_datasets 
                        where dataset_namespace=%s and dataset_name=%s""", (self.Namespace, self.Name))
        return c.fetchone()[0]     
    
    def to_jsonable(self):
        return dict(
            namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace,
            name = self.Name,
            parent_namespace = self.ParentNamespace.Name if isinstance(self.ParentNamespace, DBNamespace) else self.ParentNamespace,
            parent_name = self.ParentName
        )
    
    def to_json(self):
        return json.dumps(self.to_jsonable())
        
    @staticmethod
    def list_datasets(db, patterns, with_children, recursively, having, limit=None):
        #
        # does not use "having" yet !
        #
        datasets = set()
        c = db.cursor()
        #print("DBDataset.list_datasets: patterns:", patterns)
        for pattern in patterns:
            match = pattern["wildcard"]
            namespace = pattern["namespace"]
            name = pattern["name"]
            #print("list_datasets: match, namespace, name:", match, namespace, name)
            if match:
                c.execute("""select namespace, name from datasets
                            where namespace = %s and name like %s""", (namespace, name))
            else:
                c.execute("""select namespace, name from datasets
                            where namespace = %s and name = %s""", (namespace, name))
            for namespace, name in c.fetchall():
                #print("list_datasets: add", namespace, name)
                datasets.add((namespace, name))
        #print("list_datasets: with_children:", with_children)
        if with_children:
            parents = datasets.copy()
            children = set()
            parents_scanned = set()
            while parents:
                this_level_children = set()
                for pns, pn in parents:
                    c.execute("""select namespace, name from datasets
                                where parent_namespace = %s and parent_name=%s""",
                                (pns, pn))
                    for ns, n in c.fetchall():
                        this_level_children.add((ns, n))
                parents_scanned |= parents
                datasets |= this_level_children
                if recursively:
                    parents = this_level_children - parents_scanned
                else:
                    parents = set()
        return limited((DBDataset.get(db, namespace, name) for namespace, name in datasets), limit)

    @staticmethod    
    def apply_dataset_selector(db, dataset_selector, limit):
        patterns = dataset_selector.Patterns
        with_children = dataset_selector.WithChildren
        recursively = dataset_selector.Recursively
        having = dataset_selector.Having
        return DBDataset.list_datasets(db, patterns, with_children, recursively, having, limit)
        
class DBNamedQuery(object):

    def __init__(self, db, namespace, name, source, parameters=[]):
        assert namespace is not None and name is not None
        self.DB = db
        self.Namespace = namespace
        self.Name = name
        self.Source = source
        self.Parameters = parameters
        self.Creator = None
        self.CreatedTimestamp = None
        
    def save(self):
        self.DB.cursor().execute("""
            insert into queries(namespace, name, source, parameters) values(%s, %s, %s, %s)
                on conflict(namespace, name) 
                    do update set source=%s, parameters=%s;
            commit""",
            (self.Namespace, self.Name, self.Source, self.Parameters, self.Source, self.Parameters))
        return self
            
    @staticmethod
    def get(db, namespace, name):
        c = db.cursor()
        print("DBNamedQuery:get():", namespace, name)
        c.execute("""select source, parameters
                        from queries
                        where namespace=%s and name=%s""",
                (namespace, name))
        (source, params) = c.fetchone()
        return DBNamedQuery(db, namespace, name, source, params)
        
    @staticmethod
    def list(db, namespace=None):
        c = db.cursor()
        if namespace is not None:
            c.execute("""select namespace, name, source, parameters
                        from queries
                        where namespace=%s""",
                (namespace,)
            )
        else:
            c.execute("""select namespace, name, source, parameters
                        from queries"""
            )
        return (DBNamedQuery(db, namespace, name, source, parameters) 
                    for namespace, name, source, parameters in fetch_generator(c)
        )

class Authenticator(object):
    
    def __init__(self, username, secrets=[]):
        self.Username = username
        self.Secrets = secrets[:]
    
    @staticmethod
    def from_db(username, typ, secrets):
        if typ == "password":   a = PasswordAuthenticator(username, secrets)
        elif typ == "x509":   a = X509Authenticator(username, secrets)
        else:
            raise ValueError(f"Unknown autenticator type {typ}")
        return a
    
    def addSecret(self, new_secret):
        raise NotImplementedError
        
    def setSecret(self, secret):
        self.Secrets = [secret]
        
    def verifySecret(self, secret):
        raise NotImplementedError
        
class PasswordAuthenticator(Authenticator):
    
    HashAlg = "sha1"
    
    def addSecret(self,new_secret):
        raise NotImplementedError("Can not add secret to a password authenticator. Use setSecret() instead")

    def hash(self, password, alg=None):
        alg = alg or self.HashAlg
        hashed = hashlib.new(alg)
        hashed.update(to_bytes(self.Username))
        hashed.update(b":")
        hashed.update(to_bytes(password))
        return "$%s:%s" % (alg, hashed.digest().hex())
                
    def setSecret(self, plain_password):
        self.Secrets = [self.hash(plain_password)]

    def verifySecret(self, plain_password):
        hashed_secret = self.Secrets[0]
        if hashed_secret.startswith("$") and ":" in hashed_secret:
            alg = hashed_secret[1:].split(":", 1)[0]
            hashed_password = self.hash(plain_password, alg)
            return hashed_password == hashed_password
        else:
            # plain text password in DB ??
            return hashed_secret == plain_password
            
class X509Authenticator(Authenticator):
    
    HashAlg = "sha1"
    
    def addSecret(self, dn):
        if not new_secret in self.Secrets:
            self.Secrets.append(dn)

    def setSecret(self, dn):
        self.Secrets = [dn]

    def verifySecret(self, dn):
        return dn in self.Secrets
            
class DBUser(object):

    def __init__(self, db, username, name, email, flags=""):
        self.Username = username
        self.Name = name
        self.EMail = email
        self.Flags = flags
        self.DB = db
        self.Authenticators = {}        # type -> [secret,...]
        self.Roles = None
        
    def __str__(self):
        return "DBUser(%s, %s, %s, %s)" % (self.Username, self.Name, self.EMail, self.Flags)
        
    __repr__ = __str__
    
    def save(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into users(username, name, email, flags) values(%s, %s, %s, %s)
                on conflict(username) 
                    do update set name=%s, email=%s, flags=%s;
            """,
            (self.Username, self.Name, self.EMail, self.Flags, self.Name, self.EMail, self.Flags))
        
        c.execute("delete from authenticators where username=%s", (self.Username,))
        c.executemany("insert into authenticators(username, type, secrets) values(%s, %s, %s)",
            [(self.Username, typ, a.Secrets) for typ, a in self.Authenticators.items()])
        if do_commit:
            c.execute("commit")
        return self
        
    def set_password(self, password):
        a = self.Authenticators.setdefault("password", PasswordAuthenticator(self.Username))
        a.setSecret(password)
        
    def verify_password(self, password):
        a = self.Authenticators.get("password")
        if not a:
            return False, "No password found"
        if not a.verifySecret(password):
            return False, "Password mismatch"
        return True, "OK"

    @staticmethod
    def get(db, username):
        c = db.cursor()
        c.execute("""select name, email, flags
                        from users
                        where username=%s""",
                (username,))
        tup = c.fetchone()
        if not tup: return None
        (name, email, flags) = tup
        u = DBUser(db, username, name, email, flags)
        c.execute("""select type, secrets from authenticators where username=%s""", (username,))
        u.Authenticators = {typ:Authenticator.from_db(username, typ, secrets) for typ, secrets in c.fetchall()}
        return u
        
    def is_admin(self):
        return "a" in (self.Flags or "")
    
    @staticmethod 
    def list(db):
        rolesdict = {}
        c = db.cursor()
        c.execute("""select u.username, u.name, u.email, u.flags, array_agg(r.name)
                from users u 
                    left outer join roles r on (u.username = any(r.users)) 
                group by u.username, u.name, u.email, u.flags
        """)
        for username, name, email, flags, roles in c.fetchall():
            if roles == [None]: roles = []
            roles = sorted(roles)
            user_roles = []
            for rn in roles:
                r = rolesdict.get(rn)
                if r is None:
                    r = DBRole.get(db, rn)
                    rolesdict[rn] = r
                user_roles.append(user_roles)
            u = DBUser(db, username, name, email, flags)
            u.Roles = user_roles
            #print("DBUser.list: yielding:", u)
            yield u
        
    def roles(self):
        if self.Roles is None:  self.Roles = DBRole.list(self.DB, user = self)
        return self.Roles
        
    def namespaces(self):
        return DBNamespace.list(self.DB, owned_by_user=self)        
        
    def add_role(self, role):
        if isinstance(role, DBRole):
            r = role
        else:
            r = DBRole.get(self.DB, role)
        r.add_user(self)
        r.save()

    def remove_role(self, role):
        if isinstance(role, DBRole):
            r = role
        else:
            r = DBRole.get(self.DB, role)
        r.remove_user(self)
        r.save()

class DBNamespace(object):

    def __init__(self, db, name, owner):
        self.Name = name
        if isinstance(owner, str):
            owner = DBRole.get(db, owner)
        self.Owner = owner
        self.DB = db
        self.Creator = None
        self.CreatedTimestamp = None
        
    def to_jsonable(self):
        return dict(
            name=self.Name,
            owner=self.Owner.Name
        )
        
    def save(self):
        c = self.DB.cursor()
        c.execute("""
            insert into namespaces(name, owner) values(%s, %s)
                on conflict(name) 
                    do update set owner=%s;
            commit
            """,
            (self.Name, self.Owner.Name, self.Owner.Name))
        return self

    def create(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into namespaces(name, owner) values(%s, %s)
            """,
            (self.Name, self.Owner.Name, self.Owner.Name))
        if do_commit:
            c.execute(commit)
        return self
        
    @staticmethod
    def get(db, name):
        c = db.cursor()
        c.execute("""select owner from namespaces where name=%s""", (name,))
        tup = c.fetchone()
        if not tup: return None
        return DBNamespace(db, name, tup[0])
        
    @staticmethod 
    def list(db, owned_by_user=None):
        c = db.cursor()
        c.execute("""select name, owner from namespaces order by name""")
        lst = (DBNamespace(db, name, owner) for  name, owner in c.fetchall())
        if owned_by_user is not None:
            lst = (ns for ns in lst if owned_by_user in ns.Owner)
        return lst

    @staticmethod
    def exists(db, name):
        return DBNamespace.get(db, name) != None

class DBRole(object):

    def __init__(self, db, name, description, users=[]):
        self.Name = name
        self.Description = description
        self.DB = db
        self.Usernames = set(users)          # set of text unsernames
        for u in users:
            if isinstance(u, DBUser):
                u = u.Username
            self.Usernames.add(u)
            
    def __str__(self):
        return "[DBRole %s %s %s]" % (self.Name, self.Description, ",".join(sorted(list(self.Usernames))))
        
    __repr__ = __str__
        
    def save(self, do_commit=True):
        c = self.DB.cursor()
        usernames = sorted(list(self.Usernames))
        c.execute("""
            insert into roles(name, description, users) values(%s, %s, %s)
                on conflict(name) 
                    do update set description=%s, users=%s
            """,
            (self.Name, self.Description, usernames, self.Description, usernames))
        if do_commit:   c.execute("commit")
        return self
        
    @staticmethod
    def get(db, name):
        c = db.cursor()
        c.execute("""select description, users from roles where name=%s""", (name,))
        tup = c.fetchone()
        if not tup: return None
        return DBRole(db, name, tup[0], sorted(tup[1]))
        
    @staticmethod 
    def list(db, user=None):
        c = db.cursor()
        c.execute("""select name, description, users from roles order by name""")
        out = (DBRole(db, name, description, users) for  name, description, users in fetch_generator(c))
        if user is not None:
            if isinstance(user, DBUser):    user = user.Username
            out = (r for r in out if user in r)
        out = list(out)
        #print("DBRole.list:", out)
        return out
        
    def __contains__(self, user):
        if user is None:    return False
        if isinstance(user, DBUser):
            user = user.Username
        #print("__contains__:", self.Usernames)
        return user in self.Usernames

    def add_user(self, user):
        if isinstance(user, DBUser):
            username = user.Username
        else:
            username = user
        self.Usernames.add(username)
        
    def remove_user(self, user):
        if isinstance(user, DBUser):
            username = user.Username
        else:
            username = user
        if username in self.Usernames:
            self.Usernames.remove(username)
            
class DBParamDefinition(object):
    
    Types =  ('int','double','text','boolean',
                'int[]','double[]','text[]','boolean[]')

    def __init__(self, db, name, typ, int_values = None, int_min = None, int_max = None, 
                            double_values = None, double_min = None, double_max = None,
                            text_values = None, text_pattern = None):
        self.DB = db
        self.Name = name
        self.Type = typ
        self.IntValues = int_values
        self.IntMin = int_min
        self.IntMax = int_max
        self.DoubleValues = double_values
        self.DoubleMin = double_min
        self.DoubleMax = double_max
        self.TextValues = text_values if text_pattern is None else set(text_values)
        self.TextPattern = text_pattern if text_pattern is None else re.compile(text_pattern)
        
        # TODO: add booleans, add is_null
        
    def as_jsonable(self):
        dct = dict(name = self.Name, type=self.Type)
        if self.Type in ("int", "int[]"):
            if self.IntMin is not None: dct["int_min"] = self.IntMin
            if self.IntMax is not None: dct["int_max"] = self.IntMax
            if self.IntValues: dct["int_values"] = self.IntValues
        elif self.Type in ("float", "float[]"):
            if self.IntMin is not None: dct["float_min"] = self.FloatMin
            if self.IntMax is not None: dct["float_max"] = self.FloatMax
            if self.IntValues: dct["float_values"] = self.FloatValues
        elif self.Type in ("text", "text[]"):
            if self.TextValues: dct["text_values"] = self.TextValues
            if self.TextPattern: dct["text_pattern"] = self.TextPattern
        return dct
        
    def as_json(self):
        return json.dumps(self.as_jsonable())
            
    @staticmethod
    def from_json(db, x):
        if isinstance(x, str):
            x = json.loads(x)
        d = DBParamDefinition(db, x["name"], x["type"],
            int_values = x.get("int_values"), int_min=x.get("int_min"), int_max=x.get("int_max"),
            float_values = x.get("float_values"), float_min=x.get("float_min"), float_max=x.get("float_max"),
            text_values = x.get("text_values"), text_pattern=x.get("text_pattern")
        )
        return d
        
    def check(self, value):
        if isinstance(value, int):
            ok = (
                (self.IntValues is None or value in self.IntValues) \
                and (self.IntMin is None or value >= self.IntMin) \
                and (self.IntMax is None or value <= self.IntMax) 
            )
            if not ok:  return False
            value = float(value)        # check floating point constraints too
        
        if isinstance(value, float):
            ok = (
                (self.FloatValues is None or value in self.FloatValues) \
                and (self.FloatMin is None or value >= self.FloatMin) \
                and (self.FloatMax is None or value <= self.FloatMax) 
            )
            if not ok:  return False
        
        if isinstance(value, str):
            ok = (
                (self.TextPattern is None or self.TextPattern.match(value) is not None) \
                and (self.TextValues is None or value in self.TextValues)
            )
            
        return ok
        
class DBParamCategory(object):

    def __init__(self, db, path, owner):
        self.Path = path
        self.DB = db
        if isinstance(owner, str):
            owner = DBRole.get(db, owner)
        self.Owner = owner
        self.Restricted = False
        self.Definitions = None           # relpath -> definition
        
    def save(self, do_commit=True):
        c = self.DB.cursor()
        defs = {name:d.to_jsonable() for name, d in self.Definitions.items()}
        defs = json.dumps(defs)
        c.execute("""
            insert into parameter_categories(path, owner, restricted, definitions) values(%{path}s, %{owner}s, %{restricted}s, %{defs}s)
                on conflict(path) 
                    do update set owner=%{owner}s, restricted=%{restricted}s, definitions=%{defs}s
            """,
            dict(path=self.Path, owner=self.Owner.Name, restricted=self.Restricted, defs=defs))
        if do_commit:
            c.execute("commit")
        return self
    
    @staticmethod
    def get(db, path):
        c = db.cursor()
        c.execute("""
            select owner, restricted, definitions from parameter_categories where path=%s""", (path)
        )
        tup = c.fetchone()
        if not tup:
            return None
        owner, restricted, defs = tup
        defs = defs or {}
        cat = DBParamCategory(db, path, owner)
        cat.Restricted = restricted
        cat.Definitions = {name: DBParamDefinition.from_json(d) for name, d in defs.items()}
        return cat
        
    @staticmethod
    def exists(db, path):
        return DBParamCategory.get(db, path) != None
        
    @staticmethod
    def category_for_path(db, path):
        # get the deepest category containing the path
        words = path.split(".")
        paths = ["."]
        p = []
        for w in words:
            p.append(w)
            paths.append(".".join(p))
            
        c = db.cursor()
        c.execute("""
            select path, owner, restricted from parameter_categories where path in %s
                order by path desc limit 1""", (paths,)
        )
        
        tup = c.fetchone()
        cat = None
        if tup:
            path, owner, restricted = tup
            cat = DBParamCategory(db, path, owner)
            cat.Restricted = restricted
        return cat
        
    def check_metadata(self, name, value):
        # name is relative to the category path
        defs = self.definitions
        d = defs.get(name)
        if d is not None:
            if not d.check(v):
                raise ValueError(f"Invalid value for {name}: {v}")
        elif self.Restricted:
            raise ValueError(f"Unknown name {name} in a restricted category")

class DBParamValidator(object):
    
    def __init__(self, db):
        self.DB = db
        self.Categories = {}        # param parent path -> category. Category can be None !
        
    def validate_metadata(self, meta):
        for k, v in sorted(meta.items()):
            words = k.rsplit(".", 1)
            if len(words) != 2:
                parent = ""
                name = k
            else:
                parent, name = words                
            cat = self.Categories.get(parent, -1)
            if cat == -1:
                self.Categories[parent] = cat = DBParamCategory.category_for_path(self.DB, parent)
            if cat is not None:
                cat.check_metadata(name, v)

                
        
    
