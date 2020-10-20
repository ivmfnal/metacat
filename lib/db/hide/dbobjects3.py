import uuid, json

class AlreadyExistsError(Exception):
    pass

class NotFoundError(Exception):
    def __init__(self, msg):
        self.Message = msg

    def __str__(self):
        return "Not found error: %s" % (self.Message,)

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
                
class DBFileSet(object):
    
    def __init__(self, db, files, limit=None):
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
        
    def __iter__(self):
        if self.Limit is None:
            return self.Files
        else:
            def limited(lst, n):
                for f in lst:
                    if n > 0:
                        yield f
                    else:
                        break
                    n -= 1
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
        return (f for f in self if not f.FID in right_ids)
        
    __sub__ = subtract
    
    @staticmethod
    def from_data_source(db, data_source, with_metadata, limit):
        datasets = DBDataset.list_datasets(db, data_source.Patterns, data_source.WithChildren, 
                data_source.Recursively, data_source.Having)
        return DBFileSet.union(db, 
                    (   ds.list_files(with_metadata = with_metadata, 
                        condition=data_source.Wheres, limit=limit) 
                        for ds in datasets
                    )
        ).limit(limit)
        
class DBFile(object):

    def __init__(self, db, namespace = None, name = None, metadata = None, fid = None):
        assert (namespace is None) == (name is None)
        self.DB = db
        self.FID = fid or uuid.uuid4().hex
        self.Namespace = namespace
        self.Name = name
        self.Metadata = metadata or {}
        self.Creator = None
        self.CreatedTimestamp = None
        
    def __str__(self):
        return "[DBFile %s %s:%s]" % (self.FID, self.Namespace, self.Name)
        
    __repr__ = __str__

    def save(self, do_commit = True):
        from psycopg2 import IntegrityError
        c = self.DB.cursor()
        try:
            meta = json.dumps(self.Metadata or {})
            c.execute("""
                insert 
                    into files(id, namespace, name, metadata) 
                    values(%s, %s, %s, %s)
                    on conflict(id) do update set namespace=%s, name=%s, metadata=%s""",
                (self.FID, self.Namespace, self.Name, meta, self.Namespace, self.Name, meta))
            if do_commit:   c.execute("commit")
        except IntegrityError:
            c.execute("rollback")
            raise AlreadyExistsError("%s:%s" % (self.Namespace, self.Name))
        return self
                            
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
        assert (fid is not None) != (namespace is not None or name is not None), "Can not specify both FID and namespace.name"
        assert (namespace is None) == (name is None)
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
            data["parents"] = [p.FID for p in self.parents()]
            data["children"] = [c.FID for c in self.children()]
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
    
    def __init__(self, meta_exp):
        #
        # meta_exp is a nested list representing the query filter expression in DNF:
        #
        # meta_exp = [meta_or, ...]
        # meta_or = [meta_and, ...]
        # meta_and = [(op, aname, avalue), ...]
        #
        self.Expression = meta_exp
        #print("validate_exp: exp:", meta_exp)
        self.validate_exp(meta_exp)
        
    def __str__(self):
        return self.file_ids_sql()
        
    __repr__= __str__
    
    @staticmethod
    def validate_exp(exp):
        #print("validate_exp: exp:", exp)
        if exp is not None:
            for and_exp in exp:
                if not isinstance(and_exp, list):
                    raise ValueError("The 'and' expression is not a list: %s" % (repr(and_exp),))
                for cond in and_exp:
                    if not isinstance(cond, tuple) or len(cond) != 3:
                        raise ValueError("The 'condition' expression must be a tuple of length 3, instead: %s" % (repr(cond),))
                    op, aname, aval = cond
                    if not op in (">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*", "in"):
                        raise ValueError("Unrecognized condition operation: %s" % (repr(op,)))                
        

    def sql_and(self, and_term, dataset_namespace, dataset_name):
        #print("sql_and: arg:", and_term)
        assert len(and_term) > 0
        parts = []
        for i, t in enumerate(and_term):
            op, aname, aval = t
            cname = None
            if op == "in":
                if isinstance(aval, bool):    cname = "bool_array"
                elif isinstance(aval, int):       cname = "int_array"
                elif isinstance(aval, float):   cname = "float_array"
                elif isinstance(aval, str):     cname = "string_array"
                else:
                        raise ValueError("Unrecognized value type %s for attribute %s" % (type(aval), aname))
                parts.append(f"a{i}.name='{aname}' and '{aval}' = any(a{i}.{cname})")
            else:
                #print("sql_and: aval:", type(aval), repr(aval))
                if isinstance(aval, bool):    cname = "bool_value"
                elif isinstance(aval, int):       cname = "int_value"
                elif isinstance(aval, float):   cname = "float_value"
                elif isinstance(aval, str):     cname = "string_value"
                else:
                        raise ValueError("Unrecognized value type %s for attribute %s" % (type(aval), aname))
                parts.append(f"a{i}.name='{aname}' and a{i}.{cname} {op} '{aval}'")
        joins = [f"inner join file_attributes a{i} on a{i}.file_id = f.id" for i in range(len(parts))]
        return """
            select f.id as fid
                from files f
                    inner join files_datasets fd on fd.file_id = f.id
                    %s
                where 
                    fd.dataset_namespace = '%s' and fd.dataset_name = '%s' and
                    %s
                """ % (
                " ".join(joins), dataset_namespace, dataset_name, " and ".join(parts))
                
    def sql_and(self, and_term, table_name):
        contains_items = []
        parts = []
        for op, aname, aval in and_term:
            if op in ("=", "=="):
                v = str(aval)
                if isinstance(aval, str):       v = '"%s"' % (aval,)
                elif isinstance(aval, bool):    v = "true" if aval else "false"
                contains_items.append('"%s":%s' % (aname, v))
            elif op == "in":
                val = '"%s"' % (aval,) if isinstance(aval, str) else str(aval)
                contains_items.append('"%s":[%s]' % (aname, val))
            else:
                if isinstance(aval, (int, float)):
                    parts.append(f"({table_name}.metadata ->> '{aname}')::float {op} {aval}")
                elif isinstance(aval, str):
                    parts.append(f"{table_name}.metadata ->> '{aname}' {op} '{aval}'")

        if contains_items:
            parts.append("%s.metadata @> '{%s}'" % (table_name, ",".join(contains_items )))
        return " and ".join(parts)
        
    def sql(self, table_name):
        if self.Expression:
            return " or ".join([self.sql_and(t, table_name) for t in self.Expression])
        else:
            return " true "

class MetaExpressionSQLConverter(object):

    def __init__(self, meta_exp):
        self.Expression = meta_exp

    def sql(self, table_name):
        return self._sql_rec(self.Expression, table_name)
        
    def _term_sql(self, exp):
        op, aname, aval = exp.C
        if op in ("=", "=="):
            v = str(aval)
            if isinstance(aval, str):       v = '"%s"' % (aval,)
            elif isinstance(aval, bool):    v = "true" if aval else "false"
            return '"%s":%s' % (aname, v)
        elif op == "in":
            val = '"%s"' % (aval,) if isinstance(aval, str) else str(aval)
            return '"%s":[%s]' % (aname, val)
        else:
            if isinstance(aval, (int, float)):
                return f"({table_name}.metadata ->> '{aname}')::float {op} {aval}"
            elif isinstance(aval, str):
                return f"{table_name}.metadata ->> '{aname}' {op} '{aval}'"
        
    def _sql_rec(self, exp, table_name):
        if exp_type == "meta_or":
            return "(" + " or ".join([self._sql_rec(e) for e in exp.C]) + ")"
        if exp_type == "meta_and":
            return "(" + " and ".join([self._sql_rec(e) for e in exp.C]) + ")"
        if exp_type == "meta_not":
            return "(not (%s))" % (self._sql_rec(exp.C[0]),)
        else:
            return self._term_sql(exp)
        
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
        
    def add_files(self, files):
        c = self.DB.cursor()
        c.executemany(f"""
            insert into files_datasets(file_id, dataset_namespace, dataset_name) values(%s, '{self.Namespace}', '{self.Name}')
                on conflict do nothing""", ((f.FID,) for f in files))
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

    def list_files(self, recursive=False, with_metadata = False, condition=None, relationship="self",
                limit=None):
        # relationship is ignored for now
        # condition is the filter condition in DNF nested list format

        meta_where_clause = MetaExpressionSQLConverter(condition).sql("files")
        limit = "" if limit is None else f"limit {limit}"        
        if with_metadata:
            sql = f"""select files.id, files.namespace, files.name, files.metadata
                                        from files
                                        inner join files_datasets fd on fd.file_id = files.id
                                        where fd.dataset_namespace='{self.Namespace}' and fd.dataset_name='{self.Name}' and
                                            {meta_where_clause}
                                        {limit}
                                        """
        else:
            sql = f"""select files.id, files.namespace, files.name, null
                                        from files
                                        inner join files_datasets fd on fd.file_id = files.id
                                        where fd.dataset_namespace='{self.Namespace}' and fd.dataset_name='{self.Name}' and
                                            {meta_where_clause}
                                        {limit}
                                        """

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
    def list_datasets(db, patterns, with_children, recursively, having):
        #
        # does not use "having" yet !
        #
        datasets = set()
        c = db.cursor()
        for match, (namespace, name) in patterns:
            #print("list_datasets: match, namespace, name", match, namespace, name)
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
        datasets = (DBDataset.get(db, namespace, name) for namespace, name in datasets)
        return datasets
        
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
        #print(namespace, name)
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
        

class DBUser(object):

    def __init__(self, db, username, name, email, flags=""):
        self.Username = username
        self.Name = name
        self.EMail = email
        self.Flags = flags
        self.DB = db
        self.Authenticators = {}        # type -> [secret,...]
        
    def __str__(self):
        return "DBUser(%s, %s, %s, %s)" % (self.Username, self.Name, self.EMail, self.Flags)
        
    __repr__ = __str__
        
    def save(self):
        c = self.DB.cursor()
        c.execute("""
            insert into users(username, name, email, flags) values(%s, %s, %s, %s)
                on conflict(username) 
                    do update set name=%s, email=%s, flags=%s;
            """,
            (self.Username, self.Name, self.EMail, self.Flags, self.Name, self.EMail, self.Flags))
        
        c.execute("delete from authenticators where username=%s", (self.Username,))
        c.executemany("insert into authenticators(username, type, secrets) values(%s, %s, %s)",
            [(self.Username, typ, lst) for typ, lst in self.Authenticators.items()])
        c.execute("commit")
        return self
        
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
        u.Authenticators = {typ:secrets for typ, secrets in c.fetchall()}
        return u
    
    @staticmethod 
    def list(db):
        c = db.cursor()
        c.execute("""select username, name, email, flags from users order by username""")
        return (DBUser(db, username, name, email, flags) for  username, name, email, flags in c.fetchall())  
        
    def roles(self):
        return DBRole.list(self.DB, user = self)
        
    def namespaces(self):
        return DBNamespace.list(self.DB, owned_by_user=self)        

class DBNamespace(object):

    def __init__(self, db, name, owner):
        self.Name = name
        if isinstance(owner, str):
            owner = DBRole.get(db, owner)
        self.Owner = owner
        self.DB = db
        self.Creator = None
        self.CreatedTimestamp = None
        
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


class DBRole(object):

    def __init__(self, db, name, description, users=[]):
        self.Name = name
        self.Description = description
        self.DB = db
        self.Users = []
        for u in users:
            if isinstance(u, str):
                u = DBUser.get(db, u)
            self.Users.append(u)
            
    def __str__(self):
        return "[DBRole %s %s %s]" % (self.Name, self.Description, ",".join([u.Username for u in self.Users]))
        
    __repr__ = __str__
        
    def save(self):
        c = self.DB.cursor()
        usernames = sorted([u.Username if isinstance(u, DBUser) else u for u in self.Users])
        c.execute("""
            insert into roles(name, description, users) values(%s, %s, %s)
                on conflict(name) 
                    do update set description=%s, users=%s;
            commit
            """,
            (self.Name, self.Description, usernames, self.Description, usernames))
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
        print("DBRole.list:", out)
        return out
        
    def __contains__(self, user):
        if user is None:    return False
        if isinstance(user, DBUser):
            user = user.Username
        #print("__contains__:", self.Users)
        return user in [u.Username for u in self.Users]

        
        
