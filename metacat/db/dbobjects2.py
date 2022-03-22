import uuid, json, hashlib, re, time, io, traceback, base64
from metacat.util import to_bytes, to_str, epoch
from metacat.auth import BaseDBUser
from psycopg2 import IntegrityError

Debug = False

def debug(*parts):
    if Debug:
        print("[debug]", *parts)

from .common import (
    DBObject, _DBManyToMany,
    AlreadyExistsError, DatasetCircularDependencyDetected, NotFoundError, MetaValidationError,
    parse_name, fetch_generator, alias, 
    first_not_empty, limited, strided, skipped, insert_bulk
)

class DBFileSet(object):
    
    def __init__(self, db, files=[]):
        self.DB = db
        self.Files = files
        self.SQL = None

    def limit(self, n):
        return DBFileSet(self.DB, limited(self.Files, n))
        
    def skip(self, n):
        if n == 0:  return self
        return DBFileSet(self.DB, skipped(self.Files, n))
        
    def stride(self, n, i=0):
        return DBFileSet(self.DB, strided(self.Files, n, i))
        
    def chunked(self, chunk_size=1000):
        chunk = []
        for f in self.Files:
            #print("chunked:", f.Namespace, f.Name)
            chunk.append(f)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
        
    @staticmethod
    def from_tuples(db, g):
        # must be in sync with DBFile.all_columns()
        return DBFileSet(db, 
            (
                DBFile.from_tuple(db, t) for t in g
            )
        )
        
    @staticmethod
    def from_id_list(db, lst):
        c = db.cursor()
        columns = DBFile.all_columns()
        c.execute(f"""
            select {columns}
                from   files
                where id = any(%s)""", (list(lst),))
        return DBFileSet.from_tuples(db, fetch_generator(c))
    
    @staticmethod
    def from_name_list(db, names, default_namespace=None):
        full_names = [parse_name(x, default_namespace) for x in names]
        just_names = [name for ns, name in full_names]
        joined = set("%s:%s" % t for t in full_names)
        c = db.cursor()
        columns = DBFile.all_columns()
        c.execute(f"""
            select {columns}, null as parents, null as children from files
                where name = any(%s)""", (just_names,))
        selected = ((fid, namespace, name, metadata) 
                    for (fid, namespace, name, metadata) in fetch_generator(c)
                    if "%s:%s" % (namespace, name) in joined)
        return DBFileSet.from_tuples(db, selected)
        
    def __iter__(self):
        if isinstance(self.Files, list):
            return (f for f in self.Files)
        else:
            return self.Files
                        
    def as_list(self):
        # list(DBFileSet) should work too
        return list(self.Files)
            
    def parents(self, with_metadata = False, with_provenance = False):
        return self._relationship("parents", with_metadata, with_provenance)
            
    def children(self, with_metadata = False, with_provenance = False):
        return self._relationship("children", with_metadata, with_provenance)
            
    def _relationship(self, rel, with_metadata, with_provenance):
        table = "files" if not with_provenance else "files_with_provenance"
        f = alias("f")
        pc = alias("pc")
        attrs = DBFile.attr_columns(f)
        if rel == "children":
            join = f"{f}.id = {pc}.child_id and {pc}.parent_id = any (%s)"
        else:
            join = f"{f}.id = {pc}.parent_id and {pc}.child_id = any (%s)"
            
        meta = "null as metadata" if not with_metadata else f"{f}.metadata"
        provenance = "null as parents, null as children" if not with_provenance else \
            f"{f}.parents, {f}.children"
            
        c = self.DB.cursor()
        file_ids = list(f.FID for f in self.Files)

        sql = f"""select distinct {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {provenance}
                    from {table} {f}, parent_child {pc}
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
        #print("DBFileSet: right_ids:", right_ids)
        return DBFileSet(self.DB, (f for f in self if not f.FID in right_ids))
        
    __sub__ = subtract
    
    @staticmethod
    def from_basic_query(db, basic_file_query, with_metadata, limit):
        
        debug("from_basic_query: with_metadata:", with_metadata)
        
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
    def sql_for_basic_query(db, basic_file_query):
        debug("sql_for_basic_query: bfq:", basic_file_query, " with provenance:", basic_file_query.WithProvenance)
        limit = basic_file_query.Limit
        limit = "" if limit is None else f"limit {limit}"
        offset = "" if not basic_file_query.Skip else f"offset {basic_file_query.Skip}"
        debug("sql_for_basic_query: offset:", offset)
            
        
        f = alias("f")

        meta = f"{f}.metadata" if basic_file_query.WithMeta else "null as metadata"
        parents = f"{f}.parents" if basic_file_query.WithProvenance else "null as parents"
        children = f"{f}.children" if basic_file_query.WithProvenance else "null as children"
        table = "files_with_provenance" if basic_file_query.WithProvenance else "files"

        debug("sql_for_basic_query: table:", table)

        file_meta_exp = MetaExpressionDNF(basic_file_query.Wheres).sql(f) or "true"

        datasets = DBDataset.datasets_for_selectors(db, basic_file_query.DatasetSelectors)
        attrs = DBFile.attr_columns(f)
        if datasets is None:
            # no dataset selection
            sql = f"""
                -- sql_for_basic_query {f}
                    select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
                        from {table} {f}
                        where {file_meta_exp}
                        {limit} {offset}
                -- end of sql_for_basic_query {f}
            """
        else:
            #datasets_sql = DBDataset.sql_for_selector(dataset_selector)
            
            datasets = list(datasets)
            ds_names = set()
            ds_namespaces = set()
            ds_specs = set()
            for ds in datasets:
                ds_names.add(ds.Name)
                ds_namespaces.add(ds.Namespace)
                ds_specs.add(ds.Namespace + ":" + ds.Name)
            
            fd = alias("fd")
            ds = alias("ds")
            
            ds_namespaces = list(ds_namespaces)
            ds_names = list(ds_names)
            ds_specs = list(ds_specs)

            namespace_where = f"{fd}.dataset_namespace = any(array %s)" % (ds_namespaces,) \
                if len(ds_namespaces) > 1 else f"{fd}.dataset_namespace = '%s'" % (ds_namespaces[0],)

            name_where = f"{fd}.dataset_name = any(array %s)" % (ds_names,) \
                if len(ds_names) > 1 else f"{fd}.dataset_name = '%s'" % (ds_names[0],)
                
            specs_where = f"{fd}.dataset_namespace || ':' || {fd}.dataset_name = any(array %s)" % (ds_specs,)\
                if (len(ds_namespaces) > 1 and len(ds_names) > 1) else "true"
        
            sql = f"""
                -- sql_for_basic_query {f}
                    select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
                        from {table} {f}
                            inner join files_datasets {fd} on {fd}.file_id = {f}.id
                        where
                            {namespace_where}
                            and {name_where}
                            and {specs_where}
                            and {file_meta_exp}
                        {limit} {offset}
                -- end of sql_for_basic_query {f}
            """
        debug("sql_for_basic_query: sql:-------\n", sql, "\n---------")
        return sql
        
    @staticmethod
    def sql_for_file_list(spec_list, with_meta, with_provenance, limit):
        f = alias("f")
        meta = f"{f}.metadata" if with_meta else "null as metadata"
        ids = []
        specs = []
        
        for s in spec_list:
            if ':' in s:
                specs.append(s)
            else:
                ids.append(s)
                
        debug("sql_for_file_list: specs, ids:", specs, ids)
                
        ids_part = ""
        specs_part = ""
        
        parts = []
        
        attrs = DBFile.attr_columns(f)

        if with_provenance:
            table = "files_with_provenance"
            prov_columns = f"{f}.parents, {f}.children"
        else:
            table = "files"
            prov_columns = f"null as parents, null as children"
        
        if ids:
            id_list = ",".join(["'%s'" % (i,) for i in ids])
            ids_part = f"""
                select {f}.id, {f}.namespace, {f}.name, {meta}, {prov_columns}, {attrs} from {table} {f}
                    where id in ({id_list})
                """
            parts.append(ids_part)
        
        if specs:
            parsed = [s.split(":",1) for s in specs]
            namespaces, names = zip(*parsed)
            namespaces = list(set(namespaces))
            assert not "" in namespaces
            names = list(set(names))
            
            namespaces = ",".join([f"'{ns}'" for ns in namespaces])
            names = ",".join([f"'{n}'" for n in names])
            specs = ",".join([f"'{s}'" for s in specs])
            
            specs_part = f"""
                select {f}.id, {f}.namespace, {f}.name, {meta}, {prov_columns}, {attrs} from {table} {f}
                    where {f}.name in ({names}) and {f}.namespace in ({namespaces}) and
                         {f}.namespace || ':' || {f}.name in ({specs})
            """
            parts.append(specs_part)

        return "\nunion\n".join(parts)

    @staticmethod
    def from_sql(db, sql):
        c = db.cursor()
        debug("DBFileSet.from_sql: executing sql...")
        c.execute(sql)
        debug("DBFileSet.from_sql: return from execute()")
        fs = DBFileSet.from_tuples(db, fetch_generator(c))
        fs.SQL = sql
        return fs
    

        
class DBFile(object):
    
    ColumnAttributes=[      # column names which can be used in queries
        "creator", "created_timestamp", "name", "namespace", "size"
    ]  
    def __init__(self, db, namespace = None, name = None, metadata = None, fid = None, size=None, checksums=None,
                    parents = None, children = None, creator = None, created_timestamp=None,
                    ):
        
        #print("DBFile.__init__: creator=", creator)            
        
        assert (namespace is None) == (name is None)
        self.DB = db
        self.FID = fid or self.generate_id()
        self.FixedFID = (fid is not None)
        self.Namespace = namespace
        self.Name = name
        self.Metadata = metadata
        self.Creator = creator
        self.CreatedTimestamp = created_timestamp
        self.Checksums = checksums
        self.Size = size
        self.Parents = parents      # list of file ids
        self.Children = children    # list of file ids
    
    @staticmethod
    def generate_id():
        return uuid.uuid4().hex
        
    def __str__(self):
        return "[DBFile %s %s:%s]" % (self.FID, self.Namespace, self.Name)
        
    __repr__ = __str__

    CoreColumnNames = [
        "id", "namespace", "name", "metadata"
    ]
    
    AttrColumnNames = [
        "creator", "created_timestamp", "size", "checksums"
    ]

    AllColumnNames = CoreColumnNames + AttrColumnNames

    @staticmethod
    def all_columns(alias=None, with_meta=False):
        if alias:
            return ','.join(f"{alias}.{c}" for c in DBFile.AllColumnNames)
        else:
            return ','.join(DBFile.AllColumnNames)

    @staticmethod
    def attr_columns(alias=None):
        if alias:
            return ','.join(f"{alias}.{c}" for c in DBFile.AttrColumnNames)
        else:
            return ','.join(DBFile.AttrColumnNames)

    def create(self, creator=None, do_commit = True):
        from psycopg2 import IntegrityError
        c = self.DB.cursor()
        try:
            meta = json.dumps(self.Metadata or {})
            checksums = json.dumps(self.Checksums or {})
            c.execute("""
                insert into files(id, namespace, name, metadata, size, checksums, creator) values(%s, %s, %s, %s, %s, %s, %s)
                """,
                (self.FID, self.Namespace, self.Name, meta, self.Size, checksums, creator))
            if self.Parents:
                insert_bulk(
                    c, 
                    "parent_child", 
                    ["parent_id", "child_id"], 
                    ((p.FID if isinstance(p, DBFile) else p, self.FID) for p in self.Parents),
                    do_commit = False
                )
            if do_commit:   c.execute("commit")
        except:
            c.execute("rollback")
            raise
        return self


    @staticmethod
    def create_many(db, files, creator=None, do_commit=True):
        files = list(files)
        files_csv = []
        parents_csv = []
        null = r"\N"
        for f in files:
            f.FID = f.FID or self.generate_id()
            files_csv.append("%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
                f.FID,
                f.Namespace or null, 
                f.Name or null,
                json.dumps(f.Metadata) if f.Metadata else '{}',
                f.Size if f.Size is not None else null,
                json.dumps(f.Checksums) if f.Checksums else '{}',
                f.Creator or creator or null
            ))
            f.Creator = f.Creator or creator
            if f.Parents:
                parents_csv += ["%s\t%s" % (f.FID, p.FID if isinstance(p, DBFile) else p) for p in f.Parents]
            f.DB = db
        
        c = db.cursor()
        c.execute("begin")

        try:
            files_data = "\n".join(files_csv)
            #open("/tmp/files.csv", "w").write(files_data)
            c.copy_from(io.StringIO("\n".join(files_csv)), "files", 
                    columns = ["id", "namespace", "name", "metadata", "size", "checksums","creator"])
            c.copy_from(io.StringIO("\n".join(parents_csv)), "parent_child", 
                    columns=["child_id", "parent_id"])
            if do_commit:   c.execute("commit")
        except Exception as e:
            c.execute("rollback")
            raise
            
        return DBFileSet(db, files)

        
    def update(self, do_commit = True):
        from psycopg2 import IntegrityError
        c = self.DB.cursor()
        meta = json.dumps(self.Metadata or {})
        checksums = json.dumps(self.Checksums or {})
        try:
            c.execute("""
                update files set namespace=%s, name=%s, metadata=%s, size=%s, checksums=%s where id = %s
                """, (self.Namespace, self.Name, meta, self.Size, checksums, self.FID)
            )
            if do_commit:   c.execute("commit")
        except:
            c.execute("rollback")
            raise    
        return self
        
    @staticmethod
    def from_tuple(db, tup):
        #print("----DBFile.from_tup: tup:", tup)
        if tup is None: return None
        try:
            try:    
                fid, namespace, name, meta, creator, created_timestamp, size, checksums, parents, children = tup
                f = DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta, size=size, checksums = checksums,
                    parents = parents, children=children, creator=creator,
                            created_timestamp=created_timestamp)
            except: 
                try:    
                    fid, namespace, name, meta, creator, created_timestamp, size, checksums = tup
                    f = DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta, size=size, checksums = checksums, creator=creator,
                            created_timestamp=created_timestamp)
                    #print(tup)
                    #print(f.__dict__)
                except: 
                    try:    
                        fid, namespace, name, meta = tup
                        f = DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta)
                    except: 
                            fid, namespace, name = tup
                            f = DBFile(db, fid=fid, namespace=namespace, name=name)
        except:
            raise ValueError("Can not unpack tuple: %s" % (tup,))
        return f

    @staticmethod
    def update_many(db, files, do_commit=True):
        from psycopg2 import IntegrityError
        tuples = [
            (f.Namespace, f.Name, json.dumps(f.Metadata or {}), f.Size, json.dumps(f.Checksums or {}), f.FID)
            for f in files
        ]
        #print("tuples:", tuples)
        c = db.cursor()
        try:
            c.executemany("""
                update files
                    set namespace=%s, name=%s, metadata=%s, size=%s, checksums=%s
                    where id=%s
                """,
                tuples)
            if do_commit:   c.execute("commit")
        except:
            c.execute("rollback")
            raise
        for f in files: f.DB = db
    
    @staticmethod
    def get_files(db, files):
        
        #
        # NOT THREAD SAFE !!
        #
        
        #print("DBFile.get_files: files:", files)
        c = db.cursor()
        strio = io.StringIO()
        for f in files:
            fid = ns = n = None
            if isinstance(f, str):
                if ':' in f:
                    ns, n = f.split(':', 1)
                else:
                    fid = f
            else:
                fid = f.get("fid")
                ns = f.get("namespace")
                n = f.get("name")
            strio.write("%s\t%s\t%s\n" % (fid or r'\N', ns or r'\N', n or r'\N'))
        c.execute("""create temp table if not exists
            temp_files (
                id text,
                namespace text,
                name text);
            truncate table temp_files;
                """)
        c.copy_from(io.StringIO(strio.getvalue()), "temp_files")
        #print("DBFile.get_files: strio:", strio.getvalue())
        
        columns = DBFile.all_columns("f")
        
        sql = f"""
            select {columns}
                 from files f, temp_files t
                 where t.id = f.id or (f.namespace = t.namespace and f.name = t.name)
        """
        
        #c.execute(sql)
        #for row in c.fetchall():
        #    print(row)

        return DBFileSet.from_sql(db, sql)
        
    @staticmethod
    def get(db, fid = None, namespace = None, name = None, with_metadata = False):
        
        assert (fid is not None) != (namespace is not None or name is not None), "Can not specify both FID and namespace.name"
        assert (namespace is None) == (name is None)
        c = db.cursor()
        fetch_meta = "metadata" if with_metadata else "null"
        attrs = DBFile.attr_columns()
        if fid is not None:
            #sql = f"""select id, namespace, name, {fetch_meta}, {attrs} 
            #        from files
            #        where id = %s"""
            #print("sql:", sql)
            c.execute(f"""select id, namespace, name, {fetch_meta}, {attrs} 
                    from files
                    where id = %s""", (fid,))
        else:
            c.execute(f"""select id, namespace, name, {fetch_meta}, {attrs} 
                    from files
                    where namespace = %s and name=%s""", (namespace, name))
        tup = c.fetchone()
        return DBFile.from_tuple(db, tup)
        
    @staticmethod
    def exists(db, fid = None, namespace = None, name = None):
        #print("DBFile.exists:", fid, namespace, name)
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
        return DBFileSet.from_tuples(db, fetch_generator(c))

    def has_attribute(self, attrname):
        return attrname in self.Metadata
        
    def get_attribute(self, attrname, default=None):
        return self.Metadata.get(attrname, default)

    def to_jsonable(self, with_datasets = False, with_metadata = False, with_provenance=False):
        ns = self.Name if self.Namespace is None else self.Namespace + ':' + self.Name
        data = dict(
            fid = self.FID,
            namespace = self.Namespace,
            name = self.Name
        )
        if self.Checksums is not None:  data["checksums"] = self.Checksums
        if self.Size is not None:       data["size"] = self.Size
        if self.Creator is not None:    data["creator"] = self.Creator
        if self.CreatedTimestamp is not None:    data["created_timestamp"] = epoch(self.CreatedTimestamp)
        if with_metadata:     data["metadata"] = self.metadata()
        if with_provenance:   
            data["parents"] = [f.FID for f in self.parents()]
            data["children"] = [f.FID for f in self.children()]
        if with_datasets:
            data["datasets"] = ["%s:%s" % tup for ds in self.datasets]
        return data

    def to_json(self, with_metadata = False, with_provenance=False):
        return json.dumps(self.to_jsonable(with_metadata=with_metadata, with_provenance=with_provenance))
        
    def children(self, as_files=False, with_metadata = False):
        if self.Children is None:
            self.Children = list(DBFileSet(self.DB, [self]).children(with_metadata))
        return self.Children
        
    def parents(self, as_files=False, with_metadata = False):
        if self.Parents is None:
            self.Parents = list(DBFileSet(self.DB, [self]).parents(with_metadata))
        return self.Parents
        
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
        
    @property
    def datasets(self):
        return _DBManyToMany(self.DB, "files_datasets", "dataset_namespace", "dataset_name", file_id = self.FID)

    def __datasets(self):
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
            negate = False

            term = ""

            if op == "present":
                aname = exp["name"]
                if not '.' in aname:
                    term = "true" if aname in DBFile.ColumnAttributes else "false"
                else:
                    term = f"{table_name}.metadata ? '{aname}'"

            elif op == "not_present":
                aname = exp["name"]
                if not '.' in aname:
                    term = "false" if aname in DBFile.ColumnAttributes else "true"
                else:
                    term = f"{table_name}.metadata ? '{aname}'"
            
            else:
                assert op in ("cmp_op", "in_range", "in_set", "not_in_range", "not_in_set")
                arg = args[0]
                assert arg.T in ("array_any", "array_subscript","array_length","scalar")
                negate = exp["neg"]
                aname = arg["name"]
                if not '.' in aname:
                    assert arg.T == "scalar", f"File attribute {aname} value must be a scalar. Got {arg.T} instead"
                    assert aname in DBFile.ColumnAttributes, f"Unrecognized file attribute {aname}"
                    
                if arg.T == "array_subscript":
                    # a[i] = x
                    aname, inx = arg["name"], arg["index"]
                    inx = json_literal(inx)
                    subscript = f"[{inx}]"
                elif arg.T == "array_any":
                    aname = arg["name"]
                    subscript = "[*]"
                elif arg.T == "scalar":
                    aname = arg["name"]
                    subscript = ""
                elif arg.T == "array_length":
                    aname = arg["name"]
                else:
                    raise ValueError(f"Unrecognozed argument type={arg.T}")

                #parts.append(f"{table_name}.metadata ? '{aname}'")

                    
                # - query time slows down significantly if this is addded
                #if arg.T in ("array_subscript", "array_any", "array_all"):
                #    # require that "aname" is an array, not just a scalar
                #    parts.append(f"{table_name}.metadata @> '{{\"{aname}\":[]}}'")
                
                if op == "in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if not '.' in aname:
                        low = sql_literal(low)
                        high = sql_literal(high)
                        term = f"{table_name}.{aname} between {low} and {high}"
                    elif arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ >= {low} && @ <= {high})'"
                    elif arg.T == "array_length":
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                if op == "not_in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if not '.' in aname:
                        low = sql_literal(low)
                        high = sql_literal(high)
                        term = f"not ({table_name}.{aname} between {low} and {high})"
                    elif arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ < {low} || @ > {high})'"
                    elif arg.T == "array_length":
                        n = "" if negate else "not"
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                elif op == "in_set":
                    if not '.' in aname:
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"{table_name}.{aname} in ({value_list})"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} in ({value_list})"
                    else:           # arg.T in ("array_any", "array_subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        or_parts = [f"@ == {v}" for v in values]
                        predicate = " || ".join(or_parts)
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "not_in_set":
                    if not '.' in aname:
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"not ({table_name}.{aname} in ({value_list}))"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "" if negate else "not"
                        negate = False
                        term = f"not(jsonb_array_length({table_name}.metadata -> '{aname}') {n} in ({value_list}))"
                    else:           # arg.T in ("array_any", "array_subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        and_parts = [f"@ != {v}" for v in values]
                        predicate = " && ".join(and_parts)
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "cmp_op":
                    cmp_op = exp["op"]
                    if cmp_op == '=': cmp_op = "=="
                    sql_cmp_op = "=" if cmp_op == "==" else cmp_op
                    value = args[1]
                    value_type, value = value.T, value["value"]
                    sql_value = sql_literal(value)
                    value = json_literal(value)
                    
                    if not '.' in aname:
                        term = f"{table_name}.{aname} {sql_cmp_op} {sql_value}"
                    elif arg.T == "array_length":
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {sql_cmp_op} {value}"
                    else:
                        if cmp_op in ("~", "~*", "!~", "!~*"):
                            negate_predicate = False
                            if cmp_op.startswith('!'):
                                cmp_op = cmp_op[1:]
                                negate_predicate = not negate_predicate
                            flags = ' flag "i"' if cmp_op.endswith("*") else ''
                            cmp_op = "like_regex"
                            value = f"{value}{flags}"
                        
                            predicate = f"@ like_regex {value} {flags}"
                            if negate_predicate: 
                                predicate = f"!({predicate})"
                            term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"

                        else:
                            # scalar, array_subscript, array_any
                            term = f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} {cmp_op} {value}'"
                    
            if negate:  term = f"not ({term})"
            parts.append(term)

        if contains_items:
            parts.append("%s.metadata @> '{%s}'" % (table_name, ",".join(contains_items )))
            
        return " and ".join([f"({p})" for p in parts])
        
    def sql(self, table_name):
        if self.DNF:
            return " or ".join([self.sql_and(t, table_name) for t in self.DNF])
        else:
            return None
            
class _DatasetParentToChild(_DBManyToMany):
    
    def __init__(self, db, parent):
        _DBManyToMany.__init__(self, db, "datasets_parent_child", "child_namespace", "child_name", 
                    parent_namespace = parent.Namespace, parent_name = parent.Name)
                    
class DBDataset(DBObject):
    
    ColumnsText = "namespace,name,frozen,monotonic,metadata,creator,created_timestamp,description,file_metadata_requirements"
    Columns = ColumnsText.split(",")

    def __init__(self, db, namespace, name, frozen=False, monotonic=False, metadata={}, file_meta_requirements=None, creator=None,
            description = None):
        assert namespace is not None and name is not None
        self.DB = db
        self.Namespace = namespace
        self.Name = name
        self.SQL = None
        self.Frozen = frozen
        self.Monotonic = monotonic
        self.Creator = creator
        self.CreatedTimestamp = None
        self.Metadata = metadata
        self.Description = description
        self.FileMetaRequirements = file_meta_requirements
    
    def __str__(self):
        return "DBDataset(%s:%s)" % (self.Namespace, self.Name)
        
    def create(self, do_commit = True):
        c = self.DB.cursor()
        namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace
        meta = json.dumps(self.Metadata or {})
        file_meta_requirements = json.dumps(self.FileMetaRequirements or {})
        #print("DBDataset.save: saving")
        column_names = self.columns(exclude="created_timestamp")        # use DB default for creation
        c.execute(f"""
            insert into datasets({column_names}) 
                values(%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (namespace, self.Name, self.Frozen, self.Monotonic, meta, self.Creator, 
                    self.Description, file_meta_requirements
            )
        )
        if do_commit:   c.execute("commit")
        return self
        
    def save(self, do_commit = True):
        c = self.DB.cursor()
        namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace
        meta = json.dumps(self.Metadata or {})
        file_meta_requirements = json.dumps(self.FileMetaRequirements or {})
        #print("DBDataset.save: saving")
        column_names = self.columns()
        c.execute(f"""
            update datasets 
                set frozen=%s, monotonic=%s, metadata=%s, description=%s, file_metadata_requirements=%s
                where namespace=%s and name=%s
            """,
            (   self.Frozen, self.Monotonic, meta, self.Description, file_meta_requirements, 
                namespace, self.Name
            )
        )
        if do_commit:   c.execute("commit")
        return self

    def subsets(self, exclude_immediate=False, meta_filter=None):
        immediate = set()
        if exclude_immediate:
            immediate = set((c.Namespace, c.Name) for c in self.children())
        cursor = self.DB.cursor()
        columns = self.columns("d")
        meta_condition = "and " + meta_filter.sql("d") if meta_filter is not None else ""
        cursor.execute(f"""
            with recursive subsets (namespace, name, path, loop) as 
            (
                select pc.child_namespace, pc.child_name, array[pc.child_namespace || ':' || pc.child_name], false
                    from datasets_parent_child pc
                    where pc.parent_namespace = %s and pc.parent_name = %s
                union
                    select pc1.child_namespace, pc1.child_name,
                        s.path || (pc1.child_namespace || ':' || pc1.child_name),
                        pc1.child_namespace || ':' || pc1.child_name = any(s.path)
                    from datasets_parent_child pc1, subsets s
                    where pc1.parent_namespace = s.namespace and pc1.parent_name = s.name and not s.loop
            )
            select distinct {columns} from subsets s, datasets d
                where d.namespace = s.namespace and
                    d.name = s.name
                    {meta_condition}
        """, (self.Namespace, self.Name))
        out = (DBDataset.from_tuple(self.DB, tup) for tup in fetch_generator(cursor))
        if exclude_immediate:
            out = (ds for ds in out if (ds.Namespace, ds.Name) not in immediate)
        return out

    def subset_count(self):
        return len(list(self.subsets()))
            
    def ancestors(self, exclude_immediate=False):
        immediate = set()
        if exclude_immediate:
            immediate = set((c.Namespace, c.Name) for c in self.parents())
        cursor = self.DB.cursor()
        columns = self.columns("d")
        cursor.execute(f"""
            with recursive ancestors (namespace, name, path, loop) as 
            (
                select pc.parent_namespace, pc.parent_name, array[pc.parent_namespace || ':' || pc.parent_name], false
                    from datasets_parent_child pc
                    where pc.child_namespace = %s and pc.child_name = %s
                union
                    select pc1.parent_namespace, pc1.parent_name,
                        a.path || (pc1.parent_namespace || ':' || pc1.parent_name),
                        pc1.parent_namespace || ':' || pc1.parent_name = any(a.path)
                    from datasets_parent_child pc1, ancestors a
                    where pc1.child_namespace = a.namespace and pc1.child_name = a.name and not a.loop
            )
            select distinct {columns} from ancestors a, datasets d
                where d.namespace = a.namespace and
                    d.name = a.name
        """, (self.Namespace, self.Name))
        out = (DBDataset.from_tuple(self.DB, tup) for tup in fetch_generator(cursor))
        if exclude_immediate:
            out = (ds for ds in out if (ds.Namespace, ds.Name) not in immediate)
        return out

    def ancestor_count(self):
        return len(list(self.ancestors()))

    def children(self, meta_filter=None):
        # immediate children as filled DBDataset objects
        c = self.DB.cursor()
        columns = self.columns("c")
        meta_where_clause = "and " + meta_filter.sql("c") if meta_filter is not None else ""
        c.execute(f"""select {columns}
                        from datasets c, datasets_parent_child pc
                        where pc.parent_namespace=%s and pc.parent_name=%s
                            and pc.child_namespace=c.namespace
                            and pc.child_name=c.name
                            {meta_where_clause}
                        """, (self.Namespace, self.Name)
        )
        return (DBDataset.from_tuple(self.DB, tup) for tup in fetch_generator(c))
        
    def has_children(self):
        c = self.DB.cursor()
        c.execute(f"""select exists (
                        select * from datasets_parent_child pc
                        where pc.parent_namespace=%s and pc.parent_name=%s
                        limit 1
                    )""", (self.Namespace, self.Name)
        )
        return c.fetchone()[0]
        
    
    def parents(self):
        # immediate children as filled DBDataset objects
        c = self.DB.cursor()
        columns = self.columns("p")
        c.execute(f"""select {columns}
                        from datasets p, datasets_parent_child pc
                        where pc.child_namespace=%s and pc.child_name=%s
                            and pc.parent_namespace=p.namespace
                            and pc.parent_name=p.name
                        """, (self.Namespace, self.Name)
        )
        return (DBDataset.from_tuple(self.DB, tup) for tup in fetch_generator(c))
    
    def add_child(self, child):
        _DatasetParentToChild(self.DB, self).add(child.Namespace, child.Name)
    
    def remove_child(self, child):
        _DatasetParentToChild(self.DB, self).remove(child.Namespace, child.Name)
    
    def add_file(self, f, do_commit = True):
        assert isinstance(f, DBFile)
        c = self.DB.cursor()
        c.execute("""
            insert into files_datasets(file_id, dataset_namespace, dataset_name) values(%s, %s, %s)
                on conflict do nothing""",
            (f.FID, self.Namespace, self.Name))
        if do_commit:   c.execute("commit")
        return self
        
    def add_files(self, files, do_commit=True, validate_meta=True):
        c = self.DB.cursor()
        c.execute("begin")
        
        existing = set(f.FID for f in self.list_files(with_metadata=False))

        csv = []
        null = r"\N"

        to_add = set(f.FID for f in files) - existing
        
        if validate_meta:
            meta_errors = []
            for f in files:
                if f.FID in to_add:
                    errors = self.validate_file_metadata(f.Metadata)
                    if errors:
                        meta_errors += errors
            if meta_errors:
                raise MetaValidationError("File metadata validation errors", meta_errors)
        
        for fid in to_add:
            csv.append("%s\t%s\t%s" % (
                fid, self.Namespace, self.Name
            ))
        csv = io.StringIO("\n".join(csv))
        

        try:
            #open("/tmp/files.csv", "w").write(files_data)
            if to_add:
                c.copy_from(csv, "files_datasets", 
                        columns = ["file_id", "dataset_namespace", "dataset_name"])
            if do_commit:   c.execute("commit")
        except Exception as e:
            print(traceback.format_exc())
            c.execute("rollback")
            raise

    def list_files(self, with_metadata=False, limit=None):
        meta = "null as metadata" if not with_metadata else "f.metadata"
        limit = f"limit {limit}" if limit else ""
        sql = f"""select f.id, f.namespace, f.name, {meta}, f.size, f.checksums, f.creator, f.created_timestamp 
                    from files f
                        inner join files_datasets fd on fd.file_id = f.id
                    where fd.dataset_namespace = %s and fd.dataset_name=%s
                    {limit}
        """
        c = self.DB.cursor()
        c.execute(sql, (self.Namespace, self.Name))
        for fid, namespace, name, meta, size, checksums, creator, created_timestamp in fetch_generator(c):
            meta = meta or {}
            checksums = checksums or {}
            f = DBFile(self.DB, fid=fid, namespace=namespace, name=name, metadata=meta, size=size, checksums = checksums)
            f.Creator = creator
            f.CreatedTimestamp = created_timestamp
            yield f
        
    @staticmethod
    def get(db, namespace, name):
        c = db.cursor()
        namespace = namespace.Name if isinstance(namespace, DBNamespace) else namespace
        #print(namespace, name)
        columns = DBDataset.columns()
        c.execute(f"""select {columns}
                        from datasets
                        where namespace=%s and name=%s""",
                (namespace, name))
        tup = c.fetchone()
        if tup is None: return None
        return DBDataset.from_tuple(db, tup)

    @staticmethod
    def get_many(db, namespaces_names):
        # namespaces_names is list of tuples [(namespace, name), ...]
        c = db.cursor()
        specs = list(set(f"{namespace}:{name}" for namespace, name in namespaces_names))
        columns = DBDataset.columns()
        c.execute(f"""select {columns}
                        from datasets
                        where (namespace || ':' || name) = any(%s) """,
                (specs,)
        )
        return (DBDataset.from_tuple(db, tup) for tup in fetch_generator(c))

    @staticmethod
    def from_tuple(db, tup):
        namespace, name, frozen, monotonic, metadata, creator, created_timestamp, description, file_metadata_requirements = tup
        dataset = DBDataset(db, namespace, name, 
            frozen=frozen, monotonic=monotonic, metadata=metadata, file_meta_requirements=file_metadata_requirements)
        dataset.Creator = creator
        dataset.CreatedTimestamp = created_timestamp
        dataset.Description = description
        return dataset
        
    @staticmethod
    def exists(db, namespace, name):
        return DBDataset.get(db, namespace, name) is not None

    @staticmethod
    def list(db, namespace=None, parent_namespace=None, parent_name=None, creator=None):
        namespace = namespace.Name if isinstance(namespace, DBNamespace) else namespace
        parent_namespace = parent_namespace.Name if isinstance(parent_namespace, DBNamespace) else parent_namespace
        creator = creator.Username if isinstance(creator, DBUser) else creator
        wheres = []
        if namespace is not None:
            wheres.append("namespace = '%s'" % (namespace,))
        if parent_namespace is not None:
            wheres.append("parent_namespace = '%s'" % (parent_namespace,))
        if parent_name is not None:
            wheres.append("parent_name = '%s'" % (parent_name,))
        if creator is not None:
            wheres.append("creator = '%s'" % (creator,))
        wheres = "" if not wheres else "where " + " and ".join(wheres)
        c=db.cursor()
        columns = DBDataset.columns()
        c.execute(f"""select {columns}
                from datasets %s""" % (wheres,))
        for tup in fetch_generator(c):
            yield DBDataset.from_tuple(db, tup)

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
            frozen = self.Frozen,
            monotonic = self.Monotonic,
            parents = [
                p.Namespace + ":" + p.Name for p in self.parents()
            ],
            children = [
                c.Namespace + ":" + c.Name for c in self.children()
            ],
            metadata = self.Metadata or {},
            creator = self.Creator,
            created_timestamp = epoch(self.CreatedTimestamp),
            file_meta_requirements = self.FileMetaRequirements,
            description = self.Description
        )
    
    def to_json(self):
        return json.dumps(self.to_jsonable())
        
    def delete(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            delete from datasets where namespace=%s and name=%s
        """, (self.Namespace, self.Name))
        if do_commit:   c.execute("commit")
        
    @staticmethod
    def list_datasets(db, patterns, with_children, recursively, limit=None):
        datasets = set()
        c = db.cursor()
        #print("DBDataset.list_datasets: patterns:", patterns)
        for pattern in patterns:
            match = pattern["wildcard"]
            namespace = pattern["namespace"]
            name = pattern["name"]
            #print("list_datasets: match, namespace, name:", match, namespace, name)
            if match:
                sql = """select namespace, name, metadata from datasets
                            where namespace = '%s' and name like '%s'""" % (namespace, name)
                #print("list_datasets: sql:", sql)
                c.execute(sql)
            else:
                c.execute("""select namespace, name, metadata from datasets
                            where namespace = %s and name = %s""", (namespace, name))
            for namespace, name, meta in c.fetchall():
                #print("list_datasets: add", namespace, name)
                datasets.add((namespace, name))
                
        #print("list_datasets: with_children:", with_children)
        specs = set(f"{namespace}:{name}" for namespace, name in datasets)
        if with_children:
            specs = subsets_rec(c, specs, set(), level=None if recursively else 0)
        return DBDataset.get_many(db, [spec.split(":",1) for spec in specs])    

    @staticmethod    
    def apply_dataset_selector(db, dataset_selector, limit):
        patterns = dataset_selector.Patterns
        with_children = dataset_selector.WithChildren
        recursively = dataset_selector.Recursively
        datasets = DBDataset.list_datasets(db, patterns, with_children, recursively)
        return limited(dataset_selector.filter_by_having(datasets), limit)
        
    @staticmethod
    def dataset_selector_sql(namespace, name, pattern, with_children, recursively, meta_filter):

            pc = alias("pc")
            pc1 = alias("pc1")
            d = alias("d")
            s = alias("s")
            ds = alias("ds")
            t = alias("t")

            columns = DBDataset.columns(as_text=False)

            name_cmp = f"{ds}.name = '{name}'" if not pattern else f"{ds}.name like '{name}'"
        
            if not with_children:
                #print([f"{ds}.{c}" for c in columns])
                columns = ",".join([f"{ds}.{c}" for c in columns])
                #print("columns:", columns)
                sql = f"select {columns} from datasets {ds} where {ds}.namespace = '{namespace}' and {name_cmp}"
                if meta_filter is not None:
                    sql += " and " + meta_filter.sql(ds)
            else:
                columns = ",".join(f"{d}.{c}" for c in columns)
                top_sql = f"""select {ds}.namespace, {ds}.name, array[{ds}.namespace || ':' || {ds}.name], false
                                from datasets {ds}
                                where {ds}.namespace = '{namespace}' and {name_cmp}
                                    """

                if not recursively:
                    selected_sql = f"""
                                with selected_datasets (namespace, name, path, loop) as 
                                (
                                    with {t} (namespace, name, path, loop) as 
                                    ( 
                                        {top_sql}
                                    ) -- 4
                                    select namespace, name, path, loop from {t}
                                    union
                                        select {pc}.child_namespace, {pc}.child_name,
                                            {t}.path || ({pc}.child_namespace || ':' || {pc}.child_name), true
                                        from datasets_parent_child {pc}, {t}
                                        where {pc}.parent_namespace = {t}.namespace and {pc}.parent_name = {t}.name
                                ) -- 3
                    """
                else:
                    selected_sql = f"""
                                with recursive selected_datasets (namespace, name, path, loop) as 
                                (
                                    {top_sql}
                                    union
                                        select {pc1}.child_namespace, {pc1}.child_name,
                                            {s}.path || ({pc1}.child_namespace || ':' || {pc1}.child_name),
                                            {pc1}.child_namespace || ':' || {pc1}.child_name = any({s}.path)
                                        from datasets_parent_child {pc1}, selected_datasets {s}
                                        where {pc1}.parent_namespace = {s}.namespace and {pc1}.parent_name = {s}.name and not {s}.loop
                                ) -- 1
                    """
                meta_condition = "and " + meta_filter.sql(d) if meta_filter is not None else ""
                sql = f"""
                    {selected_sql}
                    select distinct {columns} 
                        from selected_datasets {s}, datasets {d}
                        where {d}.namespace = {s}.namespace and
                            {d}.name = {s}.name
                            {meta_condition}
                """
            debug("sql_for_selector(", namespace, name, pattern, with_children, recursively, meta_filter, ")\n", sql)
            return sql

    @staticmethod
    def datasets_for_selector(db, selector):
        if selector.is_explicit():
            ds = DBDataset.get(db, selector.Namespace, selector.Name)
            if ds is None:
                out = []
            else:
                out = [ds]
        else:
            sql = DBDataset.dataset_selector_sql(selector.Namespace, selector.Name, selector.Pattern, selector.WithChildren,
                    selector.Recursively, selector.Having)
            c = db.cursor()
            c.execute(sql)
            out = [DBDataset.from_tuple(db, tup) for tup in fetch_generator(c)]
        return out
        
    @staticmethod
    def datasets_for_selectors(db, selectors):
        out = {}        # (namespace, name) -> Dataset, to filter out duplicates
        for sel in selectors:
            for ds in DBDataset.datasets_for_selector(db, sel):
                out[(ds.Namespace, ds.Name)] = ds
        return list(out.values())
        
    def validate_file_metadata(self, meta):
        """
        File metadata requirements:
        [
            "name":
            {
                "required":true/false,  # optional, default 'false'
                "values":[...],         # optional
                "min":  value,          # optional
                "max":  value,          # optional
                "pattern": "re pattern" # optional
            },
            ...
        ]
        """
        errors = []
        if self.FileMetaRequirements:
            for k, v in meta.items():
                reqs = self.FileMetaRequirements.get(k)
                if reqs:
                    if "values" in reqs:
                        values = reqs["values"]
                        if isinstance(v, list):
                            if any(not x in values for x in v):
                                errors.append(dict(name=k, value=v, reason="Invalid value"))
                        else:
                            if not v in values:
                                errors.append(dict(name=k, value=v, reason="Invalid value"))
                    if "min" in reqs:
                        vmin = reqs["min"]
                        if isinstance(v, list):
                            if any(x < vmin for x in v):
                                errors.append(dict(name=k, value=v, reason="Value out of range"))
                        else:
                            if v < vmin:
                                errors.append(dict(name=k, value=v, reason="Value out of range"))
                    if "max" in reqs:
                        vmax = reqs["max"]
                        if isinstance(v, list):
                            if any(x > vmax for x in v):
                                errors.append(dict(name=k, value=v, reason="Value out of range"))
                        else:
                            if v > vmax:
                                errors.append(dict(name=k, value=v, reason="Value out of range"))
                    if "pattern" in reqs:
                        r = re.compile(reqs["pattern"])
                        if isinstance(v, list) and any(isinstance(x, str) and not r.match(x) for x in v):
                            errors.append(dict(name=k, value=v, reason="Value does not match pattern"))
                        elif isinstance(v, str) and not r.match(v):
                            errors.append(dict(name=k, value=v, reason="Value does not match pattern"))
            for k, d in self.FileMetaRequirements.items():
                debug("validate_file_metadata: FileMetaRequirements:", self.FileMetaRequirements)
                if d.get("required", False) == True and not k in meta:
                    errors.append(dict(name=k, reason="Required parameter is missing"))
        return errors
        
    @staticmethod
    def datasets_for_files(db, files):
        file_ids = [f.FID for f in files]
        dataset_map = {}       # { fid -> [DBDataset, ...]}
        datasets = {}       # {(ns,n) -> DBDataset}
        c = db.cursor()
        ds_columns = self.columns("ds")
        c.execute(f"""
            select distinct f.id, {ds_columns}
                        from datasets ds, files f, files_datasets fd
                        where f.id = any(%s) and
                            fd.dataset_namespace = ds.namespace and fd.dataset_name = ds.name and fd.file_id = f.id
                        order by f.id, ds.namespace, ds.name
                        """, (file_ids,)
        )
        
        for tup in fetch_generator(c):
            fid = tup[0]
            namespace, name  = tup[1:3]
            ds = datasets.get((namespace, name))
            if ds is None:
                ds = datasets[(namespace, name)] = DBDataset.from_tuple(db, tup[1:])
            dslist = dataset_map.setdefault(fid, [])
            dslist.append(ds)
        
        return dataset_map
        
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
        debug("DBNamedQuery:get():", namespace, name)
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
        self.AuthInfo = {}        # type -> [secret,...]        # DB representation
        self.RoleNames = None

class DBUser(BaseDBUser):

    @property
    def roles(self):
        return _DBManyToMany(self.DB, "users_roles", "role_name", username = self.Username)
        
    def namespaces(self):
        return DBNamespace.list(self.DB, owned_by_user=self)        
        
    def add_role(self, role):
        self.roles.add(role.Name if isinstance(role, DBRole) else role)

    def remove_role(self, role):
        self.roles.remove(role.Name if isinstance(role, DBRole) else role)

class DBNamespace(object):

    def __init__(self, db, name, owner_user=None, owner_role=None, description=None):
        self.Name = name
        assert None in (owner_user, owner_role)
        self.OwnerUser = owner_user
        self.OwnerRole = owner_role
        self.Description = description
        self.DB = db
        self.Creator = None
        self.CreatedTimestamp = None
        
    def to_jsonable(self):
        return dict(
            name=self.Name,
            owner_user=self.OwnerUser,
            owner_role=self.OwnerRole,
            creator = self.Creator,
            description = self.Description,
            created_timestamp = epoch(self.CreatedTimestamp)
        )
        
    def save(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into namespaces(name, owner_user, owner_role, description, creator) values(%s, %s, %s, %s, %s)
                on conflict(name) 
                    do update set owner_user=%s, owner_role=%s, description=%s, creator=%s;
            commit
            """,
            (self.Name, self.OwnerUser, self.OwnerRole, self.Description, self.Creator, self.OwnerUser, self.OwnerRole, self.Description, self.Creator))
        if do_commit:
            c.execute("commit")
        return self

    def create(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into namespaces(name, owner_user, owner_role, description, creator) values(%s, %s, %s, %s, %s)
            """,
            (self.Name, self.OwnerUser, self.OwnerRole, self.Description, self.Creator))
        if do_commit:
            c.execute("commit")
        return self
        
    @staticmethod
    def get(db, name):
        #print("DBNamespace.get: name:", name)
        c = db.cursor()
        c.execute("""select owner_user, owner_role, description, creator, created_timestamp 
                from namespaces where name=%s""", (name,))
        tup = c.fetchone()
        if not tup: return None
        owner_user, owner_role, description, creator, created_timestamp = tup
        ns = DBNamespace(db, name, owner_user, owner_role, description)
        ns.Creator = creator
        ns.CreatedTimestamp = created_timestamp
        return ns
        
    @staticmethod
    def get_many(db, names):
        #print("DBNamespace.get: name:", name)
        c = db.cursor()
        c.execute("""select name, owner_user, owner_role, description, creator, created_timestamp 
                from namespaces where name=any(%s)""", (list(names),))
        for name, owner_user, owner_role, description, creator, created_timestamp in c.fetchall():
            ns = DBNamespace(db, name, owner_user, owner_role, description)
            ns.Creator = creator
            ns.CreatedTimestamp = created_timestamp
            yield ns
            
    @staticmethod
    def exists(db, name):
        return DBNamespace.get(db, name) != None
        
    @staticmethod
    def list(db, owned_by_user=None, owned_by_role=None, directly=False):
        c = db.cursor()
        if isinstance(owned_by_user, DBUser):   owned_by_user = owned_by_user.Username
        if isinstance(owned_by_role, DBRole):   owned_by_role = owned_by_role.Name
        if owned_by_user is not None:
            sql = """
                select name, owner_user, owner_role, description, creator, created_timestamp 
                        from namespaces
                        where owner_user=%s
            """
            args = (owned_by_user,)
            if not directly:
                sql += """
                    union
                    select name, owner_user, owner_role, description, creator, created_timestamp 
                            from namespaces ns, users_roles ur
                            where ur.username = %s and ur.role_name = ns.owner_role
                """
                args = args + (owned_by_user,)
        elif owned_by_role is not None:
            sql = """select name, owner_user, owner_role, description, creator, created_timestamp 
                        from namespaces
                        where owner_role=%s
            """
            args = (owned_by_role,)
        else:
            sql = """select name, owner_user, owner_role, description, creator, created_timestamp 
                        from namespaces
            """
            args = ()
        #print("DBNamespace.list: sql, args:", sql, args)
        c.execute(sql, args)
        for name, owner_user, owner_role, description, creator, created_timestamp in c.fetchall():
            ns = DBNamespace(db, name, owner_user, owner_role, description)
            ns.Creator = creator
            ns.CreatedTimestamp = created_timestamp
            yield ns

    def owners(self, directly=False):
        if self.OwnerUser is not None:
            return [self.OwnerUser]
        elif not directly and self.OwnerRole is not None:
            r = self.OwnerRole
            if isinstance(r, str):
                r = DBRole(self.DB, r)
            return r.members
        else:
            return []

    def owned_by_user(self, user, directly=False):
        if isinstance(user, DBUser):   user = user.Username
        return user in self.owners(directly)
        
    def owned_by_role(self, role):
        if isinstance(role, DBRole):   role = role.Name
        return self.OwnerRole == role

    def file_count(self):
        c = self.DB.cursor()
        c.execute("""select count(*) from files where namespace=%s""", (self.Name,))
        tup = c.fetchone()
        if not tup: return 0
        else:       return tup[0]
        
    def dataset_count(self):
        c = self.DB.cursor()
        c.execute("""select count(*) from datasets where namespace=%s""", (self.Name,))
        tup = c.fetchone()
        if not tup: return 0
        else:       return tup[0]
        
    def query_count(self):
        c = self.DB.cursor()
        c.execute("""select count(*) from queries where namespace=%s""", (self.Name,))
        tup = c.fetchone()
        if not tup: return 0
        else:       return tup[0]

class DBRole(object):

    def __init__(self, db, name, description=None, users=[]):
        self.Name = name
        self.Description = description
        self.DB = db
            
    def __str__(self):
        return "[DBRole %s %s]" % (self.Name, self.Description)
        
    __repr__ = __str__

    @property
    def members(self):
        return _DBManyToMany(self.DB, "users_roles", "username", role_name=self.Name)
        
    def save(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into roles(name, description) values(%s, %s)
                on conflict(name) 
                    do update set description=%s
            """,
            (self.Name, self.Description, self.Description))
        if do_commit:   c.execute("commit")
        return self
        
    @staticmethod
    def get(db, name):
        c = db.cursor()
        c.execute("""select r.description
                        from roles r
                        where r.name=%s
        """, (name,))
        tup = c.fetchone()
        if not tup: return None
        (desc,) = tup
        return DBRole(db, name, desc)
        
    @staticmethod 
    def list(db, user=None):
        c = db.cursor()
        if isinstance(user, DBUser):    user = user.Username
        if user:
            c.execute("""select r.name, r.description
                        from roles r
                            inner join users_roles ur on ur.role_name=r.name
                    where ur.username = %s
                    order by r.name
            """, (user,))
        else:
            c.execute("""select r.name, r.description
                            from roles r
                            order by r.name""")
        
        out = [DBRole(db, name, description) for  name, description in fetch_generator(c)]
        #print("DBRole.list:", out)
        return out
        
    def add_member(self, user):
        self.members.add(user)
        return self
        
    def remove_member(self, user):
        self.members.remove(user)
        return self
        
    def set_members(self, users):
        self.members.set(users)
        return self
        
    def __contains__(self, user):
        if isinstance(user, DBUser):
            user = user.Username
        return user in self.members
        
    def __iter__(self):
        return self.members.__iter__()
            
