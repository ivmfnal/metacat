import uuid, json, hashlib, re, time, io, traceback, base64
from metacat.util import to_bytes, to_str, epoch, chunked, limited, strided, skipped, first_not_empty, validate_metadata, insert_sql
from metacat.auth import BaseDBUser
from metacat.common import MetaExpressionDNF
from psycopg2 import IntegrityError
from textwrap import dedent
from datetime import datetime, timezone

Debug = False

def debug(*parts):
    if Debug:
        print("[debug]", *parts)

from .common import (
    DBObject, _DBManyToMany,
    AlreadyExistsError, DatasetCircularDependencyDetected, NotFoundError, MetaValidationError,
    parse_name, fetch_generator, alias, 
    insert_bulk
)

class DBFileSet(object):
    
    def __init__(self, db, files=[], count=None):
        self.DB = db
        self.Files = files
        self.SQL = None
        if hasattr(files, "__len__"):
            count = len(files)
        self.Count = count
        
    def __len__(self):
        return self.Count

    def limit(self, n):
        return DBFileSet(self.DB, limited(self.Files, n), 
            count = None if self.Count is None else min(n, self.Count)
        )
        
    def skip(self, n):
        if n == 0:  return self
        return DBFileSet(self.DB, skipped(self.Files, n),
            count = None if self.Count is None else max(0, self.Count - n)
        )
        
    def stride(self, n, i=0):
        count = None
        if self.Count is not None:
            count = (self.Count + i + n-1)//n
        return DBFileSet(self.DB, strided(self.Files, n, i), count = count)
        
    def chunked(self, chunk_size=1000):
        return chunked(self.Files, chunk_size)

    def ordered(self):
        files = sorted(self, lambda f: f.ID)
        yield from files

    @staticmethod
    def from_tuples(db, g, count=None):
        # must be in sync with DBFile.all_columns()
        if isinstance(g, (list, tuple, set)):
            count = len(g)
        return DBFileSet(db, 
            (
                DBFile.from_tuple(db, t) for t in g
            ),
            count = count
        )
        
    @staticmethod
    def from_id_list(db, lst):
        c = db.cursor()
        columns = DBFile.all_columns()
        c.execute(f"""
            select {columns}
                from   files
                where id = any(%s)""", (list(lst),))
        return DBFileSet.from_tuples(db, fetch_generator(c), count=c.rowcount)
    
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
        return DBFileSet.from_tuples(db, selected, count=c.rowcount)
        
    @staticmethod
    def from_namespace_name_specs(db, specs, default_namespace=None):
        # specs: list of dicts {"name":..., "namespace":...} - namespace is optional
        specs = [(s.get("namespace", default_namespace), s["name"]) for s in specs]
        assert all(s["namespace"] for s in specs), "Incomplete file specification: " + s["name"]
        just_names = set(name for ns, name in specs)
        dids = set("%s:%s" % t for t in specs)
        c = db.cursor()
        columns = DBFile.all_columns()
        c.execute(f"""
            select {columns}, null as parents, null as children from files
                where name = any(%s)""", (just_names,))
        selected = ((fid, namespace, name, metadata) 
                    for (fid, namespace, name, metadata) in fetch_generator(c)
                    if "%s:%s" % (namespace, name) in dids)
        return DBFileSet.from_tuples(db, selected, count=c.rowcount)
        
    def __iter__(self):
        if isinstance(self.Files, (list, set, tuple)):
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
        return DBFileSet.from_tuples(self.DB, fetch_generator(c), count=c.rowcount)

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
        return DBFileSet(db, (f for f in file_list if f.FID in file_ids), count=len(file_ids))

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
    
    def __add__(self, other):
        assert self.DB is other.DB
        return DBFileSet.union([self, other])
        
    @staticmethod
    def from_basic_query(db, basic_file_query, with_metadata, limit):
        if limit is None:
            limit = basic_file_query.Limit
        elif basic_file_query.Limit is not None:
            limit = min(limit, basic_file_query.Limit)
            
        bdq = basic_file_query.DatasetSelector
        datasets = None
        if bdq is not None:
            datasets = list(bdq.datasets(db))
            if not datasets:
                return DBFileSet(db)      # empty File Set

        if bdq is None:
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
    def ____sql_for_basic_query(db, basic_file_query):
        debug("sql_for_basic_query: bfq:", basic_file_query, " with provenance:", basic_file_query.WithProvenance)

        f = alias("f")

        limit = basic_file_query.Limit
        limit = "" if limit is None else f"limit {limit}"
        offset = "" if not basic_file_query.Skip else f"offset {basic_file_query.Skip}"
        #order = f"order by {f}.id" if basic_file_query.Skip or basic_file_query.Limit or basic_file_query.Ordered else ""
        order = f"order by {f}.id" if basic_file_query.Ordered else ""
        
        debug("sql_for_basic_query: offset:", offset)

        meta = f"{f}.metadata" if basic_file_query.WithMeta else "null as metadata"
        parents = f"{f}.parents" if basic_file_query.WithProvenance else "null as parents"
        children = f"{f}.children" if basic_file_query.WithProvenance else "null as children"
        table = "files_with_provenance" if basic_file_query.WithProvenance else "files"

        debug("sql_for_basic_query: table:", table)

        file_meta_exp = MetaExpressionDNF(basic_file_query.Wheres).sql(f) or "true"

        datasets = None if basic_file_query.DatasetSelectors is None else list(DBDataset.datasets_for_bdqs(db, basic_file_query.DatasetSelectors))
        debug("sql_for_basic_query: datasets:", datasets)
        
        
        
        attrs = DBFile.attr_columns(f)
        if datasets is None:
            # no dataset selection
            sql = dedent(f"""\
                -- sql_for_basic_query {f}
                    select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
                        from {table} {f}
                        where {file_meta_exp}
                        {order} {limit} {offset}
                -- end of sql_for_basic_query {f}
            """)
        else:
            #datasets_sql = DBDataset.sql_for_selector(dataset_selector)
            
            datasets = list(datasets)
            if not datasets:
                return None
            ds_names = set()
            ds_namespaces = set()
            ds_specs = set()
            for ns, n in datasets:
                ds_names.add(ns)
                ds_namespaces.add(n)
                ds_specs.add(ns + ":" + n)
                
            fd = alias("fd")
            ds = alias("ds")
            
            ds_namespaces = list(ds_namespaces)
            ds_names = list(ds_names)
            ds_specs = list(ds_specs)

            pairs_where = f"({fd}.dataset_namespace, {fd}.dataset_name) in %s" % (tuple(datasets),)
        
            sql = insert_sql(f"""\
                -- sql_for_basic_query {f}
                    select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
                        from {table} {f}
                            inner join files_datasets {fd} on {fd}.file_id = {f}.id
                        where
                            {pairs_where}
                            and (
                                $file_meta_exp
                            )
                        {order} {limit} {offset}
                -- end of sql_for_basic_query {f}
            """, file_meta_exp = file_meta_exp)
        debug("sql_for_basic_query: sql:-------\n", sql, "\n---------")
        return sql
        
    @staticmethod
    def sql_for_basic_query(db, basic_file_query, include_retired=False):
        debug("sql_for_basic_query: bfq:", basic_file_query, " with provenance:", basic_file_query.WithProvenance)

        f = alias("f")

        limit = basic_file_query.Limit
        limit = "" if limit is None else f"limit {limit}"
        offset = "" if not basic_file_query.Skip else f"offset {basic_file_query.Skip}"
        #order = f"order by {f}.id" if basic_file_query.Skip or basic_file_query.Limit or basic_file_query.Ordered else ""
        order = f"order by {f}.id" if basic_file_query.Ordered else ""
        
        debug("sql_for_basic_query: offset:", offset)

        meta = f"{f}.metadata" if basic_file_query.WithMeta else "null as metadata"
        parents = f"{f}.parents" if basic_file_query.WithProvenance else "null as parents"
        children = f"{f}.children" if basic_file_query.WithProvenance else "null as children"
        table = "files_with_provenance" if basic_file_query.WithProvenance else "files"

        debug("sql_for_basic_query: table:", table)

        file_meta_exp = MetaExpressionDNF(basic_file_query.Wheres).sql(f) or "true"
        retired_condition = "true" if include_retired else f"not {f}.retired"

        attrs = DBFile.attr_columns(f)
        debug("attrs:", attrs)
        if basic_file_query.DatasetSelectors is None:
            # no dataset selection
            sql = insert_sql(f"""\
                -- sql_for_basic_query {f}
                    select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
                        from {table} {f}
                        where {retired_condition}   -- include retired ? 
                            and (   -- metadata
                                $file_meta_exp
                            )
                        {order} {limit} {offset}
                -- end of sql_for_basic_query {f}
            """, file_meta_exp = file_meta_exp)
        else:
            datasets_sql = DBDataset.sql_for_bdqs(basic_file_query.DatasetSelectors, names_only=True)
            #datasets_sql = DBDataset.sql_for_selector(dataset_selector)
            
            fd = alias("fd")
            ds = alias("ds")
            
            sql = insert_sql(f"""\
                -- sql_for_basic_query {f}
                    select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
                        from {table} {f},
                            files_datasets {fd},
                            (
                                $datasets_sql
                            ) as {ds}(namespace, name)
                        where {fd}.file_id = {f}.id
                            and {ds}.namespace = {fd}.dataset_namespace
                            and {ds}.name = {fd}.dataset_name
                            and {retired_condition}   -- include retired ? 
                            and (  -- metadata
                                $file_meta_exp
                            )
                        {order} {limit} {offset}
                -- end of sql_for_basic_query {f}
            """, file_meta_exp = file_meta_exp, datasets_sql=datasets_sql)
        debug("sql_for_basic_query: sql:-------\n", sql, "\n---------")
        return sql
        
    @staticmethod
    def sql_for_file_list(spec_type, spec_list, with_meta, with_provenance, limit, skip):
        
        f = alias("f")
        meta = f"{f}.metadata" if with_meta else "null as metadata"
        ids = []
        specs = []
        
        attrs = DBFile.attr_columns(f)

        if with_provenance:
            table = "files_with_provenance"
            prov_columns = f"{f}.parents, {f}.children"
        else:
            table = "files"
            prov_columns = f"null as parents, null as children"

        sql = dedent(f"""\
                select {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {prov_columns} from {table} {f}
        """)

        if spec_type == "fid":
            id_list = ",".join(["'%s'" % (i,) for i in spec_list])
            sql += f" where id in ({id_list}) "
        else:
            namespace_names = []
            for spec in spec_list:
                if not spec.get("namespace"):
                    raise ValueError("No namespace is given for " + spec.get("name"))
                namespace_names.append("('%(namespace)s', '%(name)s')" % spec)
            namespace_names = ','.join(namespace_names)
            sql += f" where (namespace, name) in ({namespace_names}) """

        sql += f" order by {f}.id "

        if limit is not None:
            sql += f" limit {limit}"
        if skip > 0:
            sql += f" offset {skip}"

        return sql

    @staticmethod
    def from_sql(db, sql):
        c = db.cursor()
        debug("DBFileSet.from_sql: executing sql:", sql)
        c.execute(sql)
        debug("DBFileSet.from_sql: return from execute()")
        fs = DBFileSet.from_tuples(db, fetch_generator(c), count=c.rowcount)
        fs.SQL = sql
        return fs
        
class DBFile(object):
    
    ColumnAttributes=[      # column names which can be used in queries
        "creator", "created_timestamp", "name", "namespace", "size"
    ]  
    def __init__(self, db, namespace = None, name = None, metadata = None, fid = None, size=None, checksums=None,
                    parents = None, children = None, creator = None, created_timestamp=None,
                    updated_timestamp = None, updated_by = None,
                    retired = False, retired_timestamp = None, retired_by = None
                    ):

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
        self.UpdatedBy = updated_by
        self.UpdatedTimestamp = updated_timestamp
        self.Retired = retired
        self.RetiredTimestamp = retired_timestamp
        self.RetiredBy = retired_by
    
    @staticmethod
    def generate_id():
        #return uuid.uuid4().hex
        done = False
        x = None
        while not done:
            x = base64.b64encode(uuid.uuid4().bytes, b"__")[:16]            # 62**16 = 5E28 = 100 lifetimes of the Universe in nanoseconds
            done = b'_' not in x
        return x.decode("utf-8")

    def __str__(self):
        return "[DBFile %s %s:%s]" % (self.FID, self.Namespace, self.Name)

    __repr__ = __str__

    CoreColumnNames = [
        "id", "namespace", "name", "metadata"
    ]

    AttrColumnNames = [
        "creator", "created_timestamp", "size", "checksums", 
        "updated_by", "updated_timestamp", 
        "retired", "retired_timestamp", "retired_by"
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
                    returning created_timestamp
                """,
                (self.FID, self.Namespace, self.Name, meta, self.Size, checksums, creator))
            self.CreatedTimestamp = c.fetchone()[0]
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

    def did(self):
        return f"{self.Namespace}:{self.Name}"

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

        
    def update(self, user, do_commit = True):
        from psycopg2 import IntegrityError
        c = self.DB.cursor()
        meta = json.dumps(self.Metadata or {})
        checksums = json.dumps(self.Checksums or {})
        try:
            c.execute("""
                update files set namespace=%s, name=%s, metadata=%s, size=%s, checksums=%s 
                    updated_by=%s, updated_timestamp = now()
                    where id = %s
                """, (self.Namespace, self.Name, meta, self.Size, checksums, user,
                        self.FID)
            )
            if do_commit:   c.execute("commit")
        except:
            c.execute("rollback")
            raise    
        return self
        
    def set_retire(self, retire, user, do_commit=True):
        #print("set_retire:", retire, user)
        from psycopg2 import IntegrityError
        if retire != self.Retired:
            c = self.DB.cursor()
            try:
                if retire:
                    self.RetiredTimestamp = datetime.now(timezone.utc)
                    self.RetiredBy = user
                    c.execute("""
                        update files set retired=true, retired_by=%s, retired_timestamp = %s
                            where id = %s
                        """, (self.RetiredBy, self.RetiredTimestamp, self.FID)
                    )
                else:
                    self.UpdatedTimestamp = datetime.now(timezone.utc)
                    self.UpdatedBy = user
                    c.execute("""
                        update files set retired=false, updated_by=%s, updated_timestamp = %s
                            where id = %s
                        """, (self.UpdatedBy, self.UpdatedTimestamp, self.FID)
                    )
                if do_commit:   c.execute("commit")
            except:
                c.execute("rollback")
                raise
            self.Retired = retire

    @staticmethod
    def from_tuple(db, tup):
        debug("----DBFile.from_tup: tup:", tup)
        if tup is None: return None
        try:
            try:    
                (fid, namespace, name, meta, creator, created_timestamp, size, checksums, 
                        updated_by, updated_timestamp, 
                        retired, retired_timestamp, retired_by,
                        parents, children) = tup
                f = DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta, size=size, checksums = checksums,
                    parents = parents, children=children, creator=creator,
                            created_timestamp=created_timestamp,
                            updated_by=updated_by, updated_timestamp=updated_timestamp, 
                            retired=retired, retired_timestamp=retired_timestamp, retired_by=retired_by
                            )
                debug("file created:", f.to_json())
            except: 
                # try without provenance
                try:    
                    (fid, namespace, name, meta, creator, created_timestamp, size, checksums, 
                         updated_by, updated_timestamp, retired, retired_timestamp, retired_by)= tup
                    f = DBFile(db, fid=fid, namespace=namespace, name=name, metadata=meta, size=size, checksums = checksums, creator=creator,
                            created_timestamp=created_timestamp,
                            updated_by=updated_by, updated_timestamp=updated_timestamp, 
                            retired=retired, retired_timestamp=retired_timestamp, retired_by=retired_by
                            )
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
        # NOT really THREAD SAFE !!
        #

        # files: list of dicts:
        #  { "fid": ... } or {"namespace":..., "name":...}
        
        #print("DBFile.get_files: files:", files)
        suffix = int(time.time()*1000)
        temp_table = f"temp_files_{suffix}"
        c = db.cursor()
        strio = io.StringIO()
        for f in files:
            ns = n = None
            fid = f.get("fid")
            if fid is None:
                ns = f.get("namespace")
                n = f.get("name")
                if ns is None or n is None:
                    raise ValueError("Invalid file specificication: " + str(f))
            strio.write("%s\t%s\t%s\n" % (fid or r'\N', ns or r'\N', n or r'\N'))
        c.execute(f"""create temp table if not exists
            {temp_table} (
                id text,
                namespace text,
                name text);
            truncate table {temp_table};
                """)
        cvs = strio.getvalue()
        c.copy_from(io.StringIO(cvs), temp_table)
        #print("DBFile.get_files: strio:", strio.getvalue())
        
        columns = DBFile.all_columns("f")
        
        sql = f"""
            select {columns}
                 from files f, {temp_table} t
                 where t.id = f.id or f.namespace = t.namespace and f.name = t.name
        """
        
        #print("   sql:", sql)
        
        #c.execute(sql)
        #for row in c.fetchall():
        #    print(row)

        return DBFileSet.from_sql(db, sql)
        
    @staticmethod
    def get(db, fid = None, namespace = None, name = None, with_metadata = False):
        assert (namespace is None) == (name is None), "Both name and namespace must be specified or both omited"
        assert (fid is None) != (name is None), "Either FID or namespace/name must be specified, but not both"
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

    # file attributes returned as JSON attributes not as part of the metadata
    Properties = "fid,namespace,name,checksums,size,creator,created_timestamp,parents,children,datasets".split(',')

    def to_jsonable(self, with_datasets = False, with_metadata = False, with_provenance=False):
        ns = self.Name if self.Namespace is None else self.Namespace + ':' + self.Name
        data = dict(
            fid = self.FID,
            namespace = self.Namespace,
            name = self.Name,
            retired = self.Retired,
            retired_by = self.RetiredBy,
            updated_by = self.UpdatedBy,
            retired_timestamp = None,
            updated_timestamp = None,
            created_timestamp = None
        )
        if self.Checksums is not None:  data["checksums"] = self.Checksums
        if self.Size is not None:       data["size"] = self.Size
        if self.Creator is not None:    data["creator"] = self.Creator
        if self.CreatedTimestamp is not None:    data["created_timestamp"] = epoch(self.CreatedTimestamp)
        if self.RetiredTimestamp is not None:    data["retired_timestamp"] = epoch(self.RetiredTimestamp)
        if self.UpdatedTimestamp is not None:    data["updated_timestamp"] = epoch(self.UpdatedTimestamp)
        if with_metadata:     data["metadata"] = self.metadata()
        if with_provenance:   
            data["parents"] = [{"fid":fid} for fid in self.parents()]
            data["children"] = [{"fid":fid} for fid in self.children()]
        if with_datasets:
            data["datasets"] = [{"namespace":ns, "name":n} for ns, n in self.datasets]
        return data

    def to_json(self, with_metadata = False, with_datasets = False, with_provenance=False):
        return json.dumps(self.to_jsonable(with_metadata=with_metadata, with_provenance=with_provenance, with_datasets=with_datasets))
        
    def parents(self, as_files=False, with_metadata = False):
        if self.Parents is None:
            c = self.DB.cursor()
            c.execute(f"""
                select parent_id from parent_child where child_id = %s
            """, (self.FID,))
            self.Parents = [fid for (fid,) in c.fetchall()]
        if as_files:
            return self.get_files(self.DB, [{"fid":fid} for fid in self.Parents])
        else:
            return self.Parents

    def children(self, as_files=False, with_metadata = False):
        if self.Children is None:
            c = self.DB.cursor()
            c.execute(f"""
                select child_id from parent_child where parent_id = %s
            """, (self.FID,))
            self.Children = [fid for (fid,) in c.fetchall()]
        if as_files:
            return self.get_files(self.DB, [{"fid":fid} for fid in self.Children])
        else:
            return self.Children

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

class _DatasetParentToChild(_DBManyToMany):
    
    def __init__(self, db, parent):
        _DBManyToMany.__init__(self, db, "datasets_parent_child", "child_namespace", "child_name", 
                    parent_namespace = parent.Namespace, parent_name = parent.Name)
                    
class DBDataset(DBObject):
    
    ColumnsText = "namespace,name,frozen,monotonic,metadata,creator,created_timestamp,description,file_metadata_requirements,file_count"
    Columns = ColumnsText.split(",")
    Table = "datasets"
    

    def __init__(self, db, namespace, name, frozen=False, monotonic=False, metadata={}, file_meta_requirements=None, creator=None,
            description = None, file_count = 0):
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
        self.FileCount = file_count
    
    def __str__(self):
        return "DBDataset(%s:%s)" % (self.Namespace, self.Name)
        
    def did(self):
        return "%s:%s" % (self.Namespace, self.Name)
        
    @staticmethod
    def from_tuple(db, tup):
        namespace, name, frozen, monotonic, metadata, creator, created_timestamp, description, file_metadata_requirements, file_count = tup
        dataset = DBDataset(db, namespace, name, 
            frozen=frozen, monotonic=monotonic, metadata=metadata, file_meta_requirements=file_metadata_requirements,
            file_count = file_count)
        dataset.Creator = creator
        dataset.CreatedTimestamp = created_timestamp
        dataset.Description = description
        return dataset
        
    def create(self, do_commit = True):
        c = self.DB.cursor()
        namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace
        meta = json.dumps(self.Metadata or {})
        file_meta_requirements = json.dumps(self.FileMetaRequirements or {})
        #print("DBDataset.save: saving")
        column_names = self.columns(exclude="created_timestamp")        # use DB default for creation
        c.execute(f"""
            insert into datasets({column_names}) 
                values(%s, %s, %s, %s, %s, %s, %s, %s, 0)
                returning created_timestamp
            """,
            (namespace, self.Name, self.Frozen, self.Monotonic, meta, self.Creator, 
                    self.Description, file_meta_requirements
            )
        )
        self.CreatedTimestamp = c.fetchone()[0]
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
                set frozen=%s, monotonic=%s, metadata=%s, description=%s, file_metadata_requirements=%s, file_count=%s
                where namespace=%s and name=%s
            """,
            (   self.Frozen, self.Monotonic, meta, self.Description, file_meta_requirements, self.FileCount,
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

    def parent_count(self):
        # immediate children as filled DBDataset objects
        c = self.DB.cursor()
        columns = self.columns("p")
        c.execute(f"""select count(*)
                        from datasets p, datasets_parent_child pc
                        where pc.child_namespace=%s and pc.child_name=%s
                            and pc.parent_namespace=p.namespace
                            and pc.parent_name=p.name
                        """, (self.Namespace, self.Name)
        )
        return c.fetchone()[0]
        
    def child_count(self,):
        # immediate children as filled DBDataset objects
        c = self.DB.cursor()
        columns = self.columns("c")
        c.execute(f"""select count(*)
                        from datasets c, datasets_parent_child pc
                        where pc.parent_namespace=%s and pc.parent_name=%s
                            and pc.child_namespace=c.namespace
                            and pc.child_name=c.name
                        """, (self.Namespace, self.Name)
        )
        return c.fetchone()[0]

    def add_child(self, child):
        _DatasetParentToChild(self.DB, self).add(child.Namespace, child.Name)
    
    def remove_child(self, child):
        _DatasetParentToChild(self.DB, self).remove(child.Namespace, child.Name)
    
    def add_file(self, f, **args):
        return self.add_files([f], **args)
        
    def add_files(self, files, do_commit=True, validate_meta=True):
        if isinstance(files, DBFile):
            files = [files]
        meta_errors = []
        c = self.DB.cursor()
        c.execute("begin")
        t = int(time.time()*1000) % 1000000
        temp_table = f"temp_{t}"
        try:
            c.execute(f"create temp table if not exists {temp_table} (fid text, namespace text, name text)")
            c.execute(f"truncate table {temp_table}")
            nfiles = 0
            for chunk in chunked(files, 1000):
                if validate_meta:
                    for f in chunk:
                        assert isinstance(f, DBFile)
                        errors = self.validate_file_metadata(f.Metadata)
                        if errors:
                            meta_errors += errors

                csv = "\n".join(["%s\t%s\t%s" % (f.FID, f.Namespace, f.Name) for f in chunk])
                c.copy_from(io.StringIO(csv), temp_table, columns = ["fid", "namespace", "name"])
                nfiles += len(chunk)

            if meta_errors:
                raise MetaValidationError("File metadata validation errors", meta_errors)

            c.execute(f"""
                insert into files_datasets(file_id, dataset_namespace, dataset_name) 
                    select distinct f.id, %s, %s 
                        from {temp_table} tt, files f
                        where tt.fid = f.id or tt.namespace = f.namespace and tt.name = f.name
                    on conflict do nothing""", (self.Namespace, self.Name))
            c.execute(f"drop table {temp_table}")
            c.execute(f"""
                update datasets
                    set file_count = file_count + %s
                    where namespace = %s and name = %s
            """, (nfiles, self.Namespace, self.Name))
            c.execute("commit")
        except:
            c.execute("rollback")
            raise
        return self

    def list_files(self, with_metadata=False, limit=None, include_retired_files=False):
        meta = "null as metadata" if not with_metadata else "f.metadata"
        limit = f"limit {limit}" if limit else ""
        retired = "" if include_retired_files else "and not f.retired"
        sql = f"""select f.id, f.namespace, f.name, {meta}, f.size, f.checksums, f.creator, f.created_timestamp 
                    from files f
                        inner join files_datasets fd on fd.file_id = f.id
                    where fd.dataset_namespace = %s and fd.dataset_name=%s
                        {retired}
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
    def exists(db, namespace, name):
        return DBDataset.get(db, namespace, name) is not None

    @staticmethod
    def list(db, namespace=None, parent_namespace=None, parent_name=None, creator=None, namespaces=None):
        namespace = namespace.Name if isinstance(namespace, DBNamespace) else namespace
        parent_namespace = parent_namespace.Name if isinstance(parent_namespace, DBNamespace) else parent_namespace
        creator = creator.Username if isinstance(creator, DBUser) else creator
        
        params = dict(
            namespace=namespace,
            parent_namespace=parent_namespace,
            parent_name=parent_name,
            creator=creator,
            namespace_names=namespaces or [] 
        )
        c=db.cursor()
        columns = DBDataset.columns("ds")
        
        if parent_namespace or parent_name:
            sql=f"""select {columns}
                            from datasets ds, datasets_parent_child pc
                            where true
            """
            if parent_name:
                sql += " and pc.parent_name=%(parent_name)s and pc.child_name=ds.name"
            if parent_namespace:
                sql += " and pc.parent_namspace=%(parent_namspace)s and pc.child_namespace=ds.namespace"
        else:
            sql=f"""select {columns}
                            from datasets ds
                            where true
                            """
        if namespace is not None:
            sql += " and ds.namespace=%(namespace)s"
        if namespaces is not None:
            sql += " and ds.namespace=any(%(namespace_names)s)"

        #print(sql % params)
        c.execute(sql, params) 
        for tup in fetch_generator(c):
            #print(tup)
            yield DBDataset.from_tuple(db, tup)

    def nfiles(self, exact=False):
        c = self.DB.cursor()
        if exact:
            c.execute("""select count(*) 
                            from files_datasets fd, files f
                            where fd.dataset_namespace=%s and fd.dataset_name=%s
                                and fd.file_id = f.id
                                and not f.retired
                            """, (self.Namespace, self.Name))
        else:
            c.execute(f"""
                select file_count from {self.Table}
                    where namespace = %s and name = %s
            """, (self.Namespace, self.Name))
        return c.fetchone()[0]     
    
    def to_jsonable(self, with_relatives=False):
        out = dict(
            namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace,
            name = self.Name,
            frozen = self.Frozen,
            monotonic = self.Monotonic,
            metadata = self.Metadata or {},
            creator = self.Creator,
            created_timestamp = epoch(self.CreatedTimestamp),
            file_meta_requirements = self.FileMetaRequirements,
            description = self.Description
        )
        if with_relatives:
            out["parents"] = [
                p.Namespace + ":" + p.Name for p in self.parents()
            ]
            out["children"] = [
                c.Namespace + ":" + c.Name for c in self.children()
            ]
        return out
    
    
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
    def datasets_for_bdq(db, bdq, limit=None):
        #print("datasets_for_bdq: bdq:", bdq)
        if not (bdq.Namespace and bdq.Name):
            name_or_pattern = bdq.Name
            raise ValueError(f"Dataset specification error: {selector.Namespace}:{name_or_pattern}")
        if bdq.is_explicit():
            out = [(bdq.Namespace, bdq.Name)]
        else:
            sql = DBDataset.sql_for_basic_dataset_query(bdq)
            debug("datasets_for_bdq: sql: " + sql)
            c = db.cursor()
            c.execute(sql)
            datasets = (DBDataset.from_tuple(db, tup) for tup in fetch_generator(c))
            out = limited(((ds.Namespace, ds.Name) for ds in datasets), limit)
        return out

    @staticmethod
    def datasets_for_bdqs(db, bdq_list):
        for bdq in bdq_list:
            debug("datasets_for_bdqs: bdq:", bdq)
            for ds in DBDataset.datasets_for_bdq(db, bdq):
                yield ds

    @staticmethod
    def datasets_from_sql(db, sql):
        c = db.cursor()
        c.execute(sql)
        return (DBDataset.from_tuple(db, tup) for tup in fetch_generator(c))

    @staticmethod
    def sql_for_bdq(bdq, names_only=False):
            namespace = bdq.Namespace
            name = bdq.Name
            columns = ["namespace", "name"] if names_only else DBDataset.columns(as_text=False)
            table = DBDataset.Table
            
            if bdq.is_explicit():
                a = alias("exp")
                columns = ",".join([f"{a}.{c}" for c in columns])
                return dedent(f"""\
                    select {columns} 
                        from {table} {a} 
                        where namespace='{namespace}' and name='{name}'
                """)

            pattern = bdq.Pattern
            regexp = bdq.RegExp
            with_children = bdq.WithChildren
            recursively = bdq.Recursively
            where = bdq.Where

            pc = alias("pc")
            pc1 = alias("pc1")
            d = alias("d")
            s = alias("s")
            ds = alias("ds")
            t = alias("t")

            meta_filter_dnf = MetaExpressionDNF(where) if where is not None else None

            name_cmp_op = "=" if not pattern else (
                "~" if regexp else "like"
            )

            name_cmp = f"{ds}.name {name_cmp_op} '{name}'"

            if not with_children:
                #print([f"{ds}.{c}" for c in columns])
                columns = ",".join([f"{ds}.{c}" for c in columns])
                #print("columns:", columns)
                meta_filter = ""
                if meta_filter_dnf is not None:
                    meta_filter = "and " + meta_filter_dnf.sql(ds)
                sql = dedent(f"""\
                    select {columns} 
                        from {table} {ds} 
                        where {ds}.namespace = '{namespace}' 
                            and {name_cmp}
                            {meta_filter}
                """)
                if meta_filter_dnf is not None:
                    sql += " and " + meta_filter_dnf.sql(ds)
            else:
                columns = ",".join(f"{d}.{c}" for c in columns)
                top_sql = dedent(f"""\
                    select {ds}.namespace, {ds}.name, array[{ds}.namespace || ':' || {ds}.name], false
                        from {table} {ds}
                        where {ds}.namespace = '{namespace}' and {name_cmp}
                    """)

                if not recursively:
                    selected_sql = insert_sql(dedent(f"""\
                        with selected_datasets (namespace, name, path, loop) as 
                        (
                            with {t} (namespace, name, path, loop) as 
                            ( 
                                $top_sql
                            )
                            select namespace, name, path, loop from {t}
                            union
                                select {pc}.child_namespace, {pc}.child_name,
                                    {t}.path || ({pc}.child_namespace || ':' || {pc}.child_name), true
                                from datasets_parent_child {pc}, {t}
                                where {pc}.parent_namespace = {t}.namespace and {pc}.parent_name = {t}.name
                        )"""), top_sql=top_sql)
                else:
                    selected_sql = insert_sql(dedent(f"""\
                        with recursive selected_datasets (namespace, name, path, loop) as 
                        (
                            $top_sql
                            union
                                select {pc1}.child_namespace, {pc1}.child_name,
                                    {s}.path || ({pc1}.child_namespace || ':' || {pc1}.child_name),
                                    {pc1}.child_namespace || ':' || {pc1}.child_name = any({s}.path)
                                from datasets_parent_child {pc1}, selected_datasets {s}
                                where {pc1}.parent_namespace = {s}.namespace and {pc1}.parent_name = {s}.name and not {s}.loop
                        )"""), top_sql=top_sql)
                meta_condition = "and " + meta_filter_dnf.sql(d) if meta_filter_dnf is not None else ""
                sql = insert_sql(f"""\
                    $selected_sql
                    select distinct {columns} 
                        from selected_datasets {s}, datasets {d}
                        where {d}.namespace = {s}.namespace and
                            {d}.name = {s}.name
                            {meta_condition}
                """, selected_sql=selected_sql)
            debug(f"sql_for_basic_dataset_query({bdq}): sql:\n", sql)
            return sql

    @staticmethod
    def sql_for_bdqs(bdqs, names_only=False):
        explicits = [q for q in bdqs if q.is_explicit()]
        others = [q for q in bdqs if not q.is_explicit()]
        
        parts = []
        if explicits:
            table = DBDataset.Table
            columns = ["namespace", "name"] if names_only else DBDataset.columns(as_text=False)
            a = alias("exp")
            columns = ",".join(f"{a}.{c}" for c in columns)
            pairs = tuple((q.Namespace, q.Name) for q in explicits)
            pairs = ','.join(str(pair) for pair in pairs)
            sql = f"""\
                select {columns}
                    from {table} {a}
                    where ({a}.namespace, {a}.name) in ({pairs})
            """
            sql = f"""\
                values {pairs}
            """
            parts.append(sql)
        parts.extend([DBDataset.sql_for_bdq(q, names_only) for q in others])
        return"\nunion\n".join(dedent(p) for p in parts)

    def validate_file_metadata(self, meta):
        """
        File metadata requirements:
        {
            "name":
            {
                "required":true/false,  # optional, default 'false'
                "values":[...],         # optional
                "min":  value,          # optional
                "max":  value,          # optional
                "pattern": "re pattern" # optional
            },
            ...
        }
        """
        error_list = validate_metadata(self.FileMetaRequirements or {}, False, meta)
        errors = []
        for name, error in error_list:
            v = meta.get(name)
            errors.append({"name":name, "value":v, "reason":error})
        return errors
        
    @staticmethod
    def datasets_for_files(db, files):
        #
        # files: list of DBFile objects. Each DBFile has either valid FID or Namespace, Name
        #        or dicts:
        #           {"namespace":"...", "name":"..."}
        #           {"fid":"..."}
        #
        # returns dict:
        #    { "file_id" -> [DBDataset, ...] }
        #    for files specified with namespace/name pairs, the output dictionary will also contain
        #    { ("namespace", "name") -> [DBDataset, ...] }
        #
        file_ids = set()
        pairs = []
        for f in files:
            if isinstance(f, DBFile):
                if f.FID:
                    file_ids.add(f.FID)
                else:
                    pairs.append(dict(namespace=f.Namespace, name=f.Name))
            elif isinstance(f, dict):
                fid = f.get("fid")
                if fid:
                    file_ids.add(fid)
                else:
                    namespace, name = f.get("namespace"), f.get("name")
                    if not (namespace and name):
                        raise ValueError("Unrecognozed file specification:", f)
                    pairs.append((namespace, name))
            else:
                raise ValueError("Unrecognozed file specification:", f)

        fid_to_pair = {}
        if pairs:
            pair_files = DBFile.get_files(db, pairs)
            for f in pair_files:
                file_ids.add(f.FID)
                fid_to_pair[f.FID] = (f.Namespace, f.Name)

        dataset_map = {}       # { fid -> [DBDataset, ...]}
        datasets = {}       # {(ns,n) -> DBDataset}
        c = db.cursor()
        ds_columns = self.columns("ds")
        
        c.execute(f"""
            select distinct fd.file_id, {ds_columns}
                        from datasets ds, files_datasets fd
                        where fd.dataset_namespace = ds.namespace and fd.dataset_name = ds.name and fd.file_id = any(%s)
                        order by fd.file_id, ds.namespace, ds.name
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
            
            ns_n_pair = fid_to_pair.get(fid)
            if ns_n_pair:
                dslist = dataset_map.setdefault(ns_n_pair, [])
                dslist.append(ds)

        return dataset_map

class DBNamedQuery(DBObject):
    
    Columns = "namespace,name,parameters,source,creator,created_timestamp".split(",")
    Table = "queries"
    PK = ["namespace", "name"]

    def __init__(self, db, namespace, name, source, parameters=[]):
        assert namespace is not None and name is not None
        self.DB = db
        self.Namespace = namespace
        self.Name = name
        self.Source = source
        self.Parameters = parameters
        self.Creator = None
        self.CreatedTimestamp = None
        
    def to_jsonable(self):
        return dict(
            namespace = self.Namespace,
            name = self.Name,
            source = self.Source,
            creator = self.Creator,
            created_timestamp = epoch(self.CreatedTimestamp),
            parameters = self.Parameters
        )

    @staticmethod
    def from_tuple(db, tup):
        namespace, name, parameters, source, creator, created_timespamp = tup
        #print("DBNamedQuery.from_tuple:", tup)
        query = DBNamedQuery(db, namespace, name, source, parameters)
        query.Creator = creator
        query.CreatedTimestamp = created_timespamp
        return query
        
    def create(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into queries(namespace, name, source, parameters, creator) values(%s, %s, %s, %s, %s)
            returning created_timestamp""",
            (self.Namespace, self.Name, self.Source, self.Parameters, self.Creator)
        )
        self.CreatedTimestamp = c.fetchone()[0]
        if do_commit:
            c.execute("commit")
        return self
            
    def save(self):
        self.DB.cursor().execute("""
            update queries 
                set source=%s, parameters=%s, creator=%s, created_timestamp=%s
                where namespace=%s and name=%s;
            commit
            """,
            (self.Source, self.Parameters, self.Creator, self.CreatedTimestamp, self.Namespace, self.Name)
        )
        return self
            
    @staticmethod
    def list(db, namespace=None):
        c = db.cursor()
        columns = DBNamedQuery.columns()
        if namespace is not None:
            c.execute(f"""select {columns}
                        from queries
                        where namespace=%s
                        order by namespace, name
                        """,
                (namespace,)
            )
        else:
            c.execute(f"""select {columns}
                        from queries
                        order by namespace, name
                        """
            )
        return (DBNamedQuery.from_tuple(db, tup) for tup in fetch_generator(c))


class DBUser(BaseDBUser):

    @staticmethod
    def from_base_user(bu):
        if bu is None:  return None
        u = DBUser(bu.DB, bu.Username, bu.Name, bu.EMail, bu.Flags, (bu.AuthInfo or {}).copy(), bu.AUID)
        u.RoleNames = bu.RoleNames
        if isinstance(u.RoleNames, list):
            u.RoleNames = u.RoleNames[:]
        return u

    @staticmethod
    def get(db, username):
        return DBUser.from_base_user(BaseDBUser.get(db, username))

    @staticmethod 
    def list(db):
        return (DBUser.from_base_user(u) for u in BaseDBUser.list(db))

    @property
    def roles(self):
        return _DBManyToMany(self.DB, "users_roles", "role_name", username = self.Username)
        
    def namespaces(self):
        return DBNamespace.list(self.DB, owned_by_user=self)        
        
    def add_role(self, role):
        self.roles.add(role.Name if isinstance(role, DBRole) else role)

    def remove_role(self, role):
        self.roles.remove(role.Name if isinstance(role, DBRole) else role)

class DBNamespace(DBObject):

    Columns = "name,owner_user,owner_role,description,creator,created_timestamp,file_count".split(",")
    Table = "namespaces"
    PK = ["name"]

    def __init__(self, db, name, owner_user=None, owner_role=None, description=None, 
                creator=None, created_timestamp=None, file_count=0):
        self.DB = db
        self.Name = name
        assert None in (owner_user, owner_role)
        self.OwnerUser = owner_user
        self.OwnerRole = owner_role
        self.Description = description
        self.Creator = creator
        self.CreatedTimestamp = created_timestamp
        self.FileCount = file_count
        
    @staticmethod
    def from_tuple(db, tup):
        name, owner_user, owner_role, description, creator, created_timestamp, file_count = tup
        ns = DBNamespace(db, name, owner_user, owner_role, description, creator, created_timestamp, file_count)
        return ns
        
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
        c.execute(f"""
            update {self.Table}
                set owner_user=%s, owner_role=%s, description=%s, file_count=%s
                where name=%s
            """,
            (self.OwnerUser, self.OwnerRole, self.Description, self.FileCount,
                self.Name)
        )
        if do_commit:
            c.execute("commit")
        return self

    def create(self, do_commit=True):
        c = self.DB.cursor()
        c.execute("""
            insert into namespaces(name, owner_user, owner_role, description, creator) values(%s, %s, %s, %s, %s)
                returning created_timestamp
            """,
            (self.Name, self.OwnerUser, self.OwnerRole, self.Description, self.Creator))
        self.CreatedTimestamp = c.fetchone()[0]
        if do_commit:
            c.execute("commit")
        return self
        
    @staticmethod
    def get_many(db, names):
        #print("DBNamespace.get: name:", name)
        c = db.cursor()
        columns = self.columns()
        c.execute(f"""select {columns}
                from {self.Table} where name=any(%s)""", (list(names),))
        return DBNamespace.from_tuples(db, fetch_generator(c))

    @staticmethod
    def list(db, owned_by_user=None, owned_by_role=None, directly=False):
        c = db.cursor()
        columns = DBNamespace.columns("ns")
        table = DBNamespace.Table
        if isinstance(owned_by_user, DBUser):   owned_by_user = owned_by_user.Username
        if isinstance(owned_by_role, DBRole):   owned_by_role = owned_by_role.Name
        if owned_by_user is not None:
            sql = f"""
                select {columns}
                        from {table} ns
                        where ns.owner_user=%s
            """
            args = (owned_by_user,)
            if not directly:
                sql += f"""
                    union
                    select {columns}
                            from {table} ns, users_roles ur
                            where ur.username = %s and ur.role_name = ns.owner_role
                """
                args = args + (owned_by_user,)
        elif owned_by_role is not None:
            sql = f"""select {columns}
                        from {table} ns
                        where ns.owner_role=%s
                        order by name
            """
            args = (owned_by_role,)
        else:
            sql = f"""select {columns}
                        from {table} ns
                        order by name
            """
            args = ()
        c.execute(sql, args)
        return DBNamespace.from_tuples(db, fetch_generator(c))

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

