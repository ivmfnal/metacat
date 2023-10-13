import uuid, json, hashlib, re, time, io, traceback, base64
from metacat.util import (to_bytes, to_str, epoch, chunked, limited, strided, 
    skipped, first_not_empty, validate_metadata, insert_sql, fetch_generator
)
from metacat.auth import BaseDBUser, BaseDBRole as DBRole
from metacat.common import FileMetaExpressionDNF, DatasetMetaExpressionDNF, DBObject, DBManyToMany, transactioned, insert_many
from metacat.util import ObjectSpec
from psycopg2 import IntegrityError
from textwrap import dedent
from datetime import datetime, timezone

Debug = False

def debug(*parts):
    if Debug:
        print("[debug]", *parts)

from .common import (
    AlreadyExistsError, DatasetCircularDependencyDetected, NotFoundError, MetaValidationError,
    parse_name, alias
)

class DBFileSet(DBObject):
    
    class FileSetSummary(object):
        
        def __init__(self, db, sql):
            self.DB = db
            self.SQL = sql
            
        def __iter__(self):
            c = self.DB.cursor()
            c.execute(self.SQL)
            return fetch_generator(c)

    def __init__(self, db, files=None, sql=None, count=None):
        DBObject.__init__(self, db)
        assert not (files and sql), "DBFileSet can not be initialized from both files and sql"
        if not sql and not files:
            files = []          # empty file set
        self.Files = files
        self.SQL = sql
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
        assert all(s["namespace"] for s in specs), "Incomplete file specification:"
        specs = [(s.get("namespace", default_namespace), s["name"]) for s in specs]
        just_names = set(name for ns, name in specs)
        dids = set("%s:%s" % t for t in specs)
        c = db.cursor()
        columns = DBFile.all_columns()
        c.execute(f"""
            select {columns} from files
                where name = any(%s)""", (list(just_names),))
        files = DBFileSet.from_tuples(db, fetch_generator(c))
        return DBFileSet(db, (f for f in files if "%s:%s" % (f.Namespace, f.Name) in dids))
        
    def __iter__(self):
        if self.Files is not None:
            return (f for f in self.Files)
        else:
            c = self.DB.cursor()
            c.execute(self.SQL)
            debug("DBFileSet.from_sql: return from execute()")
            return (f for f in DBFileSet.from_tuples(self.DB, fetch_generator(c), count=c.rowcount))

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
        return DBFileSet.union(self.DB, [self, other])
        
    @staticmethod
    def from_basic_query(db, basic_file_query, with_metadata, limit):
        if limit is None:
            limit = basic_file_query.Limit
        elif basic_file_query.Limit is not None:
            limit = min(limit, basic_file_query.Limit)
            
        bdq = basic_file_query.DatasetSelector
        assert bdq is not None
        datasets = None
        if bdq is not None:
            datasets = list(bdq.datasets(db))
            if not datasets:
                return DBFileSet(db)      # empty File Set

        #if bdq is None:
        #    return DBFileSet.all_files(db, dnf, with_metadata, limit)
            
        if len(datasets) == 1:
            return datasets[0].list_files(with_metadata = with_metadata, condition=basic_file_query.Wheres, limit=limit,
                        relationship = basic_file_query.Relationship)
        else:
            return DBFileSet.union(
                db,
                [   ds.list_files(
                        with_metadata = with_metadata, condition=basic_file_query.Wheres,
                        relationship = basic_file_query.Relationship, limit=limit
                        )
                    for ds in datasets
                ]
            )
        
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

        file_meta_exp = FileMetaExpressionDNF(basic_file_query.Wheres).sql(f) or "true"
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
            distinct = "" if basic_file_query.single_dataset() else "distinct"
            sql = insert_sql(f"""\
                -- sql_for_basic_query {f}
                    select {distinct} {f}.id, {f}.namespace, {f}.name, {meta}, {attrs}, {parents}, {children}
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

    def counts(self):
        total_size = n = 0
        if self.SQL:
            sql = insert_sql(f"""
                -- summary:count
                    select count(*), sum(size)
                    from (
                        $fileset_sql
                    ) keys
                -- end of summary:count
            """, fileset_sql=self.SQL)
            c = self.DB.cursor()
            c.execute(sql)
            n, total_size = c.fetchone()
        else:
            for f in self:
                n += 1
                total_size += f.Size
        return (n or 0, int(total_size or 0))

    def metadata_keys(self):
        if self.SQL:
            sql = insert_sql(f"""
                -- summary:keys
                    select distinct jsonb_object_keys(metadata) as key
                    from (
                        $fileset_sql
                    ) keys
                    order by key
                -- end of summary:keys
            """, fileset_sql=self.SQL)
            c = self.DB.cursor()
            c.execute(sql)
            for tup in fetch_generator(c):
                yield tup[0]
        else:
            seen = set()
            for f in self:
                for k in f.metadata():
                    if not k in seen:
                        seen.add(k)
                        yield k
    
    def metadata_key_values(self):
        if self.SQL:
            sql = insert_sql(f"""
                -- summary:key/values
                    select distinct (jsonb_each(metadata)).*
                    from (
                        $fileset_sql
                    ) keyvalues
                    order by key
                -- end of summary:key/values
            """, fileset_sql=self.SQL)
            c = self.DB.cursor()
            c.execute(sql)
            yield from fetch_generator(c)
        else:
            seen = {}
            for f in self:
                for k, v in f.metadata():
                    if not k in seen or v != seen[k]:
                        seen[k] = v
                        yield k, v
                        
    def summary(self, mode):
        if mode is None:
            return self
        elif mode == "keys":
            return self.metadata_keys()
        elif mode == "key-values":
            return self.metadata_key_values()
        elif mode == "count":
            return self.count()

class DBFile(DBObject):
    
    Table = "files"
    
    def __init__(self, db, namespace = None, name = None, metadata = None, fid = None, size=None, checksums=None,
                    parents = None, children = None, creator = None, created_timestamp=None,
                    updated_timestamp = None, updated_by = None,
                    retired = False, retired_timestamp = None, retired_by = None
                    ):

        DBObject.__init__(self, db)
        assert (namespace is None) == (name is None)
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
            
    @transactioned
    def delete(self, transaction=None):
        # delete the file from the DB
        transaction.execute("""
                delete from parent_child where parent_id = %s;
                delete from parent_child where child_id = %s;
                delete from files_datasets where file_id = %s;
                delete from files where id = %s;
            """, (self.FID, self.FID, self.FID, self.FID))

    @transactioned
    def create(self, creator=None, transaction=None):
        from psycopg2 import IntegrityError
        meta = json.dumps(self.Metadata or {})
        checksums = json.dumps(self.Checksums or {})
        transaction.execute("""
            insert into files(id, namespace, name, metadata, size, checksums, creator) values(%s, %s, %s, %s, %s, %s, %s)
                returning created_timestamp
            """,
            (self.FID, self.Namespace, self.Name, meta, self.Size, checksums, creator))
        self.CreatedTimestamp = c.fetchone()[0]
        if self.Parents:
            insert_many(self.DB,
                "parent_child", 
                ((p.FID if isinstance(p, DBFile) else p, self.FID) for p in self.Parents),
                column_names=["parent_id", "child_id"], 
                transaction=transaction
            )
        return self

    def did(self):
        return f"{self.Namespace}:{self.Name}"

    @staticmethod
    @transactioned
    def create_many(db, files, creator, transaction=None):
        if isinstance(creator, DBUser):
            creator = DBUser.Username
        files = list(files)
        files_csv = []
        parents_csv = []
        null = r"\N"
        for f in files:
            f.FID = f.FID or DBFile.generate_id()
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
        
        files_data = "\n".join(files_csv)
        #open("/tmp/files.csv", "w").write(files_data)
        transaction.copy_from(io.StringIO(files_data), "files", 
                columns = ["id", "namespace", "name", "metadata", "size", "checksums","creator"])
        transaction.copy_from(io.StringIO("\n".join(parents_csv)), "parent_child", 
                columns=["child_id", "parent_id"])
            
        return DBFileSet(db, files)

        
    @transactioned
    def update(self, user, transaction=None):
        if isinstance(user, DBUser):
            user = user.Username
        from psycopg2 import IntegrityError
        meta = json.dumps(self.Metadata or {})
        checksums = json.dumps(self.Checksums or {})
        transaction.execute("""
                update files set namespace=%s, name=%s, metadata=%s, size=%s, checksums=%s,
                    updated_by=%s, updated_timestamp = now()
                    where id = %s
                """, (self.Namespace, self.Name, meta, self.Size, checksums, user,
                        self.FID)
            )
        return self
        
    @transactioned
    def set_retire(self, retire, user, transaction=None):
        #print("set_retire:", retire, user)
        from psycopg2 import IntegrityError
        if retire != self.Retired:
            if retire:
                self.RetiredTimestamp = datetime.now(timezone.utc)
                self.RetiredBy = user
                transaction.execute("""
                    update files set retired=true, retired_by=%s, retired_timestamp = %s
                        where id = %s
                    """, (self.RetiredBy, self.RetiredTimestamp, self.FID)
                )
            else:
                self.UpdatedTimestamp = datetime.now(timezone.utc)
                self.UpdatedBy = user
                transaction.execute("""
                    update files set retired=false, updated_by=%s, updated_timestamp = %s
                        where id = %s
                    """, (self.UpdatedBy, self.UpdatedTimestamp, self.FID)
                )
            self.Retired = retire
        return self

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
    @transactioned
    def update_many(db, files, transaction=None):
        from psycopg2 import IntegrityError
        tuples = [
            (f.Namespace, f.Name, json.dumps(f.Metadata or {}), f.Size, json.dumps(f.Checksums or {}), f.FID)
            for f in files
        ]
        #print("tuples:", tuples)
        transaction.executemany("""
            update files
                set namespace=%s, name=%s, metadata=%s, size=%s, checksums=%s
                where id=%s
            """,
            tuples)
        for f in files: f.DB = db
    
    @staticmethod
    @transactioned
    def get_files(db, files, transaction=None):
        
        #
        # NOT really THREAD SAFE !!
        #

        # files: list of dicts:
        #  { "fid": ... } or {"namespace":..., "name":...} or {"did":"namespace:name"}
        
        #print("DBFile.get_files: files:", files)
        suffix = int(time.time()*1000)
        temp_table = f"temp_files_{suffix}"
        strio = io.StringIO()
        for f in files:
            if isinstance(f, DBFile):
                ns = f.Namespace
                n = f.Name
                fid = f.FID
            else:
                try:    spec = ObjectSpec(f)
                except ValueError:
                    raise ValueError("Invalid file specificication: " + str(f))
                ns = spec.Namespace
                n = spec.Name
                fid = spec.FID
            strio.write("%s\t%s\t%s\n" % (fid or r'\N', ns or r'\N', n or r'\N'))
        transaction.execute(f"""create temp table if not exists
            {temp_table} (
                id text,
                namespace text,
                name text);
            truncate table {temp_table};
                """)
        csv = strio.getvalue()
        transaction.copy_from(io.StringIO(csv), temp_table)
        #print("DBFile.get_files: strio:", strio.getvalue())
        
        columns = DBFile.all_columns("f")
        
        sql = f"""
            select {columns}
                 from files f, {temp_table} t
                 where t.id = f.id or f.namespace = t.namespace and f.name = t.name
        """
        
        return DBFileSet(db, sql=sql)
        
    @staticmethod
    @transactioned
    def move_to_namespace(db, to_namespace, files, transaction=None, authorized_namespaces=None):
        """
        files expected to be a list of DBFile objects with correct file ids, name and namespace.
        authorized_namespaces is a set of ns names the user owns (directly or through a role)
        """

        suffix = int(time.time()*1000) % 10000
        temp_table = f"temp_fids_{suffix}"
        transaction.execute(f"create temp table {temp_table} ( id text );")
        
        errors = []
        for chunk in chunked(files, 1000):
            authorized = []
            for f in chunk:
                if authorized_namespaces is None or f.Namespace in authorized_namespaces:
                    authorized.append((f.FID,))
                else:
                    errors.append("not authorized to move file: " + f.did())
            if authorized:
                insert_many(db, temp_table, authorized, column_names=["id"], transaction=transaction)

        transaction.execute(f"""
            update files set namespace = %(ns)s
                from {temp_table} tt
                where files.id = tt.id
                    and files.namespace != %(ns)s
            """, {"ns": to_namespace}
        )
        return transaction.rowcount, errors
        
    @staticmethod
    @transactioned
    def get(db, fid = None, namespace = None, name = None, with_metadata = False, transaction=None):
        assert (namespace is None) == (name is None), "Both name and namespace must be specified or both omited"
        assert (fid is None) != (name is None), "Either FID or namespace/name must be specified, but not both"
        fetch_meta = "metadata" if with_metadata else "null"
        attrs = DBFile.attr_columns()
        if fid is not None:
            #sql = f"""select id, namespace, name, {fetch_meta}, {attrs} 
            #        from files
            #        where id = %s"""
            #print("sql:", sql)
            transaction.execute(f"""select id, namespace, name, {fetch_meta}, {attrs} 
                    from files
                    where id = %s""", (fid,))
        else:
            transaction.execute(f"""select id, namespace, name, {fetch_meta}, {attrs} 
                    from files
                    where namespace = %s and name=%s""", (namespace, name))
        tup = transaction.one()
        return DBFile.from_tuple(db, tup)
        
    @staticmethod
    @transactioned
    def exists(db, fid = None, namespace = None, name = None, transaction=None):
        #print("DBFile.exists:", fid, namespace, name)
        if fid is not None:
            assert (namespace is None) and (name is None),  "If FID is specified, namespace and name must be null"
        else:
            assert (namespace is not None) and (name is not None), "Both namespace and name must be specified"
        if fid is not None:
            transaction.execute("""select namespace, name 
                    from files
                    where id = %s""", (fid,))
        else:
            transaction.execute("""select id 
                    from files
                    where namespace = %s and name=%s""", (namespace, name))
        return transaction.fetchone() != None
        
    @transactioned
    def fetch_metadata(self, transaction=None):
        transaction.execute("""
            select metadata
                from files
                where id=%s""", (self.FID,))
        meta = None
        tup = transaction.one()
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
    @transactioned
    def list(db, namespace=None, transaction=None):
        transaction.execute("""select id, namespace, name from files
                where %s is null or namespace=%s""", (namespace, namespace))
        return DBFileSet.from_tuples(db, transaction.cursor_iterator())

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
        
    @transactioned
    def parents(self, as_files=False, with_metadata=False, transaction=None):
        if self.Parents is None:
            transaction.execute(f"""
                select parent_id from parent_child where child_id = %s
            """, (self.FID,))
            self.Parents = [fid for (fid,) in transaction]
        if as_files:
            return self.get_files(self.DB, [{"fid":fid} for fid in self.Parents])
        else:
            return self.Parents

    @transactioned
    def children(self, as_files=False, with_metadata = False, transaction=None):
        if self.Children is None:
            transaction.execute(f"""
                select child_id from parent_child where parent_id = %s
            """, (self.FID,))
            self.Children = [fid for (fid,) in transaction]
        if as_files:
            return self.get_files(self.DB, [{"fid":fid} for fid in self.Children])
        else:
            return self.Children

    @transactioned
    def add_child(self, child, transaction=None):
        child_fid = child if isinstance(child, str) else child.FID
        transaction.execute("""
            insert into parent_child(parent_id, child_id)
                values(%s, %s)        
                on conflict(parent_id, child_id) do nothing;
            """, (self.FID, child_fid)
        )
        
    @transactioned
    def add_parents(self, parents, transaction=None):
        parent_fids = [(p if isinstance(p, str) else p.FID,) for p in parents]
        transaction.executemany(f"""
            insert into parent_child(parent_id, child_id)
                values(%s, %s)        
                on conflict(parent_id, child_id) do nothing;
            """, [(fid, self.FID) for fid in parent_fids]
        )
        
    @transactioned
    def set_parents(self, fids_or_files, transaction=None):
        parent_fids = [(p if isinstance(p, str) else p.FID,) for p in fids_or_files]
        transaction.execute(f"delete from parent_child where child_id=%s", (self.FID,))
        transaction.executemany(f"""
            insert into parent_child(parent_id, child_id)
                values(%s, %s)        
            """, [(fid, self.FID) for fid in parent_fids]
        )
        
    @transactioned
    def add_children(self, children, transaction=None):
        child_fids = [(p if isinstance(p, str) else p.FID,) for p in children]
        transaction.executemany(f"""
            insert into parent_child(parent_id, child_id)
                values(%s, %s)
                on conflict(parent_id, child_id) do nothing;
            """, [(self.FID, fid) for fid in child_fids]
        )
        
    @transactioned
    def set_children(self, fids_or_files, transaction=None):
        child_fids = [(p if isinstance(p, str) else p.FID,) for p in fids_or_files]
        #print("set_parents: fids:", parent_fids)
        transaction.execute("delete from parent_child where parent_id=%s", (self.FID,))
        transaction.executemany(f"""
            insert into parent_child(parent_id, child_id)
                values(%s, %s)        
            """, [(self.FID, fid) for fid in child_fids]
        )
        
    @transactioned
    def remove_child(self, child, transaction=None):
        child_fid = child if isinstance(child, str) else child.FID
        transaction.execute("""
            delete from parent_child where
                parent_id = %s and child_id = %s;
            """, (self.FID, child_fid)
        )

    @transactioned
    def add_parent(self, parent, transaction=None):
        parent_fid = parent if isinstance(parent, str) else parent.FID
        return DBFile(self.DB, fid=parent_fid).add_child(self, transaction=transaction)
        
    @transactioned
    def remove_parent(self, parent, transaction=None):
        parent_fid = parent if isinstance(parent, str) else parent.FID
        return DBFile(self.DB, fid=parent_fid).remove_child(self, transaction=transaction)
        
    @property
    def datasets(self):
        return DBManyToMany(self.DB, "files_datasets", "dataset_namespace", "dataset_name", file_id = self.FID)

    @staticmethod
    def file_count_by_namespace(db):
        table = DBFile.Table
        c = db.cursor()
        c.execute(f"""
            select namespace, count(*) from {table} group by namespace
        """)
        return dict(fetch_generator(c))

class _DatasetParentToChild(DBManyToMany):
    
    def __init__(self, db, parent):
        DBManyToMany.__init__(self, db, "datasets_parent_child", "child_namespace", "child_name", 
                    parent_namespace = parent.Namespace, parent_name = parent.Name)
                    
class DBDataset(DBObject):
    
    ColumnsText = "namespace,name,frozen,monotonic,metadata,creator,created_timestamp,description,file_metadata_requirements,file_count"
    Columns = ['namespace', 'name', 'frozen', 'monotonic', 
            'metadata', 'creator', 'created_timestamp', 'description', 
            'file_metadata_requirements', 'file_count',
            'updated_timestamp', 'updated_by'
    ]
    Table = "datasets"
    

    def __init__(self, db, namespace, name, frozen=False, monotonic=False, metadata={}, file_meta_requirements=None, creator=None,
            description = None, file_count = 0, updated_timestamp=None, updated_by=None):
        DBObject.__init__(self, db)
        assert namespace is not None and name is not None
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
        self.UpdatedTimestamp = updated_timestamp
        self.UpdatedBy = updated_by
    
    def __str__(self):
        return "DBDataset(%s:%s)" % (self.Namespace, self.Name)
        
    def did(self):
        return "%s:%s" % (self.Namespace, self.Name)
        
    @staticmethod
    def from_tuple(db, tup):
        (namespace, name, frozen, monotonic, metadata, creator, created_timestamp, description,
            file_metadata_requirements, file_count, updated_timestamp, updated_by) = tup
        dataset = DBDataset(db, namespace, name, 
            frozen=frozen, monotonic=monotonic, metadata=metadata, file_meta_requirements=file_metadata_requirements,
            file_count = file_count, updated_timestamp=updated_timestamp, updated_by=updated_by)
        dataset.Creator = creator
        dataset.CreatedTimestamp = created_timestamp
        dataset.Description = description
        return dataset
        
    @transactioned
    def create(self, transaction=None):
        namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace
        meta = json.dumps(self.Metadata or {})
        file_meta_requirements = json.dumps(self.FileMetaRequirements or {})
        #print("DBDataset.save: saving")
        column_names = self.columns(exclude="created_timestamp")        # use DB default for creation
        transaction.execute(f"""
            insert into datasets({column_names}) 
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, null, null)
                returning created_timestamp
            """,
            (namespace, self.Name, self.Frozen, self.Monotonic, meta, self.Creator, 
                    self.Description, file_meta_requirements, self.FileCount
            )
        )
        self.CreatedTimestamp = transaction.fetchone()[0]
        return self
        
    @transactioned
    def save(self, updated_by=None, transaction=None):
        namespace = self.Namespace.Name if isinstance(self.Namespace, DBNamespace) else self.Namespace
        meta = json.dumps(self.Metadata or {})
        file_meta_requirements = json.dumps(self.FileMetaRequirements or {})
        #print("DBDataset.save: saving")
        column_names = self.columns()
        if updated_by:
            transaction.execute(f"""
                update datasets 
                    set frozen=%s, monotonic=%s, metadata=%s, description=%s, 
                        file_metadata_requirements=%s, file_count=%s,
                        updated_by=%s, updated_timestamp=now()
                    where namespace=%s and name=%s
                    returning updated_timestamp
                """,
                (   self.Frozen, self.Monotonic, meta, self.Description, file_meta_requirements, self.FileCount,
                    updated_by,
                    namespace, self.Name
                )
            )
            self.UpdatedTimestamp = transaction.fetchone()[0]
        else:
            transaction.execute(f"""
                update datasets 
                    set frozen=%s, monotonic=%s, metadata=%s, description=%s, 
                        file_metadata_requirements=%s, file_count=%s
                    where namespace=%s and name=%s
                """,
                (   self.Frozen, self.Monotonic, meta, self.Description, file_meta_requirements, self.FileCount,
                    namespace, self.Name
                )
            )
        return self

    @transactioned
    def subsets(self, exclude_immediate=False, meta_filter=None, transaction=None):
        immediate = set()
        if exclude_immediate:
            immediate = set((c.Namespace, c.Name) for c in self.children())
        columns = self.columns("d")
        meta_condition = "and " + meta_filter.sql("d") if meta_filter is not None else ""
        transaction.execute(f"""
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
        out = (DBDataset.from_tuple(self.DB, tup) for tup in transaction)
        if exclude_immediate:
            out = (ds for ds in out if (ds.Namespace, ds.Name) not in immediate)
        return out

    def subset_count(self):
        return len(list(self.subsets()))
            
    @transactioned
    def ancestors(self, exclude_immediate=False, transaction=None):
        immediate = set()
        if exclude_immediate:
            immediate = set((c.Namespace, c.Name) for c in self.parents())
        columns = self.columns("d")
        transaction.execute(f"""
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
        out = (DBDataset.from_tuple(self.DB, tup) for tup in transaction)
        if exclude_immediate:
            out = (ds for ds in out if (ds.Namespace, ds.Name) not in immediate)
        return out

    def ancestor_count(self):
        return len(list(self.ancestors()))

    @transactioned
    def children(self, meta_filter=None, transaction=None):
        # immediate children as filled DBDataset objects
        columns = self.columns("c")
        meta_where_clause = "and " + meta_filter.sql("c") if meta_filter is not None else ""
        transaction.execute(f"""select {columns}
                        from datasets c, datasets_parent_child pc
                        where pc.parent_namespace=%s and pc.parent_name=%s
                            and pc.child_namespace=c.namespace
                            and pc.child_name=c.name
                            {meta_where_clause}
                        """, (self.Namespace, self.Name)
        )
        return (DBDataset.from_tuple(self.DB, tup) for tup in transaction.results())

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
    
    @transactioned
    def add_file(self, f, transaction=None, **args):
        return self.add_files([f], transaction=transaction, **args)
        
    @transactioned
    def add_files(self, files, validate_meta=True, transaction=None):
        if isinstance(files, DBFile):
            files = [files]
        meta_errors = []
        t = int(time.time()*1000) % 1000000
        temp_table = f"temp_{t}"
        transaction.execute(f"""
            create temp table {temp_table} (fid text, namespace text, name text);
            truncate table {temp_table}
        """)
        for chunk in chunked(files, 1000):
            if validate_meta:
                for f in chunk:
                    assert isinstance(f, DBFile)
                    errors = self.validate_file_metadata(f.Metadata)
                    if errors:
                        meta_errors += errors

            csv = "\n".join(["%s\t%s\t%s" % (f.FID, f.Namespace, f.Name) for f in chunk])
            transaction.copy_from(io.StringIO(csv), temp_table, columns = ["fid", "namespace", "name"])

        if meta_errors:
            ransaction.rollback()
            raise MetaValidationError("File metadata validation errors", meta_errors)

        transaction.execute(f"""
            insert into files_datasets(file_id, dataset_namespace, dataset_name) 
                select f.id, %s, %s 
                    from {temp_table} tt, files f
                    where tt.fid = f.id or (tt.namespace = f.namespace and tt.name = f.name)
                on conflict do nothing""", (self.Namespace, self.Name))
        nadded = transaction.rowcount
        transaction.execute(f"drop table {temp_table}")
        transaction.execute(f"""
            update datasets
                set file_count = file_count + %s
                where namespace = %s and name = %s
        """, (nadded, self.Namespace, self.Name))
        return nadded

    @transactioned
    def remove_files(self, files, transaction=None):
        """
        files: iterable with DBFile objcts or file ids 
        """
        file_ids = [item.FID if isinstance(item, DBFile) else item for item in files]
        transaction.execute("""
            delete from files_datasets
                where dataset_namespace = %s
                    and dataset_name = %s
                    and file_id = any(%s)
        """, (self.Namespace, self.Name, file_ids))
        return transaction.rowcount

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
    @transactioned
    def get(db, namespace, name, transaction=None):
        namespace = namespace.Name if isinstance(namespace, DBNamespace) else namespace
        #print(namespace, name)
        columns = DBDataset.columns()
        transaction.execute(f"""select {columns}
                        from datasets
                        where namespace=%s and name=%s""",
                (namespace, name))
        tup = transaction.fetchone()
        if tup is None: return None
        return DBDataset.from_tuple(db, tup)

    @staticmethod
    @transactioned
    def get_many(db, namespaces_names, transaction=None):
        # namespaces_names is list of tuples [(namespace, name), ...]
        specs = list(set(f"{namespace}:{name}" for namespace, name in namespaces_names))
        columns = DBDataset.columns()
        transaction.execute(f"""select {columns}
                        from datasets
                        where (namespace || ':' || name) = any(%s) """,
                (specs,)
        )
        return (DBDataset.from_tuple(db, tup) for tup in transaction.results())

    @staticmethod
    @transactioned
    def exists(db, namespace, name, transaction=None):
        return DBDataset.get(db, namespace, name, transaction=transaction) is not None

    @staticmethod
    @transactioned
    def list(db, namespace=None, parent_namespace=None, parent_name=None, creator=None, namespaces=None, transaction=None):
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
        transaction.execute(sql, params)
        return (DBDataset.from_tuple(db, tup) for tup in transaction.results())

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
            description = self.Description,
            file_count = self.FileCount,
            updated_timestamp = epoch(self.UpdatedTimestamp),
            updated_by = self.UpdatedBy
        )
        if with_relatives:
            out["parents"] = [
                p.Namespace + ":" + p.Name for p in self.parents()
            ]
            out["children"] = [
                c.Namespace + ":" + c.Name for c in self.children()
            ]
        return out
    
    
    @transactioned
    def delete(self, transaction=None):
        transaction.execute("""
            delete from datasets where namespace=%s and name=%s
        """, (self.Namespace, self.Name))
        
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
            specs = subsets_rec(c, specs, set(), level=None if recursively else 0)              # FIXME
        return DBDataset.get_many(db, [spec.split(":",1) for spec in specs])    

    @staticmethod
    def datasets_for_bdq(db, bdq, limit=None):
        #print("datasets_for_bdq: bdq:", bdq)
        if not (bdq.Namespace and bdq.Name):
            name_or_pattern = bdq.Name
            raise ValueError(f"Dataset specification error: {bdq.Namespace}:{name_or_pattern}")
        if bdq.is_explicit():
            out = [(bdq.Namespace, bdq.Name)]
        else:
            sql = DBDataset.sql_for_bdq(bdq)
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
                sql = f"""\
                        select {columns} 
                            from {table} {a}
                            where {a}.namespace = '{namespace}' and {a}.name = '{name}'
                """
                #print("was explicit. SQL:", sql)
                return dedent(sql)

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

            meta_filter_dnf = DatasetMetaExpressionDNF(where) if where is not None else None

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
                #if meta_filter_dnf is not None:
                #    sql += " and " + meta_filter_dnf.sql(ds)
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
            #sql = f"""\
            #    values {pairs}
            #"""
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
        ds_columns = DBDataset.columns("ds")
        
        c.execute(f"""
            select distinct fd.file_id, {ds_columns}
                        from datasets ds, files_datasets fd
                        where fd.dataset_namespace = ds.namespace and fd.dataset_name = ds.name and fd.file_id = any(%s)
                        order by fd.file_id, ds.namespace, ds.name
                        """, (list(file_ids),)
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

    @staticmethod
    def file_count_by_dataset(db):
        c = db.cursor()
        c.execute(f"""
            select dataset_namespace, dataset_name, count(*) 
                from files_datasets 
                group by dataset_namespace, dataset_name
        """)
        return dict(((ds_ns, ds_name), n) for ds_ns, ds_name, n in fetch_generator(c))


class DBNamedQuery(DBObject):
    
    Columns = "namespace,name,parameters,source,creator,created_timestamp,description,metadata".split(",")
    Table = "queries"
    PK = ["namespace", "name"]

    def __init__(self, db, namespace, name, source, parameters, description, metadata):
        DBObject.__init__(self, db)
        assert namespace is not None and name is not None
        self.Namespace = namespace
        self.Name = name
        self.Source = source
        self.Parameters = parameters
        self.Creator = None
        self.CreatedTimestamp = None
        self.Description = description
        self.Metadata = metadata
        
    def to_jsonable(self):
        return dict(
            namespace = self.Namespace,
            name = self.Name,
            source = self.Source,
            creator = self.Creator,
            created_timestamp = epoch(self.CreatedTimestamp),
            description = self.Description,
            metadata = self.Metadata
        )

    @staticmethod
    def from_tuple(db, tup):
        namespace, name, parameters, source, creator, created_timespamp, description, metadata = tup
        #print("DBNamedQuery.from_tuple:", tup)
        query = DBNamedQuery(db, namespace, name, source, parameters, description, metadata)
        query.Creator = creator
        query.CreatedTimestamp = created_timespamp
        return query
        
    @transactioned
    def create(self, transaction=None):
        meta = json.dumps(self.Metadata or {})
        transaction.execute("""
            insert into queries(namespace, name, source, parameters, creator, description, metadata) 
                values(%s, %s, %s, %s, %s, %s, %s)
            returning created_timestamp""",
            (self.Namespace, self.Name, self.Source, self.Parameters, self.Creator, self.Description,
                meta)
        )
        self.CreatedTimestamp = transaction.fetchone()[0]
        return self
            
    @transactioned
    def save(self, transaction=None):
        meta = json.dumps(self.Metadata or {})
        transaction.execute("""
            update queries 
                set source=%s, parameters=%s, creator=%s, created_timestamp=%s,
                    description=%s, metadata=%s
                where namespace=%s and name=%s;
            """,
            (self.Source, self.Parameters, self.Creator, self.CreatedTimestamp, 
                self.Description, meta,
            self.Namespace, self.Name)
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

    @staticmethod
    def sql_for_bqq(bqq):
        namespace = bqq.Namespace
        name = bqq.Name
        regexp = bqq.RegExp
        where = bqq.Where
        a = alias("nq")
        meta_filter_dnf = DatasetMetaExpressionDNF(where) if where is not None else None
        where_sql = "" if meta_filter_dnf is None else " and " + meta_filter_dnf.sql(a)
        table = DBNamedQuery.Table
        columns = DBNamedQuery.columns(table_name=a)
        if regexp:
            sql = f"""select {columns}
                from {table} {a}
                where {a}.namespace='{namespace}' and {a}.name ~ '{name}'
                    {where_sql}
            """
        else:
            sql = f"""select {columns}
                from {table} {a}
                where {a}.namespace='{namespace}' and {a}.name like '{name}'
                    {where_sql}
            """
        return sql
    
    @staticmethod
    def queries_from_sql(db, sql):
        c = db.cursor()
        c.execute(sql)
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

    def namespaces(self):
        return DBNamespace.list(self.DB, owned_by_user=self)        

class DBNamespace(DBObject):

    Columns = "name,owner_user,owner_role,description,creator,created_timestamp,file_count".split(",")
    Table = "namespaces"
    PK = ["name"]

    def __init__(self, db, name, owner_user=None, owner_role=None, description=None, 
                creator=None, created_timestamp=None, file_count=0):
        DBObject.__init__(self, db)
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
        
    @transactioned
    def save(self, transaction=None):
        transaction.execute(f"""
            update {self.Table}
                set owner_user=%s, owner_role=%s, description=%s, file_count=%s
                where name=%s
            """,
            (self.OwnerUser, self.OwnerRole, self.Description, self.FileCount,
                self.Name)
        )
        return self

    @transactioned
    def create(self, transaction=None):
        transaction.execute("""
            insert into namespaces(name, owner_user, owner_role, description, creator) values(%s, %s, %s, %s, %s)
                returning created_timestamp
            """,
            (self.Name, self.OwnerUser, self.OwnerRole, self.Description, self.Creator))
        self.CreatedTimestamp = transaction.fetchone()[0]
        return self
        
    @staticmethod
    def get_many(db, names):
        #print("DBNamespace.get: name:", name)
        c = db.cursor()
        columns = DBNamespace.columns()
        c.execute(f"""select {columns}
                from {DBNamespace.Table} where name=any(%s)""", (list(names),))
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
