from .trees import Ascender, Node
from metacat.db import DBFileSet, alias
from .meta_evaluator import MetaEvaluator

class SQLConverter(Ascender):
    
    def __init__(self, db, filters, with_meta = True, limit=None, debug=False):
        self.DB = db
        self.WithMeta = with_meta
        self.Limit = limit
        self.Filters = filters
        self.Debug = debug
        
    def debug(self, *params, **args):
        parts = ["SQLConverter:"]+list(params)
        if self.Debug:
            print(*parts, **args)
            
    def convert(self, tree):
        result = self.walk(tree)
        if result.T == "sql":
            self.debug("sql:---------\n", result["sql"], "\n-------------")
        return self.node_to_file_set(result)
        
    def node_to_file_set(self, node):
        if node.T == "sql":
            file_set = DBFileSet.from_sql(self.DB, node["sql"])
        else:
            file_set = node["file_set"]
        return file_set
        
    def meta_filter(self, node, query=None, meta_exp=None):
        #print("meta_filter: args:", args)
        evaluator = MetaEvaluator()
        if meta_exp is not None:
            out = (f for f in self.node_to_file_set(query)
                    if evaluator(f.metadata(), meta_exp)
            )
            return Node("file_set", file_set = DBFileSet(self.DB, out))
        else:
            return query

    def basic_file_query(self, node, *args, query=None):
        #assert isinstance(query, BasicFileQuery)
        #print("_FileEvaluator:basic_file_query: query:", query.pretty())
        #print("FileEvaluator.basic_file_query: query.WithMeta:", query.WithMeta)
        #return DBFileSet.from_basic_query(self.DB, query, self.WithMeta or query.WithMeta, self.Limit)
        self.debug("basic_file_query: with_meta:", self.WithMeta)
        sql = DBFileSet.sql_for_basic_query(query, self.WithMeta, self.Limit)
        self.debug("basic_file_query: sql: --------\n", sql, "\n--------")
        return Node("sql", sql=sql)
        
    def file_list(self, node, specs=None):
        self.debug("file_list: specs:", specs)
        return Node("sql", sql=DBFileSet.sql_for_file_list(self.WithMeta, specs))
    
    def union(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set") for n in args)
        sqls = [n for n in args if n.T == "sql"]
        self.debug("sqls:")
        for sql in sqls:
            self.debug(sql.pretty())
        file_sets = [n for n in args if n.T == "file_set"]
        from_file_sets = DBFileSet.union(self.DB, [n["file_set"] for n in file_sets]) if file_sets else None
        u_parts = ["\n(\n%s\n)" % (n["sql"],) for n in sqls]
        u_sql = None if not sqls else "\nunion\n".join(u_parts)
        
        if not file_sets:
            return Node("sql", sql=u_sql)        
        elif not u_sql:
            return Node("file_set", file_set=from_file_sets)
        else:
            from_sql = DBFileSet.from_sql(self.DB, u_sql)
            return DBFileSet.union(self.DB, [from_sql, from_file_sets])


    def join(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set") for n in args)
        sqls = [n for n in args if n.T == "sql"]
        file_sets = [n for n in args if n.T == "file_set"]
        from_file_sets = DBFileSet.union(self.DB, [n["file_set"] for n in file_sets]) if file_sets else None
        u_parts = ["\n(\n%s\n)" % (n["sql"],) for n in sqls]
        u_sql = None if not sqls else "\nintersect\n".join(u_parts)
        
        if not file_sets:
            return Node("sql", sql=u_sql)        
        elif not u_sql:
            return Node("file_set", file_set=from_file_sets)
        else:
            from_sql = DBFileSet.from_sql(self.DB, u_sql)
            Node("file_set", file_set = DBFileSet.join(self.DB, [from_sql, from_file_sets]))

    def minus(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert len(args) == 2
        assert all(n.T in ("sql","file_set") for n in args)
        left, right = args
        if left.T == "sql" and right.T == "sql":
            s1 = left["sql"]
            s2 = right["sql"]
            sql = f"""({s1})\nexcept\n({s2})"""
            return Node("sql", sql=sql)
        else:
            left_set = left["file_set"] if left.T == "file_set" else DBFileSet.from_sql(self.DB, left["sql"])
            right_set = right["file_set"] if right.T == "file_set" else DBFileSet.from_sql(self.DB, right["sql"])
            Node("file_set", file_set = left_set - right_set)

    def parents_of(self, node, *args):
        assert len(args) == 1
        arg = args[0]
        assert arg.T in ("sql","file_set")
        if arg.T == "sql":
            arg_sql = arg["sql"]
            p = alias("p")
            c = alias("c")
            pc = alias("pc")
            meta = f"{p}.metadata" if self.WithMeta else "null"
            new_sql = f"""
                select {p}.id, {p}.namespace, {p}.name, {meta} as metadata
                from files {p}
                    inner join parent_child {pc} on {p}.id = {pc}.parent_id
                    inner join ({arg_sql}) as {c} on {c}.id = {pc}.child_id
            """
            return Node("sql", sql=new_sql)
        else:
            return Node("file_set", file_set = arg["file_set"].parents(with_metadata=self.WithMeta))

    def children_of(self, node, *args):
        assert len(args) == 1
        arg = args[0]
        assert arg.T in ("sql","file_set")
        if arg.T == "sql":
            arg_sql = arg["sql"]
            p = alias("p")
            c = alias("c")
            pc = alias("pc")
            meta = f"{c}.metadata" if self.WithMeta else "null"
            new_sql = f"""
                select {c}.id, {c}.namespace, {c}.name, {meta} as metadata
                from files {c}
                    inner join parent_child {pc} on {c}.id = {pc}.child_id
                    inner join ({arg_sql}) as {p} on {p}.id = {pc}.parent_id
            """
            return Node("sql", sql=new_sql)
        else:
            return Node("file_set", file_set = arg["file_set"].children(with_metadata=self.WithMeta))

    def limit(self, node, arg, limit=None):
        if limit is None:
            return arg
        if arg.T == "sql":
            sql = arg["sql"]
            tmp = alias()
            new_sql = f"""
                select id, namespace, name, metadata from
                (
                    {sql}
                ) as {tmp} limit {limit}
            """
            return Node("sql", sql=new_sql)
        else:
            return Node("file_set", file_set = limited(arg["file_set"]))

    def filter(self, node, *queries, name=None, params=[]):
        #print("Evaluator.filter: inputs:", inputs)
        assert name is not None
        filter_function = self.Filters[name]
        queries = [self.node_to_file_set(q) for q in queries]
        return Node("file_set", file_set = DBFileSet(self.DB, filter_function(queries, params)))
