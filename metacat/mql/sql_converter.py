from .trees import Ascender, Node
from metacat.db import DBFileSet, alias, limited, MetaExpressionDNF, DBDataset
from .meta_evaluator import MetaEvaluator

class CollapseSkipLimit(Ascender):
    #
    # converts Nodes of type "skip" and "limit" and sequences limit(skip) into "skip_limit" nodes
    # also detects empty sets
    #
    
    def combine_limits(self, l1, l2):
        if l1 is None:  return l2
        if l2 is None:  return l1
        return min(l1, l2)
    
    def skip(self, node, child, skip=0):
        if child.T == "skip_limit":
            child_skip = child.get("skip", 0)
            child_limit = child.get("limit")
            if child_limit:
                child_limit -= skip
                if child_limit <= 0:
                    return Node("empty")
            child_skip += skip
            return Node("skip_limit", [child], skip=child_skip, limit=child_limit)
        else:
            return Node("skip_limit", [child], skip=skip, limit=None)

    def limit(self, node, child, limit=None):
        if limit == 0:
            return Node("empty")
        if child.T == "skip_limit":
            return Node("skip_limit", [child.C[0]], skip=child["skip"], limit=self.combine_limits(limit, child["limit"]))
        else:
            return Node("skip_limit", [child], skip=0, limit=limit)

class SQLConverter(Ascender):
    
    def __init__(self, db, filters, debug=False):
        self.DB = db
        self.Filters = filters
        self.Debug = debug
        
    def columns(self, t, with_meta=True, with_provenance=True):
        meta = f"{t}.metadata" if with_meta else "null as metadata"
        if with_provenance:
            parents = f"{t}.parents"
            children = f"{t}.children"
        else:
            parents = "null as parents"
            children = "null as children"
        return f"{t}.id, {t}.namespace, {t}.name, {meta}, {t}.creator, {t}.created_timestamp, {t}.size, {t}.checksums, {parents}, {children}"
        
    def debug(self, *params, **args):
        if self.Debug:
            parts = ["SQLConverter:"]+list(params)
            print(*parts, **args)
            
    def convert(self, tree):
        self.debug("SQL converter: input tree:----------\n", tree.pretty(), "\n-------------")
        tree = CollapseSkipLimit().walk(tree, self.Debug)
        self.debug("SQL converter: after CollapseSkipLimit:----------\n", tree.pretty(), "\n-------------")        
        result = self.walk(tree)
        if result.T == "sql":
            self.debug("SQL converter: sql:---------\n", result["sql"], "\n-------------")
        file_set = self.node_to_file_set(result)
        self.debug("convert(): returning file set")
        return file_set
        
    def node_to_file_set(self, node):
        if node.T == "sql":
            file_set = DBFileSet.from_sql(self.DB, node["sql"])
        elif node.T == "empty":
            file_set = DBFileSet(self.DB)
        else:
            #print(node)
            file_set = node["file_set"]
        return file_set

    #
    # Tree node methods
    #
    
    def empty(self, node, *args):
        return Node("file_set", file_set=DBFileSet(self.DB))    # empty file set
        
    def meta_filter(self, node, query=None, meta_exp=None, with_meta=False, with_provenance=False):
        #print("meta_filter: args:", args)
        assert query.T in ("sql","file_set")        
        if meta_exp is not None:
            if query.T == "sql":
                t = alias("t")
                dnf = MetaExpressionDNF(meta_exp)
                where_sql = dnf.sql(t)
                if not where_sql:
                    return node
                columns = self.columns(t, with_meta, with_provenance)
                query_sql = query["sql"]
                sql = f"""
                    -- meta_filter {t}
                        select {columns} 
                        from (
                            {query_sql}
                        ) {t} where {where_sql} 
                    -- end of meta_filter {t}
                """
                return Node("sql", sql=sql)
            else:
                evaluator = MetaEvaluator()
                out = (f for f in self.node_to_file_set(query)
                        if evaluator(f.metadata(), meta_exp)
                )
                return Node("file_set", file_set = DBFileSet(self.DB, out))
        else:
            return query

    def basic_file_query(self, node, *args, query=None):
        sql = DBFileSet.sql_for_basic_query(self.DB, query)
        self.debug("basic_file_query: sql: --------\n", sql, "\n--------")
        return Node("sql", sql=sql)
        
    def file_list(self, node, specs=None, with_meta=False, with_provenance=False):
        return Node("sql", sql=DBFileSet.sql_for_file_list(specs, with_meta, with_provenance))
    
    def union(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set","empty") for n in args)
        args = [a for a in args if a.T != "empty"]
        if not args:
            return Node("empty")
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


    def join(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set","empty") for n in args)
        if any(n.T == "empty" for n in args):
            return Node("empty")
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

    def minus(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        assert len(args) == 2
        assert all(n.T in ("sql", "file_set", "empty") for n in args)
        left, right = args
        if left.T == "empty":
            return Node("empty")
        elif right.T == "empty":
            return left
        if left.T == "sql" and right.T == "sql":
            s1 = left["sql"]
            s2 = right["sql"]
            sql = f"""({s1})\nexcept\n({s2})"""
            if self.Debug:
                print("SQLConverter.minus: sql:---------\n", sql, "\n-----------")
            return Node("sql", sql=sql)
        else:
            #print("minus: \n   left:", left.pretty(), "\n   right:", right.pretty())
            left_set = left["file_set"] if left.T == "file_set" else DBFileSet.from_sql(self.DB, left["sql"])
            right_set = right["file_set"] if right.T == "file_set" else DBFileSet.from_sql(self.DB, right["sql"])
            #print("minus: left_set, right_set:", left_set, right_set)
            
            return Node("file_set", file_set = left_set - right_set)

    def parents_of(self, node, *args, with_meta=False, with_provenance=False):
        assert len(args) == 1
        arg = args[0]
        if arg.T == "empty":    return arg
        assert arg.T in ("sql","file_set")
        with_meta = node["with_meta"]
        with_provenance = node["with_provenance"]
        if arg.T == "sql":
            arg_sql = arg["sql"]
            p = alias("p")
            c = alias("c")
            pc = alias("pc")
            columns = self.columns(p, with_meta, with_provenance)
            if with_provenance:
                new_sql = f"""
                    -- parents of {p}
                        select {columns}
                        from files_with_provenance {p}
                            inner join parent_child {pc} on {p}.id = {pc}.parent_id
                            inner join ({arg_sql}) as {c} on {c}.id = {pc}.child_id
                    -- end of parents of {p}
                """
            else:
                new_sql = f"""
                    -- parents of {p}
                        select {columns}
                        from files {p}
                            inner join parent_child {pc} on {p}.id = {pc}.parent_id
                            inner join ({arg_sql}) as {c} on {c}.id = {pc}.child_id
                    -- end of parents of {p}
                """
            return Node("sql", sql=new_sql)
        else:
            return Node("file_set", file_set = arg["file_set"].parents(with_metadata=with_meta, with_provenance=with_provenance))

    def children_of(self, node, *args, with_meta=False, with_provenance=False):
        assert len(args) == 1
        arg = args[0]
        if arg.T == "empty":    return arg
        assert arg.T in ("sql","file_set")
        if arg.T == "sql":
            arg_sql = arg["sql"]
            p = alias("p")
            c = alias("c")
            pc = alias("pc")
            columns = self.columns(c, with_meta, with_provenance)
            if with_provenance:
                new_sql = f"""
                    -- children of {c}
                        select {columns}
                        from files_with_provenance {c}
                            inner join parent_child {pc} on {c}.id = {pc}.child_id
                            inner join ({arg_sql}) as {p} on {p}.id = {pc}.parent_id
                    -- end of children of {c}
                """
            else:
                new_sql = f"""
                    -- children of {c}
                        select {columns}
                        from files {c}
                            inner join parent_child {pc} on {c}.id = {pc}.child_id
                            inner join ({arg_sql}) as {p} on {p}.id = {pc}.parent_id
                    -- end of children of {c}
                """

            return Node("sql", sql=new_sql)
        else:
            return Node("file_set", file_set = arg["file_set"].children(with_metadata=with_meta, with_provenance=with_provenance))

    def skip_limit(self, node, arg, skip=0, limit=None, **kv):
        if limit is None and skip == 0:
            return arg
        if arg.T == "empty":
            return arg
        assert arg.T in ("file_set", "sql")
        if arg.T == "sql":
            sql = arg["sql"]
            tmp = alias()
            columns = self.columns(tmp)
            
            limit_clouse = "" if limit is None else f"limit {limit}"
            offset_clouse = "" if skip == 0 else f"offset {skip}"
            
            new_sql = f"""
                -- limit {limit} {tmp}
                    select {columns} 
                    from (
                        {sql}
                    ) {tmp} {limit_clouse} {offset_clouse}
                -- end of limit {limit} {tmp}
            """
            return Node("sql", sql=new_sql)
        else:
            return Node("file_set", file_set = arg["file_set"].skip(skip).limit(limit))
            
    def filter(self, node, *queries, name=None, params=[], **kv):
        #print("Evaluator.filter: inputs:", inputs)
        assert name is not None
        filter_object = self.Filters[name]
        queries = [self.node_to_file_set(q) for q in queries]
        limit = node.get("limit")
        skip = node.get("skip", 0)
        kv = node.get("kv", {})
        node = Node("file_set", file_set = DBFileSet(self.DB, filter_object.run(queries, params, kv, limit=limit, skip=skip)))
        #print("filter: returning:", node.pretty())
        return node

    #def _default(self, node, *children, **named):
    #    raise ValueError("SQL converter found a node of unknown type: %s" % (node,))
