from .trees import Ascender, Node
from metacat.db import DBFileSet, alias, MetaExpressionDNF, DBDataset
from .meta_evaluator import MetaEvaluator
from metacat.util import limited
from textwrap import dedent, indent

class SQLConverter(Ascender):
    
    def __init__(self, db, debug=False):
        self.DB = db
        self.Debug = False
        
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
            
    def __call__(self, tree):
        self.debug("\nSQL converter: input tree:----------\n", tree.pretty(), "\n-------------")
        result = self.walk(tree)
        self.debug("\nSQL converter: output tree:----------\n", result.pretty(), "\n-------------")
        return result
        
    #
    # Tree node methods
    #
    def query(self, node, *args, namespace=None, name=None):
        raise RuntimeError(f"Named query {namespace}:{name} not assembled")
        
    def meta_filter(self, node, query=None, meta_exp=None, with_meta=False, with_provenance=False):
        if meta_exp is None:
            return query
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
            return node

    def basic_file_query(self, node, *args, query=None):
        sql = DBFileSet.sql_for_basic_query(self.DB, query)
        if sql:
            return Node("sql", sql=sql)
        else:
            return Node("empty")            # empty file set
        
    def file_list(self, node, specs=None, spec_type=None, with_meta=False, with_provenance=False, limit=None, skip=0):
        return Node("sql", sql=DBFileSet.sql_for_file_list(spec_type, specs, with_meta, with_provenance, limit, skip))

    def union(self, node, *args):
        #print("Evaluator.union: args:", args)

        assert all(n.T in ("sql","file_set","empty") for n in args)

        args = [a for a in args if a.T != "empty"]
        if not args:
            return Node("empty")

        sqls = [n for n in args if n.T == "sql"]
        if len(sqls) < 2:
            return Node("union", args)

        file_sets = [n for n in args if n.T == "file_set"]
        u_parts = ["\n(\n%s\n)" % (n["sql"],) for n in sqls]
        u_sql = None if not sqls else "\nunion\n".join(u_parts)

        if not file_sets:
            return Node("sql", sql=u_sql)
        
        return Node("union", [Node("sql", sql=u_sql)] + file_sets)

    def join(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set","empty") for n in args)
        if any(n.T == "empty" for n in args):
            return Node("empty")

        sqls = [n for n in args if n.T == "sql"]
        if len(sqls) < 2:
            return node
            
        file_sets = [n for n in args if n.T == "file_set"]
        u_parts = ["\n(\n%s\n)" % (n["sql"],) for n in sqls]
        u_sql = None if not sqls else "\nintersect\n".join(u_parts)
        
        if not file_sets:
            return Node("sql", sql=u_sql)
        
        return Node("join", [Node("sql", sql=u_sql)] + file_sets)

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
            return node

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
                new_sql = dedent(f"""\
                    -- parents of {p}
                        select {columns}
                        from files_with_provenance {p}
                            inner join parent_child {pc} on {p}.id = {pc}.parent_id
                            inner join ({arg_sql}) as {c} on {c}.id = {pc}.child_id
                    -- end of parents of {p}
                """)
            else:
                new_sql = dedent(f"""\
                    -- parents of {p}
                        select {columns}
                        from files {p}
                            inner join parent_child {pc} on {p}.id = {pc}.parent_id
                            inner join ({arg_sql}) as {c} on {c}.id = {pc}.child_id
                    -- end of parents of {p}
                """)
            return Node("sql", sql=new_sql)
        else:
            return node

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
                new_sql = dedent(f"""\
                    -- children of {c}
                        select {columns}
                        from files_with_provenance {c}
                            inner join parent_child {pc} on {c}.id = {pc}.child_id
                            inner join ({arg_sql}) as {p} on {p}.id = {pc}.parent_id
                    -- end of children of {c}
                """)
            else:
                new_sql = dedent(f"""
                    -- children of {c}
                        select {columns}
                        from files {c}
                            inner join parent_child {pc} on {c}.id = {pc}.child_id
                            inner join ({arg_sql}) as {p} on {p}.id = {pc}.parent_id
                    -- end of children of {c}
                """)

            return Node("sql", sql=new_sql)
        else:
            return node
            
    def skip(self, node, child, skip=0):
        if child.T == "skip_limit":
            new_skip = child["skip"] + skip
            new_limit = child["limit"] - skip
            if new_limit <= 0:
                return Node("empty")
            else:
                return Node("skip_limit", child.C, skip=new_skip, limit=new_limit)
        else:
            return Node("skip_limit", [child], skip=skip, limit=None)

    def limit(self, node, child, limit=None):
        if child.T == "skip_limit":
            new_skip = child["skip"]
            new_limit = child["limit"]
            if limit is not None:
                if new_limit is None:
                    new_limit = limit
                else:
                    new_limit = min(limit, new_limit)
            if new_limit <= 0:
                return Node("empty")
            else:
                return Node("skip_limit", child.C, skip=new_skip, limit=new_limit)
        else:
            return Node("skip_limit", [child], skip=0, limit=limit)

    def skip_limit(self, node, arg, skip=0, limit=None, **kv):
        if limit is None and skip == 0:
            return arg
        if arg.T == "empty":
            return arg
        if arg.T == "skip_limit":
            new_skip = arg["skip"]
            new_limit = arg["limit"]
            if skip:
                new_skip += skip
                if new_limit is not None:
                    new_limit -= skip
            if limit is not None:
                if new_limit is None:
                    new_limit = limit
                else:
                    new_limit = min(limit, new_limit)
            if new_limit is not None and new_limit <= 0:
                return Node("empty")
            return Node("skip_limit", arg.C, skip=new_skip, limit=new_limit)
        elif arg.T == "sql":
            sql = arg["sql"]
            tmp = alias()
            columns = self.columns(tmp)
            
            limit_clouse = "" if limit is None else f"limit {limit}"
            offset_clouse = "" if skip == 0 else f"offset {skip}"
            
            sql = indent("\n" + sql + "\n", "    ")
            new_sql = dedent(f"""\
                -- skip {skip} limit {limit} {tmp}
                    select {columns} 
                    from ({sql}) {tmp} {limit_clouse} {offset_clouse}
                -- end of limit {limit} {tmp}
            """)
            return Node("sql", sql=new_sql)
        else:
            return node