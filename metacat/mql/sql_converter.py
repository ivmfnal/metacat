from metacat.common.trees import Ascender, Node
from metacat.db import DBFileSet, alias, DBDataset, DBFile
from metacat.common import FileMetaExpressionDNF
from .meta_evaluator import MetaEvaluator
from metacat.util import limited, insert_sql
from textwrap import dedent, indent

class SQLConverter(Ascender):
    
    def __init__(self, db, debug=False, include_retired=False, summary=None):
        self.DB = db
        self.Debug = debug
        self.IncludeRetired = include_retired
        self.Summary = summary

    def columns(self, t, with_meta=True, with_provenance=True):
        meta = f"{t}.metadata" if with_meta else "null as metadata"
        if with_provenance:
            parents = f"{t}.parents"
            children = f"{t}.children"
        else:
            parents = "null as parents"
            children = "null as children"

        attrs = DBFile.attr_columns(alias=t)

        return f"{t}.id, {t}.namespace, {t}.name, {meta}, {attrs}, {parents}, {children}"

    def debug(self, *params, **args):
        if self.Debug:
            parts = ["SQLConverter:"]+list(params)
            print(*parts, **args)

    def __call__(self, tree):
        #print("hello")
        self.debug("\nSQL converter: input tree:----------\n", tree.pretty(), "\n-------------")
        result = self.walk(tree)
        if self.Summary:
            result = Node("summary", [result], mode=mode)
        #print("debug:", self.Debug)
        self.debug("\nSQL converter: output tree:----------\n", result.pretty(), "\n-------------")
        return result

    #
    # Tree node methods
    #
    def query(self, node, *args, namespace=None, name=None):
        raise RuntimeError(f"Named query {namespace}:{name} not assembled")

    def meta_filter(self, node, query=None, meta_exp=None, with_meta=False, with_provenance=False):
        #print("meta_filter: query=", query.pretty())
        #print("meta_filter: node[query]=", node["query"].pretty())
        if meta_exp is None:
            return query
        if query.T == "sql":
            t = alias("t")
            dnf = FileMetaExpressionDNF(meta_exp)
            where_sql = dnf.sql(t)
            if not where_sql:
                return node
            columns = self.columns(t, with_meta, with_provenance)
            query_sql = query["sql"]
            sql = insert_sql(f"""
                -- meta_filter {t}
                    select {columns} 
                    from (
                        $query_sql
                    ) {t} where {where_sql}
                -- end of meta_filter {t}
            """, query_sql=query_sql)
            return Node("sql", sql=sql)
        else:
            return node

    def ordered(self, node, child):
        if child.T == "sql":
            t = alias("t")
            child_sql = child["sql"]
            sql = insert_sql(f"""\
                -- ordered {t}
                    select {t}.*
                    from (
                        $child_sql
                    ) {t} order by {t}.id
                -- end of ordered {t}
            """, child_sql = child_sql)
            return Node("sql", sql=sql)
        elif child.T == "filter":
            return child.clone(ordered=True)
        else:
            return node

    def basic_file_query(self, node, *args, query=None):
        sql = DBFileSet.sql_for_basic_query(self.DB, query, self.IncludeRetired)
        if sql:
            out = Node("sql", sql=sql)
        else:
            out = Node("empty")            # empty file set
        return out

    def file_list(self, node, specs=None, spec_type=None, with_meta=False, with_provenance=False, limit=None, skip=0):
        return Node("sql", sql=DBFileSet.sql_for_file_list(spec_type, specs, with_meta, with_provenance, limit, skip))

    def union(self, node, *args):
        #print("Evaluator.union: args:", args)
        args = [a for a in args if a.T != "empty"]
        if not args:
            return Node("empty")
        sqls = [n for n in args if n.T == "sql"]
        if not sqls:
            return Node("union", args)
        if len(sqls) >= 2:
            inner_sqls = {f"__{i}": n["sql"] for i, n in enumerate(sqls)}
            parts = [f"(\n    $__{i}\n)" for i, n in enumerate(sqls)]
            template = "\nunion\n".join(parts)
            combined_sql = insert_sql(template, **inner_sqls)
            sqls = [Node("sql", sql=combined_sql)]
        others = [n for n in args if n.T != "sql"]
        if not others:
            return sqls[0]
        return Node("union", sqls + others)

    def join(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        if any(n.T == "empty" for n in args):
            return Node("empty")
        sqls = [n for n in args if n.T == "sql"]
        if not sqls:
            return node
        if len(sqls) >= 2:
            inner_sqls = {f"__{i}": n["sql"] for i, n in enumerate(sqls)}
            parts = [f"(\n    $__{i}\n)" for i, n in enumerate(sqls)]
            template = "\nintersect\n".join(parts)
            combined_sql = insert_sql(template, **inner_sqls)
            sqls = [Node("sql", sql=combined_sql)]
        others = [n for n in args if n.T != "sql"]
        if not others:
            return sqls[0]
        return Node("join", sqls + others)

    def minus(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        assert len(args) == 2
        left, right = args
        if left.T == "empty":
            return Node("empty")
        elif right.T == "empty":
            return left
        if left.T == "sql" and right.T == "sql":
            s1 = left["sql"]
            s2 = right["sql"]
            sql = insert_sql("""\
                (
                    $s1
                )
                except
                (
                    $s2
                )""", s1=s1, s2=s2)
            self.debug("SQLConverter.minus: sql:---------\n", sql, "\n-----------")
            return Node("sql", sql=sql)
        else:
            return node

    def parents_of(self, node, *args, with_meta=False, with_provenance=False, ordered=False):
        assert len(args) == 1
        arg = args[0]
        if arg.T == "empty":    return arg
        if arg.T == "sql":
            with_meta = node["with_meta"]
            with_provenance = node["with_provenance"]
            arg_sql = arg["sql"]
            p = alias("p")
            c = alias("c")
            pc = alias("pc")
            columns = self.columns(p, with_meta, with_provenance)
            order = f"order by {p}.id" if ordered else ""
            table = "files_with_provenance" if with_provenance else "files"
            new_sql = insert_sql(f"""\
                --  parents of {p}
                    select {columns}
                    from {table} {p}
                        inner join parent_child {pc} on {p}.id = {pc}.parent_id
                        inner join (
                            $arg_sql
                        ) as {c} on {c}.id = {pc}.child_id
                    {order}
                --  end of parents of {p}
            """, arg_sql=arg_sql)
            return Node("sql", sql=new_sql)
        else:
            return node

    def children_of(self, node, *args, with_meta=False, with_provenance=False, ordered=False):
        assert len(args) == 1
        arg = args[0]
        if arg.T == "empty":    return arg
        if arg.T == "sql":
            with_meta = node["with_meta"]
            with_provenance = node["with_provenance"]
            arg_sql = arg["sql"]
            p = alias("p")
            c = alias("c")
            pc = alias("pc")
            columns = self.columns(c, with_meta, with_provenance)
            order = f"order by {p}.id" if ordered else ""
            table = "files_with_provenance" if with_provenance else "files"
            new_sql = insert_sql(f"""\
                -- children of {c}
                    select {columns}
                    from {table} {c}
                        inner join parent_child {pc} on {c}.id = {pc}.child_id
                        inner join (
                            $arg_sql
                        ) as {p} on {p}.id = {pc}.parent_id
                    {order}
                -- end of children of {c}
            """, arg_sql=arg_sql)
            return Node("sql", sql=new_sql)
        else:
            return node
            
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
            new_sql = insert_sql(f"""\
                -- skip {skip} limit {limit} {tmp}
                    select {columns} 
                    from (
                        $sql
                    ) {tmp} 
                    {limit_clouse} {offset_clouse}
                -- end of limit {limit} {tmp}
            """, sql=sql)
            return Node("sql", sql=new_sql)
        else:
            return node

    def _default(self, node, *children, **named):
        #print("_default:", node.pretty())
        #print("      children:")
        #for c in children:
        #    print("+ ", c.pretty())
        out = Node(node.T, children, _meta=node.M, _data=named)
        #print("_default returning:", out.pretty())
        return out
