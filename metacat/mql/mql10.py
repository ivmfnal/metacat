import json, time, pprint, traceback
from metacat.db import DBDataset, DBFile, DBNamedQuery, DBFileSet
from metacat.util import limited, unique
from metacat.common.trees import Node, Ascender, Descender, Converter
from metacat.common import FileMetaExpressionDNF
from .sql_converter import SQLConverter
from .query_executor import FileQueryExecutor
from .meta_evaluator import MetaEvaluator
from datetime import date, datetime, timezone

from lark import Lark, LarkError
from lark import Tree, Token

CMP_OPS = [">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*"]

from .grammar import MQL_Grammar
_Parser = Lark(MQL_Grammar, start="query")

class MQLError(Exception):

    def __init__(self, message):
        self.Message = message

class MQLSyntaxError(MQLError):
    
    def __str__(self):
        return f"MQL syntax Error: {self.Message}"
        
class MQLCompilationError(MQLError):
    
    def __str__(self):
        return f"MQL compilation Error: {self.Message}"
        
class MQLExecutionError(MQLError):
    
    def __str__(self):
        return f"MQL execution Error: {self.Message}"
        
def _merge_skip_limit(existing_skip, existing_limit, skip=0, limit=None):
    if existing_limit is None:
        return existing_skip+skip, limit
    elif limit is None:
        return existing_skip + skip, max(0, existing_limit - skip)
    else:
        return existing_skip + skip, max(0, min(existing_limit - skip, limit))
        
class _QueryQueryCompiler(Ascender):

    def basic_query_query(self, node, *args, query=None):
        return Node("sql", sql=DBNamedQuery.sql_for_bqq(query))

class _QueryQueryExecutor(Ascender):
    
    def __init__(self, db):
        Ascender.__init__(self)
        self.DB = db

    def sql(self, node, sql=None):
        return DBNamedQuery.queries_from_sql(self.DB, sql)

class QueryQuery(object):
    
    Type = "query"
    
    def __init__(self, tree):
        self.Tree = tree
        self.Compiled = None

    def compile(self, with_meta=False, with_provenance=False):
        self.Compiled = self.Compiled or _QueryQueryCompiler()(self.Tree)
        return self.Compiled

    def run(self, db, debug=False, **ignore):
        compiled = self.compile()
        return _QueryQueryExecutor(db)(compiled)

class _DatasetQueryCompiler(Ascender):

    def dataset_query_list(self, node, *args):
        queries = [a["query"] for a in args]
        sql = DBDataset.sql_for_bdqs(queries)
        return Node("sql", sql=sql)

class _DatasetQueryExecutor(Ascender):
    
    def __init__(self, db):
        Ascender.__init__(self)
        self.DB = db

    def sql(self, node, sql=None):
        return DBDataset.datasets_from_sql(self.DB, sql)

class DatasetQuery(object):
    
    Type = "dataset"
    
    def __init__(self, tree):
        self.Tree = tree
        self.Compiled = None

    def compile(self, with_meta=False, with_provenance=False):
        self.Compiled = self.Compiled or _DatasetQueryCompiler()(self.Tree)
        return self.Compiled

    def run(self, db, debug=False, **ignore):
        compiled = self.compile()
        return _DatasetQueryExecutor(db)(compiled)


class FileQuery(object):

    Type = "file"

    def __init__(self, tree, include_retired=False):
        self.Tree = tree
        self.Assembled = self.Optimized = self.Compiled = None
        self.IncludeRetired = include_retired

    def __str__(self):
        return "FileQuery(\n%s\n)" % (self.Tree.pretty("  "),)

    def skip_assembly(self):
        if self.Assembled is None:
            self.Assembled = self.Tree
        return self.Assembled

    def optimize(self, debug=False, skip=0, limit=None):
        if self.Optimized is None:
            #print("Query.optimize: assembled:----\n", self.Assembled.pretty())

            optimized = self.Tree
            if debug:
                print("Query.optimize: initial:----")
                print(optimized.pretty("    "))

            #optimized = _SkipLimitApplier().walk(optimized)
            #if debug:
            #    print("Query.optimize: after 1st _SkipLimitApplier:----")
            #    print(optimized.pretty("    "))
                
            #print("starting _MetaExpPusher...")
            optimized = _MetaExpPusher().walk(optimized, None)
            if debug:
                print("Query.optimize: after _MetaExpPusher:----")
                print(optimized.pretty("    "))

            optimized = _RemoveEmpty().walk(optimized, debug)
            if debug:
                print("Query.optimize: after _RemoveEmpty:----")
                print(optimized.pretty("    "))
            
            #print("Query.optimize(): calling second _SkipLimitApplier: skip=", skip, "   limit=", limit)
            optimized = _SkipLimitApplier().walk(optimized, skip, limit)
            if debug:
                print("Query.optimize: after 2nd _SkipLimitApplier:----")
                print(optimized.pretty("    "))
            
            #optimized = _OrderedApplier()(optimized)
            #if debug:
            #    print("Query.optimize: after applying ordering:----")
            #    print(optimized.pretty("    "))
            
            self.Optimized = optimized
        return self.Optimized

    def compile(self, db=None, skip=0, limit=None, with_meta=False, with_provenance=False, debug=False):
        try:
            optimized = self.optimize(debug=debug, skip=skip, limit=limit)
            optimized = _QueryOptionsApplier().walk(optimized, 
                dict(
                    with_provenance = with_provenance,
                    with_meta = with_meta
                ))
            if debug:
                print("after _QueryOptionsApplier:", optimized.pretty())
            self.Compiled = compiled = SQLConverter(db, debug=debug, include_retired=self.IncludeRetired)(optimized)
        except Exception as e:
            raise MQLCompilationError(traceback.format_exc(limit=-1))

        if debug:
            print("\nCompiled:", compiled.pretty())

        return compiled

    def run(self, db=None, filters={}, skip=0, limit=None, with_meta=True, with_provenance=True, debug=False):

        compiled = self.compile(db=db, 
                    skip=skip, limit=limit, 
                    with_meta=with_meta, with_provenance=with_provenance,
                    debug=debug)
        try:
            result = FileQueryExecutor(db, filters, debug=debug)(compiled)
        except Exception as e:
            raise MQLExecutionError(str(e))
        assert isinstance(result, DBFileSet)
        return result

class _OrderedApplier(Descender):
    
    def walk(self, tree, ordered=False):
        return Descender.walk(self, tree, ordered)
        
    __call__ = walk

    def ordered(self, node, ordered):
        children = [self.walk(c) for c in node.C]
        return node.clone(children=children)
        
    def basic_file_query(self, node, ordered):
        node["query"].Ordered = node["query"].Ordered or ordered
        return node
        
    def parents_of(self, node, ordered):
        return node.clone(ordered=ordered or node.get("ordered"))

    def children_of(self, node, ordered):
        return node.clone(ordered=ordered or node.get("ordered"))

    basic_dataset_query = basic_file_query

    def skip_limit(self, node, ordered):
        if ordered and node["skip"] == 0:
            child = node.C[0]
            
        child = self.walk(node.C[0], False)
        return node.clone(children=[child])

    def filter(self, node, ordered):
        children = [self.walk(c) for c in node.C]
        node = node.clone(children = children, ordered = node.get("ordered", False) or ordered)
        return node

    def file_list(self, node, ordered):
        return node     # already fixed order

    def _default(self, node, ordered):
        children = [self.walk(c) for c in node.C]
        node = node.clone(children=children)
        if ordered:
            return Node("ordered", [node])
        else:
            return node

class _SkipLimitApplier(Descender):
    #
    # Optimize skip/limit: combine them if in the right order, push them into BFQs when possible
    # skip_limit node is applied by applying "skip" first and then "limit", consistently with Postgres SQL
    #
    
    #def skip(self, node, skip_limit):
    #    skip, limit = skip_limit
    #    node_skip = node["skip"]
    #    return self.walk(node.C[0], (skip+node_skip, limit))
    
    def walk(self, tree, skip=0, limit=None):
        return Descender.walk(self, tree, (skip, limit))
    
    def meta_filter(self, node, skip_limit):
        node = Node("meta_filter", 
            query = self.walk(node["query"]),     # apply skips/limits inside the query
            meta_exp = node["meta_exp"]
        )
        return self._default(node, skip_limit)
    
    def skip_limit(self, node, skip_limit):
        skip, limit = skip_limit
        node_skip = node.get("skip", 0)
        node_limit = node.get("limit")
        #print("_SkipLimitApplier.skip_limit(): node:", node_skip, node_limit, "   context:", skip, limit)
        skip, limit = _merge_skip_limit(node_skip, node_limit, skip, limit)
        #print("           merged:", skip, limit)
        if limit is not None and limit <= 0:
            return Node("empty")
        else:
            applied = self.walk(node.C[0], skip, limit)
            #print("        applied:")
            #print(applied.pretty(indent="        "))
            return applied

    def basic_file_query(self, node, skip_limit):
        query = node["query"]
        #print("_SkipLimitApplier: applying skip_limit", skip_limit, " to BFQ:", query)
        skip, limit = skip_limit
        query.add_skip_limit(skip, limit)
        if query.Limit is not None and query.Limit <= 0:
            return Node("empty")
        else:
            return node

    def union(self, node, skip_limit):
        #print("_SkipLimitApplier: union: skip_limit:", skip_limit, "  children:", node.C)
        skip, limit = skip_limit
        if limit is not None and limit <= 0:
            node = Node("empty")
        else:
            node = Node("union", [self.walk(c) for c in node.C])
            if skip or limit:
                node = Node("skip_limit", [node], skip=skip, limit=limit)
        return node

    def join(self, node, skip_limit):
        #print("_SkipLimitApplier: skip_limit:", skip_limit, "  children:", node.C)
        skip, limit = skip_limit
        if limit is not None and limit <= 0:
            node = Node("empty")
        else:
            node = Node("join", [self.walk(c) for c in node.C])
            if skip or limit:
                node = Node("skip_limit", [node], skip=skip, limit=limit)
        return node

    def filter(self, node, skip_limit):
        skip, limit = skip_limit
        node_skip = node["skip"]
        node_limit = node["limit"]
        skip, limit = _merge_skip_limit(node_skip, node_limit, skip, limit)
        node["limit"] = limit
        node["skip"] = skip
        node.C = [self.walk(c) for c in node.C]
        if limit is not None and limit <= 0:
            return Node("empty")
        else:
            return node

    def file_list(self, node, skip_limit):
        skip, limit = skip_limit
        node_skip = node["skip"]
        node_limit = node["limit"]
        skip, limit = _merge_skip_limit(node_skip, node_limit, skip, limit)
        node["limit"] = limit
        node["skip"] = skip
        if limit is not None and limit <= 0:
            return Node("empty")
        else:
            return node
    
    def empty(self, node, skip_limit):
        return node

    def _default(self, node, skip_limit):
        # print("_LimitApplier._default: node:", node.pretty())
        skip, limit = skip_limit
        node = self.visit_children(node, (0, None))
        if skip or limit:
            node = Node("skip_limit", [node], skip=skip, limit=limit)
        return node

class _RemoveEmpty(Ascender):
    
    def union(self, node, *children):
        children = [c for c in children if c.T != "empty"]
        #print("_RemoveEmpty.union: filtered children:", children)
        if not children:
            return Node("empty")
        elif len(children) == 1:
            #print("         returning:", children[0])
            return children[0]
        else:
            return Node("union", children)

    def join(self, node, *children):
        if any(c.T == "empty" for c in children):
            return Node("empty")
        else:
            return node
            
    def minus(self, node, left, right):
        if right.T == "empty" or left.T == "empty":
            return left
        else:
            return node
            
    def skip_limit(self, node, child, skip=0, limit=None):
        if child.T == "empty":
            return child
        else:
            return node
            
class _QueryOptionsApplier(Descender):
    
    #
    # Applies query params set outside of MQL: with/without metadata, with/without provenance, limit, default namespace
    #
    
    def basic_file_query(self, node, params):
        #print("LimitApplier: applying limit", limit)
        with_meta = params.get("with_meta")
        with_provenance = params.get("with_provenance")
        query = node["query"]
        query.WithMeta = query.WithMeta or with_meta
        query.WithProvenance = query.WithProvenance or with_provenance
        return node
        
    def _default(self, node, params):
        #print("_LimitApplier._default: node:", node.pretty())
        return self.visit_children(node, params)
            
    def meta_filter(self, node, params):
        node["with_provenance"] = params.get("with_provenance", False)
        node["with_meta"] = params.get("with_meta", False)
        new_params = params.copy()
        new_params["with_meta"] = True
        return self.visit_children(node, new_params)

    def parents_of(self, node, params):
        #print("_QueryOptionsApplier.parents_of/children_of: params:", params)
        node["with_provenance"] = params.get("with_provenance", False)
        node["with_meta"] = params.get("with_meta", False)
        new_params = params.copy()
        new_params["with_provenance"] = True
        return self.visit_children(node, new_params)
        
    children_of = parents_of

    def filter(self, node, params):
        # the filter may need metadata but probably not provenance
        new_params = params.copy()
        new_params["with_meta"] = True
        node["with_meta"] = params.get("with_meta", False)
        return self.visit_children(node, new_params)
        
    def file_list(self, node, params):
        node["with_meta"] = node["with_meta"] or params.get("with_meta", False)
        node["with_provenance"] = node["with_provenance"] or params.get("with_provenance", False)
        return node

class _MetaExpPusher(Descender):

    def join(self, node, meta_exp):
        return Node("join", [self.walk(c, meta_exp) for c in node.C])

    def union(self, node, meta_exp):
        return Node("union", [self.walk(c, meta_exp) for c in node.C])
        
    def minus(self, node, meta_exp):
        assert len(node.C) == 2
        left, right = node.C
        return Node("minus", [self.walk(left, meta_exp), self.walk(right, None)])
        
    def basic_file_query(self, node, meta_exp):
        #print("_MetaExpPusher.basic_file_query: meta_exp:", meta_exp)
        if meta_exp is not None:    
            bfq = node["query"]
            assert isinstance(bfq, BasicFileQuery)
            #assert bfq.Relationship is None             # this will be added by ProvenancePusher, as the next step of the optimization
            if bfq.Skip or bfq.Limit:
                node = Node("meta_filter", query=node, meta_exp=meta_exp)
            else:
                bfq.addWhere(meta_exp)
            #bfq.WithMeta = True
        return node
        
    def children_of(self, node, meta_exp):
        if meta_exp is None:
            return self.visit_children(node, None)
        else:
            #
            # meta_filter node is created when we can not push the meta_exp down any further
            #
            return Node("meta_filter", query=self.visit_children(node, None), meta_exp=meta_exp)
        
    parents_of = children_of
    
    def meta_filter(self, node, meta_exp):
        child = node["query"]
        if child.T in ("filter","file_list"):
            return node
        node_exp = node["meta_exp"]
        if node_exp is None:
            new_exp = meta_exp
        elif meta_exp is None:
            new_exp = node_exp
        else:
            new_exp = FileMetaExpressionDNF.regularize(Node("meta_and", [meta_exp, node_exp]))
        return self.walk(child, new_exp)

class _DatasetEvaluator(Ascender):
    
    def __init__(self, db, with_meta, limit):
        Ascender.__init__(self)
        self.DB = db
        self.WithMeta = with_meta
        self.Limit = limit

    def __call__(self, tree):
        out = self.walk(tree)
        return out

    def dataset_query_list(self, node, *args):
        queries = (a["query"] for a in args)
        return limited(unique(DBDataset.datasets_for_bdqs(self.DB, queries), key=lambda ds: (ds.Namespace, ds.Name)), self.Limit)
        

CMP_OPS = [">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*"]

def _merge_skip_limit(existing_skip, existing_limit, skip=0, limit=None):
    if existing_limit is None:
        return existing_skip+skip, limit
    elif limit is None:
        return existing_skip + skip, max(0, existing_limit - skip)
    else:
        return existing_skip + skip, max(0, min(existing_limit - skip, limit))

class BasicQueryQuery(object):

    def __init__(self, namespace, name, pattern=False, regexp=False, with_children=False, recursively=False, where=None):
        self.Namespace = namespace
        self.Name = name
        self.RegExp = regexp
        self.Where = where
        
    def line(self):
        return "BasicQueryQuery(%s:%s%s)" % (
                self.Namespace, self.Name, 
                " (regexp) " if self.RegExp else " (pattern) ")

    __str__ = line
    __repr__ = line
                
    def setWhere(self, where):
        self.Where = where
        
    def queries(self, db, limit=None):
        return DBQuery.queries_for_bqq(db, self, limit)

class BasicDatasetQuery(object):

    def __init__(self, namespace, name, pattern=False, regexp=False, with_children=False, recursively=False, where=None):
        self.Namespace = namespace
        self.Name = name
        self.Pattern = pattern
        self.RegExp = regexp
        self.WithChildren = with_children
        self.Recursively = recursively
        self.Where = where
        self.Ordered = False
        
    def is_explicit(self):
        return not self.Pattern and not self.WithChildren

    def line(self):
        return "BasicDatasetQuery(%s:%s%s%s%s%s)" % (
                self.Namespace, self.Name, 
                " (pattern) " if self.Pattern and not self.RegExp else (" (pattern re) " if self.Pattern and self.RegExp else ""),
                " with children" if self.WithChildren else "",
                " recursively" if self.Recursively else "",
                (" having:" + self.Where.pretty("    ") if self.Where is not None else ""))

    __str__ = line
    __repr__ = line
                
    def setWhere(self, where):
        self.Where = where
        
    def datasets(self, db, limit=None):
        return DBDataset.datasets_for_bdq(db, self, limit)
        
    def apply_params(self, params):
        # apply params from "with ..."
        default_namespace = params.get("namespace")
        if default_namespace:
            self.Namespace = self.Namespace or default_namespace
                
    def filter_by_where(self, datasets):
        if self.Where is None:
            yield from datasets
        evaluator = MetaEvaluator()
        for ds in datasets:
            if evaluator(ds.Metadata, self.Where):
                yield ds
                
class BasicFileQuery(object):
    
    def __init__(self, dataset_selectors, where=None):
        assert dataset_selectors is None or isinstance(dataset_selectors, list)
        if isinstance(dataset_selectors, list):
            assert all(isinstance(ds, BasicDatasetQuery) for ds in dataset_selectors)
        self.DatasetSelectors = dataset_selectors
        self.Wheres = where
        self.Limit = None
        self.WithMeta = False       
        self.WithProvenance = False
        self.Skip = 0
        self.Ordered = False
        
    def __str__(self):
        out = "BasicFileQuery(datasets:%s, limit:%s, skip:%s, %smeta, %sprovenance%s)" % (
            ",".join(str(s) for s in self.DatasetSelectors or []), 
            self.Limit, self.Skip,
            "with " if self.WithMeta else "no ",
            "with " if self.WithProvenance else "no ",
            "" if not self.Ordered else ", ordered",
            )
        if self.Wheres:
            out += "\n" + self.Wheres.pretty(indent="    ")
        return out

    __repr__ = __str__
        
    def _pretty(self, indent="", headline_indent=""):
        #print(f"BasicFileQuery._pretty(indent='{indent}', headline_indent='{headline_indent}')")
        head = str(self)
        lines = []
        if self.Wheres is not None:
            where_head, where_lines = self.Wheres._pretty(indent + "|   ")
            lines.append(indent + "where = " + where_head)
            lines += where_lines
        return head, lines

    def _pretty(self, indent="", headline_indent=""):
        #print(f"BasicFileQuery._pretty(indent='{indent}', headline_indent='{headline_indent}')")
        head = str(self)
        lines = []
        if self.Wheres is not None:
            where_head, where_lines = self.Wheres._pretty(indent + "|   ")
            lines.append(indent + "where = " + where_head)
            lines += where_lines
        return head, lines

    def pretty(self, indent=""):
        #print("pretty---")
        return "\n".join(self._pretty(indent))

    def addWhere(self, where):
        #print("BasicFileQuery.addWhere():----")
        #print("  self.Wheres:", self.Wheres)
        #print("  where:\n", where.pretty("    "))
        assert isinstance(where, Node) and where.T == "meta_or"
        if self.Wheres is None:
            wheres = where
        else:
            wheres = Node("meta_and", self.Wheres, where)
        self.Wheres = FileMetaExpressionDNF.regularize(wheres)
        #print("BasicFileQuery.addWhere() result:")
        #print(self.Wheres.pretty("    "))
            
    def addLimit(self, limit):
        self.add_skip_limit(0, limit)

    def addSkip(self, nskip):
        self.add_skip_limit(nskip, None)
            
    def add_skip_limit(self, skip, limit):
        self.Skip, self.Limit = _merge_skip_limit(self.Skip, self.Limit, skip, limit)

    def apply_params(self, params):
        default_namespace = params.get("namespace")
        if self.DatasetSelectors:
            for ds in self.DatasetSelectors:
                ds.apply_params(params)
    
    def single_dataset(self):
        return not self.DatasetSelectors or len(self.DatasetSelectors) == 1 and self.DatasetSelectors[0].is_explicit()
                
class Orderer(Descender):

    def __call__(self, tree):
        #print("Orderer: input: ", tree.pretty())
        out = self.walk(tree)
        #print("Orderer: output:", out.pretty())
        return out

    def basic_file_query(self, node, _):
        q = node["query"]
        q.Ordered = True
        return node

    basic_dataset_query = basic_file_query
    
    def filter(self, node, _):
        return node.clone(ordered = True)

    parents_of = children_of = filter
    
    def _done(self, node, _):
        return node
        
    ordered = file_list = _done

    def skip_limit(self, node, _):
        if node["skip"]:
            return node     # already ordered
        else:
            node.clone(children=[self(node.C[0])])

    def _default(self, node, _):
        return Node("ordered", [node])

class QueryConverter(Converter):
    
    #
    # converts parsed query (eiher file or dataset) from Lark tree structure to my own Node
    #

    def __init__(self, db=None, loader=None, default_namespace=None):
        self.DB = db
        self.Loader = loader
        self.DefaultNamespace = default_namespace

    def convert(self, tree):
        q = self.transform(tree)
        q.Parsed = tree
        return q
        
    def query(self, args):
        if False and len(args) == 2:
            params, query = args
            # ----  FIXME q = _WithParamsApplier().walk(query, params)
            #print("_Converter.query(): after applying params:", q.pretty())
        else:
            q = args[0]

        if q.T == "top_file_query":         out = FileQuery(q.C[0])
        elif q.T == "top_dataset_query":    out = DatasetQuery(q.C[0])
        elif q.T == "top_query_query":      out = QueryQuery(q.C[0])
        else:
            raise ValueError("Unrecognozed top level node type: %s" % (q.T,))
        print("QueryConverter: returning:", out.pretty())
        return out
    
    def __default__(self, typ, children, meta):
        return Node(typ, children, _meta=meta)
        
    def file_query_term(self, args):
        return args[0]

    def merge_meta(self, m1, m2):
        if m1 is None or m2 is None:
            return m1 or m2
        else:
            m = {}
            m.update(m1)
            m.update(m2)
            return m

    def limited_file_query_expression(self, args):
        assert len(args) == 2
        child, limit = args
        limit = int(limit)
        if child.T == "filter":
            child["limit"] = limit
        return Node("limit", [child], limit = limit)
        #return Node("file_query", [args[0]], meta = {"limit":int(args[1].value)})

    def ____make_ordered(self, node):
        if node.T in ("basic_file_query", "basic_dataset_query"):
            q = node["query"]
            q.Ordered = True
        elif node.T in ("filter", "parents_of", "children_of"):
            return node.clone(ordered = True)
        elif node.T in ("file_list", "ordered"):
            pass
        elif node.T == "skip_limit":
            if node["skip"]:
                pass        # aready ordered
            else:
                node = node.clone(children=[self.make_ordered(node.C[0])])
        else:
            node = Node("ordered", [node])
        return node
        
    def make_ordered(self, node):
        return Orderer()(node)

    def ordered(self, args):
        return self.make_ordered(args[0])

    def skip(self, args):
        assert len(args) == 2
        child, skip = args
        skip=int(skip)
        #print("skip: before make_ordered:", child.pretty())
        child = self.make_ordered(child)            # even if skip=0, still make it ordered
        #print("skip: after  make_ordered:", child.pretty())
        if skip == 0:   return child
        if child.T == "basic_file_query":
            q = child["query"]
            skip, limit = _merge_skip_limit(q.Skip, q.Limit, skip=skip)
            q.Skip = skip
            q.Limit = limit
            return child
        elif child.T == "skip_limit":
            skip, limit = _merge_skip_limit(child["skip"], child["limit"], skip=skip)
            return child.clone(skip=skip, limit=limit)
        else:   
            return Node("skip_limit", [child], skip=skip, limit=None)

    def limit(self, args):
        assert len(args) == 2
        child, limit = args
        limit=int(limit)
        if limit == 0:  return Node("empty")
        elif limit is None: return child
        elif child.T == "basic_file_query":
            q = child["query"]
            skip, limit = _merge_skip_limit(q.Skip, q.Limit, limit=limit)
            q.Skip = skip
            q.Limit = limit
            return child
        elif child.T == "skip_limit":
            skip, limit = _merge_skip_limit(child["skip"], child["limit"], limit=limit)
            return child.clone(skip=skip, limit=limit)
        else:
            return Node("skip_limit", [child], limit=limit, skip=0)     # do not make ordered for limit

    def meta_filter(self, args):
        q, meta_exp = args
        meta_exp = FileMetaExpressionDNF.regularize(meta_exp)
        if q.T == "basic_file_query":
            bfq = q["query"]
            if not (bfq.Skip or bfq.Limit):
                bfq.addWhere(meta_exp)
                return q
        return Node("meta_filter", query=q, meta_exp=meta_exp)

    def basic_file_query(self, args):
        if args:
            #print(args[0].pretty())
            assert len(args) == 1, "Expected 0 or 1 dataset selector list. Got: "+str(args)
            #print("Converter.basic_file_query: args[0].T:", args[0].T)
            assert args[0].T == "dataset_query_list"
            return Node("basic_file_query", query=BasicFileQuery([a["query"] for a in args[0].C]))
        else:
            return Node("basic_file_query", query=BasicFileQuery(None))

    def name_list(self, args):
        return [a.value for a in args]

    def file_list(self, args):
        spec_type = args[0].value
        assert spec_type in ("files", "fids", "file", "fid")
        if spec_type.endswith('s'):
            spec_type = spec_type[:-1]
        if spec_type == "file":
            namespace = self.DefaultNamespace
            specs = []
            for qname in args[-1].C:
                assert qname.T == "qualified_name"
                namespace = qname.get("namespace") or namespace
                specs.append(dict(namespace=namespace, name=qname["name"]))
        else:       # "fids"
            #print(args[-1], args[-1].C)
            specs = [fid.value for fid in args[-1].C]
        return Node("file_list", specs=specs, spec_type=spec_type,
                skip=0, limit=None,
                with_meta=False, with_provenance=False)

    def int_constant(self, args):
        v = args[0]
        return Node("int", value=int(v.value))
        
    def float_constant(self, args):
        v = args[0]
        return Node("float", value=float(v.value))

    def bool_constant(self, args):
        v = args[0]
        #print("bool_constant:", args, args[0].value)
        return Node("bool", value=v.value.lower() == "true")
        
    def unpack_string(self, node):
        assert node.type in ("STRING", "UNQUOTED_STRING")
        s = node.value
        if node.type == "STRING":
            if s[0] in ('"', "'"):
                s = s[1:-1]
        if '"' in s or "'" in s:        # sanitize
            raise ValueError("Unsafe string constant containing double or single quote: %s" % (repr(s),))
        return s

    def datetime_constant(self, args):
        s = self.unpack_string(args[0])
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return Node("float", value=float(dt.timestamp()))

    def date_constant(self, args):
        dt = self.unpack_string(args[0])
        date.fromisoformat(dt)         # check if it parses
        if len(args) == 1:
            dt = datetime.fromisoformat(dt)
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            tzshift = self.unpack_string(args[1]) or "+00:00"
            if tzshift[0] not in "-+":
                tzshift = "+" + tzshift
            dt = datetime.fromisoformat(dt + " 00:00:00" + tzshift)
        return Node("date_constant", value=float(dt.timestamp()))

    def string_constant(self, args):
        s = self.unpack_string(args[0])
        return Node("string", value=s)

    #def constant_list(self, args):
    #    return [n["value"] for n in args]

    def qualified_name(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            out = Node("qualified_name", namespace=self.DefaultNamespace, name=args[0].value)      # no namespace
        else:
            out = Node("qualified_name", namespace=args[0].value, name=args[1].value)
        #print("Converter.qualified_name: returning: %s" % (out.pretty(),))
        return out
        
    def named_query(self, args):
        if self.DB is None and self.Loader is None:
            raise RuntimeError("Can not load named query without access to the database or query loader")
        (q,) = args
        namespace = q["namespace"] or self.DefaultNamespace
        name = q["name"]
        if self.DB is not None:
            loaded = MQLQuery.from_db(self.DB, namespace, name, convert=False)
        else:
            loaded = MQLQuery.from_loader(self.Loader, namespace, name, convert=False)
        if loaded is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        tree = self.convert(loaded)
        #print("named_query: parsed tree:", tree.pretty())
        if tree.T != "top_file_query":
            raise ValueError(f"Named query {namespace}:{name} must be a file query")
        return tree.C[0]

    def param_def_list(self, args):
        return dict([(a.C[0].value, a.C[1]["value"]) for a in args])
        
    def filter_params(self, args):
        if len(args) == 2:
            assert isinstance(args[0], list)
            assert isinstance(args[1], dict)
            return tuple(args)
        else:
            assert len(args) == 1
            if isinstance(args[0], list):
                return (args[0], {})
            elif isinstance(args[0], dict):
                return ([], args[0])
            else:
                assert False, "Unknown type of filter param:" + str(args[0])

    def union(self, args):
        assert len(args) == 1
        args = args[0].C
        if len(args) == 1:  return args[0]
        unions = []
        others = []
        for a in args:
            if isinstance(a, Node) and a.T == "union":
                unions += a[1:]
            else:
                others.append(a)
        return Node("union", unions + others)
        
    def join(self, args):
        #print("join: args:", args)
        #for a in args:
        #    print("  ", a.pretty())
        assert len(args) == 1
        args = args[0].C
        if len(args) == 1:  return args[0]
        joins = []
        others = []
        for a in args:
            if isinstance(a, Node) and a.T == "join":
                joins += a.C
            else:
                others.append(a)
        return Node("join", joins + others)

    def params_list(self, args):
        # convert date, datetime to floats
        assert len(args) == 1 and args[0].T == "constant_list"
        return [c["value"] for c in args[0].C]

    def filter(self, args):
        if len(args) == 3:
            name, (params, kv), queries = args
        else:
            assert len(args) == 2
            name, queries = args
            params = []
            kv = {}
        queries = queries.C
        for q in queries:
            for bfq in q.find_all("basic_file_query"):
                bfq["query"].WithMeta = True
        node = Node("filter", queries, name = name.value, params=params, kw=kv, skip=0, limit=None, ordered=False, with_meta=False)
        return node

    def meta_attribute(self, args):
        (t,) = args
        return Node("meta_attribute", name=t.value)

    def object_attribute(self, args):
        (t,) = args
        return Node("object_attribute", name=t.value)

    def _convert_array_all(self, node):
        left = node.C[0]
        if left.T == "array_all":
            if node.T == "cmp_op":
                new_op = {
                    "~":   "!~",
                    "!~":  "~",
                    "~*":   "!~*",
                    "!~*":  "~*",
                    ">":    "<=",
                    "<":    ">=",
                    ">=":    "<",
                    "<=":    ">",
                    "=":    "!=",
                    "==":    "!=",
                    "!=":    "=="
                }[node["op"]]
                node["op"] = new_op
            else:
                node.T = {
                    "in_set":"not_in_set",
                    "in_range":"not_in_range",
                    "not_in_set":"in_set",
                    "not_in_range":"in_range",
                }[node.T]
            left.T = "array_any"
            node["neg"] = not node.get("neg")
        #print("_convert_array_all: returning:", node.pretty())
        return node
    
    def array_any(self, args):
        (n,) = args
        return Node("array_any", name=n.value)
        
    def array_all(self, args):
        (n,) = args
        return Node("array_all", name=n.value)
        
    def array_length(self, args):
        (n,) = args
        return Node("array_length", name=n.value)
        
    def subscript(self, args):
        name, inx = args
        if inx.type == "STRING":
            inx = inx.value[1:-1]
        else:
            inx = int(inx.value)
        return Node("subscript", name=name.value, index=inx)

    def json_path(self, args):
        node = Node("json_path", [args[0]], neg=False)

    def cmp_op(self, args):
        left, op, right = args
        if right.T == "date_constant":
            t = right["value"]
            if op in ("=", "=="):
                node = Node("meta_and",
                    [
                        Node("cmp_op", [left, Node("float", value=t)], op=">=", neg=False),
                        Node("cmp_op", [left, Node("float", value=t + 3600*24)], op="<", neg=False),
                    ]
                )
            elif op == "!=":
                node = Node("meta_or",
                    [
                        Node("cmp_op", [left, Node("float", value=t)], op="<", neg=False),
                        Node("cmp_op", [left, Node("float", value=t + 3600*24)], op=">=", neg=False),
                    ]
                )
            elif op == "<":
                node = Node("cmp_op", [left, Node("float", value=t)], op="<", neg=False)
            elif op == "<=":
                node = Node("cmp_op", [left, Node("float", value=t + 3600*24)], op="<", neg=False)
            elif op == ">":
                node = Node("cmp_op", [left, Node("float", value=t + 3600*24)], op=">=", neg=False)
            elif op == ">=":
                node = Node("cmp_op", [left, Node("float", value=t)], op=">=", neg=False)
            else:
                raise ValueError("Unsopported comparison operation for date constant: %s", op)
        else:
            node = Node("cmp_op", [args[0], args[2]], op=args[1].value, neg=False)
        return self._convert_array_all(node)
        
    def constant_in_array(self, args):
        return Node("cmp_op",
            [Node("array_any", name=args[1].value), args[0]], op="=", neg=False
        )
        
    def constant_not_in_array(self, args):
        return Node("cmp_op",
            [Node("array_any", name=args[1].value), args[0]], op="=", neg=True
        )
        
    def constant_in(self, args):
        const_arg = args[0]
        const_type = const_arg.T
        const_value = const_arg["value"]
        array_in = Node("cmp_op", [Node("array_any", name=args[1].value), const_arg], op="=", neg=False)
        if const_type == "string":
            return Node("meta_or",
                [   array_in,
                    Node("cmp_op", [
                        Node("meta_attribute", name=args[1].value), 
                        Node("string", value=".*%s.*" % (const_value,))
                    ], op="~", neg=False)
                ]
            )
        else:
            return array_in
            
        
    def constant_not_in(self, args):
        const_arg = args[0]
        const_type = const_arg.T
        const_value = const_arg["value"]
        array_not_in = Node("cmp_op", [Node("array_any", name=args[1].value), const_arg], op="=", neg=True)
        if const_type == "string":
            return Node("meta_and",
                [   array_not_in,
                    Node("cmp_op", [
                        Node("meta_attribute", name=args[1].value), 
                        Node("string", value=".*%s.*" % (const_value,))
                    ], op="~", neg=True)
                ]
            )
        else:
            return array_not_in
        
    def in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float", "date_constant") and args[2].T in ("string", "int", "float", "date_constant")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        typ = args[1].T
        if typ == "date_constant":
            left = args[0]
            low = args[1]["value"]
            high = args[2]["value"] + 24*3600
            node = Node("meta_and",
                [
                    Node("cmp_op", [left, Node("float", value=low)], op=">="),
                    Node("cmp_op", [left, Node("float", value=high)], op="<"),
                ]
            )
        else:
            node = Node("in_range", [args[0]], low=args[1]["value"], high=args[2]["value"], neg=False, type=args[1].T)
        return self._convert_array_all(node)
    
    def not_in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float", "date_constant") and args[2].T in ("string", "int", "float", "date_constant")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        typ = args[1].T
        if typ == "date_constant":
            left = args[0]
            low = args[1]["value"]
            high = args[2]["value"] + 24*3600
            node = Node("meta_or",
                [
                    Node("cmp_op", [left, Node("float", value=low)], op="<"),
                    Node("cmp_op", [left, Node("float", value=high)], op=">="),
                ]
            )
        else:
            node = Node("in_range", [args[0]], low=args[1]["value"], high=args[2]["value"], neg=True, type=args[1].T)
        return self._convert_array_all(node)

    def in_set(self, args):
        assert len(args) == 2 and args[1].T == "constant_list"
        constant_list = args[1]
        if any(c.T == "date_constant" for c in constant_list.C):
            raise ValueError("in_set operation is not supported for date()")
        values = [c["value"] for c in constant_list.C]
        return self._convert_array_all(Node("in_set", [args[0]], neg=False, set=values))
        
    def not_in_set(self, args):
        assert len(args) == 2 and args[1].T == "constant_list"
        constant_list = args[1]
        if any(c.T == "date_constant" for c in constant_list.C):
            raise ValueError("in_set operation is not supported for date()")
        values = [c["value"] for c in constant_list.C]
        return self._convert_array_all(Node("in_set", [args[0]], neg=True, set=values))
        
    def index(self, args):
        return args[0].value
        
    def meta_and(self, args):
        children = []
        for a in args:
            if a.T == "meta_and":
                children += a.C
            else:
                children.append(a)
        return Node("meta_and", children)
        
    def meta_or(self, args):
        children = []
        for a in args:
            if a.T == "meta_or":
                children += a.C
            else:
                children.append(a)
        return Node("meta_or", children)
        
    def present(self, args):
        assert len(args) == 1
        return Node("present", name = args[0].value)

    def not_present(self, args):
        assert len(args) == 1
        return Node("not_present", name = args[0].value)

    def _apply_not(self, node):
        
        def reverse_array_wildcard(node):
            if node.T == "array_any":
                node = node.clone()
                node.T = "array_all"
            elif node.T == "array_all":
                node = node.clone()
                node.T = "node_any"
            else:
                pass
            return node
        
        if node.T in ("meta_and", "meta_or") and len(node.C) == 1:
            return self._apply_not(node.C[0])
        if node.T == "meta_and":
            return Node("meta_or", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_or":
            return Node("meta_and", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_not":
            return node.C[0]
        elif node.T in ("cmp_op", "in_set", "in_range", "json_path"):   # why cmp_op is here ??? 
            node["neg"] = not node.get("neg")
            return node
        elif node.T == "cmp_op":                                        # why cmp_op is not here ???
            new_op = {
                "~":   "!~",
                "!~":  "~",
                "~*":   "!~*",
                "!~*":  "~*",
                ">":    "<=",
                "<":    ">=",
                ">=":    "<",
                "<=":    ">",
                "=":    "!=",
                "==":    "!=",
                "!=":    "=="
            }[node["op"]]
            return node.clone(op=new_op)
        elif node.T == "present":
            return Node("not_present", name=node["name"])
        elif node.T == "not_present":
            return Node("present", name=node["name"])
        else:
            raise ValueError("Unknown node type %s while trying to apply NOT operation" % (node.T,))
            
    def meta_not(self, children):
        assert len(children) == 1
        return self._apply_not(children[0])
        
    #
    # Datasets
    #
    
    def _______did_pattern(self, args):
        assert len(args) in (1,2)
        namespace = self.DefaultNamespace
        if len(args) == 1:
            name = args[0].value
        else:
            namespace, name = args[0].value, args[1].value
        # unquote the string
        if name.startswith("'") or name.startswith('"'):
            name = name[1:-1]
        name = name.replace('*', '%')
        name = name.replace('?', '_')
        out = Node("did_pattern", namespace=namespace, name=name)
        return out

    def regexp_pattern(self, args):
        assert len(args) in (1,2)
        namespace = self.DefaultNamespace
        if len(args) == 1:
            name = args[0].value
        else:
            namespace, name = args[0].value, args[1].value
        name = name[1:-1]           # remove enclosing quotes
        out = Node("regexp_pattern", namespace=namespace, name=name)
        return out

    def sql_pattern(self, args):
        assert len(args) in (1,2)
        namespace = self.DefaultNamespace
        if len(args) == 1:
            name = args[0].value
        else:
            namespace, name = args[0].value, args[1].value
        name = name.replace("*", "%").replace("?", "_")
        out = Node("sql_pattern", namespace=namespace, name=name)
        return out

    def did(self, args):
        assert len(args) == 2
        namespace, name = args[0].value, args[1].value
        return Node("did", namespace=namespace, name=name)

    def dataset_spec(self, args):
        name_or_pattern = args[-1]
        pattern = "matching" in args
        regexp = "regexp" in args
        assert name_or_pattern.T in ("did", "sql_pattern", "regexp_pattern")
        name = name_or_pattern["name"]
        namespace = name_or_pattern.get("namespace") or self.DefaultNamespace
        query = BasicDatasetQuery(namespace, name, pattern=pattern, regexp=regexp)
        return Node("basic_dataset_query", query=query)
        
    def dataset_add_where(self, children):
        assert len(children) == 2
        bdq, where = children
        assert bdq.T == "basic_dataset_query", "Unknown node type: "+bdq.pretty()
        q = bdq["query"]
        q.setWhere(children[1])
        return Node("basic_dataset_query", query=q)

    def dataset_add_subsets(self, children):
        assert len(children) == 2
        bdq, subsets = children
        q = bdq["query"]
        q.WithChildren = True
        q.Recursively = subsets["recursive"]
        return Node("basic_dataset_query", query=q)

    def dataset_provenance_op(self, children):
        return Node("subsets", recursive=any(c.value == "recursively" for c in children))
        
    #
    # Queries
    #
    def query_name_match(self, args):
        did_pattern = args[-1]
        assert did_pattern.T in ("sql_pattern", "regexp_pattern")
        query = BasicQueryQuery(did_pattern["namespace"], did_pattern["name"], 
            regexp=did_pattern.T == "regexp_pattern")
        return Node("basic_query_query", query=query)

    def top_query_query(self, args):
        meta_exp = args[-1] if "where" in args else None
        if "matching" in args:
            bqq = args[2]["query"]
        else:
            bqq = BasicQueryQuery()
        if "where" in args:
            bqq.setWhere(args[-1])
        return Node("basic_query_query", query=bqq)

class MQLQuery(object):
    
    @staticmethod
    def parse(text, db=None, loader=None, debug=False, convert=True, default_namespace=None, include_retired_files=None):
        out = []
        for l in text.split("\n"):
            l = l.split('#', 1)[0]
            out.append(l)
        text = '\n'.join(out)
        try:
            parsed = _Parser.parse(text)
            print("parsed:\n", parsed.pretty())
            if convert:
                converted = QueryConverter(db=db, loader=loader, default_namespace=default_namespace).convert(parsed)
                print("converted:\n", converted.pretty())
                if converted.T == "top_file_query":
                    q = FileQuery(converted.C[0], include_retired_files)
                elif converted.T == "top_dataset_query":
                    q = DatasetQuery(converted.C[0])
                else:
                    q = QueryQuery(converted)
                q.Parsed = parsed
                return q
            else:
                return parsed
        except LarkError as e:
            raise MQLSyntaxError(str(e))
        
    @staticmethod
    def from_db(db, namespace, name, convert=True):
        q = DBNamedQuery.get(db, namespace, name)
        if q is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        text = q.Source
        return MQLQuery.parse(text, convert=convert)
        
    @staticmethod
    def from_loader(loader, namespace, name, convert=True):
        data = loader.get_named_query(namespace, name)
        if isinstance(data, dict):
            text = data["source"]
        else:
            text = data
        if text is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        return MQLQuery.parse(text, convert=convert)
        
    @staticmethod
    def from_db_raw(db, namespace, name):
        q = DBNamedQuery.get(db, namespace, name)
        if q is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        return q.Source
