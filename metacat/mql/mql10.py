from metacat.db import DBDataset, DBFile, DBNamedQuery, DBFileSet
from metacat.util import limited, unique
from .trees import Node, pass_node, Ascender, Descender, Visitor, Converter, LarkToNodes
from .sql_converter import SQLConverter
from .query_executor import FileQueryExecutor
from .meta_evaluator import MetaEvaluator
from .converter import QueryConverter
import json, time

from lark import Lark, LarkError
from lark import Tree, Token
import pprint

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
        
def _make_DNF(exp, op=None, exp1=None):
    if op == "or":
        exp = Node("meta_or", [exp, exp1])
    elif op == "and":
        exp = Node("meta_and", [exp, exp1])

    return _MetaRegularizer().walk(exp)
    
def _merge_skip_limit(existing_skip, existing_limit, skip=0, limit=None):
    if existing_limit is None:
        return existing_skip+skip, limit
    elif limit is None:
        return existing_skip + skip, max(0, existing_limit - skip)
    else:
        return existing_skip + skip, max(0, min(existing_limit - skip, limit))

class DatasetQuery(object):
    
    Type = "dataset"
    
    def __init__(self, tree):
        self.Tree = tree

    def run(self, db, limit=None, with_meta=True, with_provenance=True, filters={}, default_namespace=None, debug=False):
        return _DatasetEvaluator(db, with_meta, limit)(self.Tree)
        
class FileQuery(object):

    Type = "file"

    def __init__(self, tree):
        self.Tree = tree
        self.Assembled = self.Optimized = self.Compiled = None

    def __str__(self):
        return "FileQuery(\n%s\n)" % (self.Tree.pretty("  "),)

    def assemble(self, db, default_namespace = None):
        #print("FileQuery.assemble: self.Assembled:", self.Assembled)
        if self.Assembled is None:
            #print("FileQuery.assemble: assembling...")
            self.Assembled = _Assembler(db, default_namespace).walk(self.Tree)
        return self.Assembled

    def skip_assembly(self):
        if self.Assembled is None:
            self.Assembled = self.Tree
        return self.Assembled

    def optimize(self, debug=False, default_namespace=None, skip=0, limit=None):
        if self.Optimized is None:
            #print("Query.optimize: assembled:----\n", self.Assembled.pretty())
            
            optimized = self.Tree
            
            optimized = _SkipLimitApplier().walk(optimized)
            if debug:
                print("Query.optimize: after 1st _SkipLimitApplier:----")
                print(optimized.pretty("    "))
                
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
            
            optimized = _OrderedApplier()(optimized)
            if debug:
                print("Query.optimize: after applying ordering:----")
                print(optimized.pretty("    "))
            
            self.Optimized = optimized
        return self.Optimized

    def compile(self, db=None, skip=0, limit=None, with_meta=False, with_provenance=False, default_namespace=None, debug=False):
        try:
            optimized = self.optimize(debug=debug, default_namespace=default_namespace, skip=skip, limit=limit)
            optimized = _QueryOptionsApplier().walk(optimized, 
                dict(
                    with_provenance = with_provenance,
                    with_meta = with_meta
                ))
            if debug:
                print("after _QueryOptionsApplier:", optimized.pretty())
            self.Compiled = compiled = SQLConverter(db, debug=debug)(optimized)
        except Exception as e:
            raise MQLCompilationError(str(e))

        if debug:
            print("\nCompiled:", compiled.pretty())

        return compiled

    def run(self, db, filters={}, skip=0, limit=None, with_meta=True, with_provenance=True, default_namespace=None, debug=False):
        compiled = self.Compiled or self.compile(db=db, skip=skip, limit=limit, 
                    with_meta=with_meta, with_provenance=with_provenance, 
                    default_namespace=default_namespace, debug=debug)
        try:
            result = FileQueryExecutor(db, filters, debug=debug)(compiled)
        except Exception as e:
            raise MQLExecutionError(str(e))
        
        return result

class _Assembler(Ascender):

    def __init__(self, db, default_namespace):
        Ascender.__init__(self)
        self.DB = db
        self.DefaultNamespace = default_namespace
        
    def named_query(self, node, name=None, namespace=None):
        namespace = namespace or self.DefaultNamespace
        parsed = MQLQuery.from_db(self.DB, namespace, name)
        assert parsed.Type == "file"
        tree = parsed.Tree
        tree = _WithParamsApplier().walk(tree, {"namespace":namespace})
        #print("_Assembler.named_query: returning:", tree.pretty())
        return tree
        
class _OrderedApplier(Descender):
    
    def walk(self, tree, ordered=False):
        return Descender.walk(self, tree, ordered)
        
    __call__ = walk

    def ordered(self, node, ordered):
        child = node.C[0]
        return self.walk(child, True)
        
    def basic_file_query(self, node, ordered):
        node["query"].Ordered = node["query"].Ordered or ordered
        return node

    basic_dataset_query = basic_file_query

    def skip_limit(self, node, ordered):
        child = self.walk(node.C[0], True)
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
        #print("_SkipLimitApplier: skip_limit:", skip_limit, "  children:", node.C)
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
        #print("_LimitApplier._default: node:", node.pretty())
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
            new_exp = _make_DNF(Node("meta_and", [meta_exp, node_exp]))
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

class MQLQuery(object):
    
    @staticmethod
    def parse(text, debug=False, convert=True):
        out = []
        for l in text.split("\n"):
            l = l.split('#', 1)[0]
            out.append(l)
        text = '\n'.join(out)
        try:
            parsed = _Parser.parse(text)
            if debug:
                print("parsed:\n", parsed.pretty())
            if convert:
                converted = QueryConverter().convert(parsed)
                if debug:
                    print("converted:\n", converted.pretty())
                if converted.T == "top_file_query":
                    q = FileQuery(converted.C[0])
                else:
                    q = DatasetQuery(converted.C[0])
                q.Parsed = parsed
                return q
            else:
                return parsed
        except LarkError as e:
            raise MQLSyntaxError(str(e))
        
    @staticmethod
    def from_db(db, namespace, name):
        q = DBNamedQuery.get(db, namespace, name)
        if q is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        text = q.Source
        return MQLQuery.parse(text)
        
    @staticmethod
    def from_db_raw(db, namespace, name):
        q = DBNamedQuery.get(db, namespace, name)
        if q is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        return q.Source
        return MQLQuery.parse(text)
        
if __name__ == "__main__":
    import sys, traceback
    import psycopg2
    from filters import filters_map
    
    def interactive(db):
        from filters import filters_map
    
        while True:
            q = ""
            prompt = "(parse|run) ... ;> "
            while not ';' in q:
                q += input(prompt)
                prompt = "...> "
            
            cmd, rest = q.split(None, 1)
            qtext = test.split(';', 1)[0]
            print (f"--- query ---\n{qtext}\n-------------")
            try:    q = MQLQuery.parse(qtext)
            except Exception as e:
                traceback.print_exc()
            else:
                if cmd == "parse":
                    q.pprint()
                else:
                    results = q.run(db, with_meta=True, filters = filters_map)
                    for r in results:
                        print (r)
            
        
    db = psycopg2.connect(sys.argv[2])

    interactive(db)
    
