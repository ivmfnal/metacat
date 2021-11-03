from metacat.db import DBDataset, DBFile, DBNamedQuery, DBFileSet, limited
from .trees import Node, pass_node, Ascender, Descender, Visitor
import json, time

from lark import Lark
from lark import Transformer, Tree, Token
import pprint

CMP_OPS = [">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*"]

from .grammar9 import MQL_Grammar
_Parser = Lark(MQL_Grammar, start="query")

class _MetaRegularizer(Ascender):
    # converts the meta expression into DNF form:
    #
    #   Node(or, [Node(and, [exp, ...])])
    #

    def _flatten_bool(self, op, nodes):
        #print("_flatten_bool: input:", nodes)
        new_nodes = []
        for c in nodes:
            if c.T == op:
                new_nodes += self._flatten_bool(op, c.C)
            else:
                new_nodes.append(c)
        #print("_flatten_bool: output:", new_nodes)
        return new_nodes

    def meta_or(self, children, meta):
        children = [x if x.T == "meta_and" else Node("meta_and", [x]) for x in self._flatten_bool("meta_or", children)]
        out = Node("meta_or", children)
        return out

    def _generate_and_terms(self, path, rest):
        if len(rest) == 0:  yield path
        else:
            node = rest[0]
            rest = rest[1:]
            if node.T == "meta_or":
                for c in node.C:
                    my_path = path + [c]
                    for p in self._generate_and_terms(my_path, rest):
                        yield p
            else:
                for p in self._generate_and_terms(path + [node], rest):
                    yield p

    def meta_and(self, children, meta):
        children = self._flatten_bool("meta_and", children)
        or_present = False
        for c in children:
            if c.T == "meta_or":
                or_present = True
                break

        if or_present:
            paths = list(self._generate_and_terms([], children))
            #print("paths:")
            #for p in paths:
            #    print(p)
            paths = [self._flatten_bool("meta_and", p) for p in paths]
            #print("meta_and: children:", paths)
            return Node("meta_or", [Node("meta_and", p) for p in paths])
        else:
            return Node("meta_and", children)
                
    
    @staticmethod
    def _make_DNF_lists(exp):
        if exp is None: return None
        if exp.T in CMP_OPS or exp.T == "in":
            return self._make_DNF(Node("meta_and", [exp]))
        elif exp.T == "meta_and":
            return self._make_DNF(Node("meta_or", [exp]))
        elif exp.T == "meta_or":
            or_exp = []
            assert exp.T == "meta_or"
            for meta_and in exp.C:
                and_exp = []
                assert meta_and.T == "meta_and"
                for c in meta_and.C:
                    assert c.T in CMP_OPS or c.T in ("in", "present", "not_present"), "Unknown comparison operation %s" % (c.T,)
                    and_exp.append((c.T, c.C[0], c.C[1]))
                or_exp.append(and_exp)
            return or_exp
            
class BasicFileQuery(object):
    
    def __init__(self, dataset_selector=None, where=None):
        self.DatasetSelector = dataset_selector
        self.Wheres = where
        self.WheresDNF = None
        self.Limit = None
        self.WithMeta = False       # this is set to True if the query has "where" clouse
        
    def line(self):
        return "BasicFileQuery(selector:%s, limit:%s, meta_exp:%s)" % (self.DatasetSelector, self.Limit, 
            self.Wheres)
        
    __str__ = line

    def addWhere(self, where):
        #print("BasicFileQuery.addWhere():----")
        #print("  self.Wheres:", self.Wheres)
        #print("  where:", where.pretty())
        assert isinstance(where, Node) and where.T == "meta_or"
        if self.Wheres is None:
            wheres = where
        else:
            wheres = self.Wheres & where
        self.Wheres = _MetaRegularizer().walk(wheres)
            
    def wheres_dnf(self):
        return self.WheresDNF
        
    def addLimit(self, limit):
        if self.Limit is None:   self.Limit = limit
        else:   self.Limit = min(self.Limit, limit)
        
    def apply_params(self, params):
        self.DatasetSelector.apply_params(params)

class DatasetSelector(object):

    def __init__(self, patterns, with_children, recursively, having):
        self.Patterns = patterns
        self.WithChildren = with_children
        self.Recursively = recursively
        self.Having = having

    def line(self):
        return "DatasetSelector(patterns=%s with_children=%s rec=%s having=%s)" % (self.Patterns, self.WithChildren,
                self.Recursively, self.Having)

    __str__ = line
                
    def setHaving(self, having):
        self.Having = having
        
    def datasets(self, db, limit=None):
        return DBDataset.apply_dataset_selector(db, self, limit)
        
    def apply_params(self, params):
        # apply params from "with ..."
        default_namespace = params.get("namespace")
        if default_namespace:
            self.Patterns = [(match, (namespace or default_namespace, name)) 
                for match, (namespace, name) in self.Patterns
            ]
        
class _ParamsApplier(Descender):
    
    # applies params from "with...", including default namespace

    def basic_file_query(self, node, params):
        bfq = node.M
        assert isinstance(bfq, BasicFileQuery)
        bfq.apply_params(params)
        return node

    def dataset_selector(self, node, params):
        print("_ParamsApplier.dataset_selector: applying params:", params)
        selector = meta
        assert isinstance(selector, DatasetSelector)
        selector.apply_params(params)
        return node
        
    def named_query(self, node, params):
        #print("_ParamsApplier:named_query %s %s" % (node, params))
        if params is not None:
            assert isinstance(params, dict)
            assert len(node.M) == 2
            if node.M[0] is None:
                node.M[0] = params.get("namespace")
        return node

    def query(self, node, params):
        if len(node.C) == 2:
            p, q = args
            new_params = params.copy()
            new_params.update(p)
            return Node("query", [self.walk(q, new_params)])
        else:
            return node
            
    def qualified_name(self, node, params):
        if params is not None:
            assert isinstance(params, dict)
            assert len(node.M) == 2
            if node.M[0] is None:
                node.M[0] = params.get("namespace")
        return node
        
class DatasetQuery(object):
    
    Type = "dataset"
    
    def __init__(self, tree):
        self.Tree = tree

    def run(self, db, limit=None, with_meta=True, filters={}, default_namespace=None):
        return _DatasetEvaluator(db, with_meta, limit).walk(self.Tree)
        
class FileQuery(object):

    Type = "file"

    def __init__(self, tree):
        self.Tree = tree
        self.Assembled = self.Optimized = None
        
    def __str__(self):
        return "FileQuery(%s)" % (self.Tree.pretty(),)
        
    def assemble(self, db, default_namespace = None):
        #print("FileQuery.assemble: self.Assembled:", self.Assembled)
        if self.Assembled is None:
            #print("FileQuery.assemble: assembling...")
            self.Assembled = _Assembler(db, default_namespace).walk(self.Tree)
        return self.Assembled
        
    def skip_assembly(self):
        if self.Assembled is None:
            self.Assembled = self.Q
        return self.Assembled
        
    def optimize(self):
        #print("Query.optimize: entry")
        assert self.Assembled is not None
        if self.Optimized is None:
            #print("Query.optimize: assembled:----\n", self.Assembled.pretty())
            optimized = _ProvenancePusher().walk(self.Assembled)
            #print("Query.optimize: after _ProvenancePusher:----\n", optimized.pretty())
            optimized = _MetaExpPusher().walk(optimized, None)
            #print("Query.optimize: after _MetaExpPusher:----\n", optimized.pretty())
            optimized = _DNFConverter().walk(optimized, None)
            #print("Query.optimize: after DNF converter:----\n", optimized.pretty())
            self.Optimized = optimized
        return self.Optimized

    def run(self, db, filters={}, limit=None, with_meta=True, default_namespace=None):
        #print("Query.run: DefaultNamespace:", self.DefaultNamespace)
        
        self.assemble(db, default_namespace = default_namespace)
        #print("Query.run: assemled:", self.Assembled.pretty())
        
        optimized = self.optimize()
        #print("Query.run: optimized: ----\n", optimized.pretty())
        
        if default_namespace is not None:
            optimized = _ParamsApplier().walk(optimized, {"namespace":default_namespace})
        
        optimized = _LimitApplier().walk(optimized, limit)
        #print("Limit %s applied: ----\n" % (limit,), optimized.pretty())
        
        out = _FileEvaluator(db, filters, with_meta, None).walk(optimized)
        #print ("run: out:", out)
        return out
        
class _Converter(Transformer):
    
    def query(self, args):
        if len(args) == 2:
            params, query = args
            print("_Converter.query(): applying params:", params)
            q = _ParamsApplier().walk(query, params)
            print("_Converter.query(): after applying params:", q.pretty())
        else:
            q = args[0]
            
        if q.T == "file_query": out = FileQuery(q)
        elif q.T == "dataset_query": out = DatasetQuery(q)
        else:
            raise ValueError("Unrecognozed top level node type: %s" % (q.T,))
        #print("_Converter.query() -> ", out)
        return out
        
    def convert(self, tree):
        return self.transform(tree)
        
    def file_query(self, args):
        limit = None
        meta_filter_exp = None
        for i, arg in enumerate(args):
            if isinstance(arg, Token) and arg.value == "where":
                meta_filter_exp = args[i+1]
            elif isinstance(arg, Token) and arg.value == "limit":
                limit = int(args[i+1].value)
        meta = {"limit":limit}
        if meta_filter_exp is None:
            return Node("file_query", args[:1], meta=meta)
        else:
            return Node("file_query", [Node("meta_filter", args[:1], meta=meta_filter_exp)], meta=meta)

    def basic_file_query(self, args):
        assert len(args) == 0 or len(args) == 1 and isinstance(args[0].M, DatasetSelector)
        dataset_selector = args[0].M if args else None
        q = BasicFileQuery(dataset_selector)
        return Node("basic_file_query", meta = q)

    def datasets_selector(self, args):
        spec_list = args[0].M
        with_children = False
        recursively = False
        having = None
        args_ = args[1:]
        for i, a in enumerate(args_):
            if a.value == "children":
                with_children = True
            elif a.value == "recursively":
                recursively = True
            elif a.value == "having":
                having = args_[i+1]
        meta = DatasetSelector(spec_list, with_children, recursively, having)
        return Node("datasets_selector", meta = meta)

    def limited_file_query(self, args):
        assert len(args) in (1,2)
        limit = None if len(args) == 1 else int(args[1].value)
        return FileQuery(args[0], limit)
            
    def s_(self, args):
        assert len(args) == 1
        return args[0]

    def int_constant(self, args):
        return int(args[0].value)
        
    def float_constant(self, args):
        return float(args[0].value)

    def bool_constant(self, args):
        #print("bool_constant:", args, args[0].value)
        return args[0].value.lower() == "true"
        
    def string_constant(self, args):
        s = args[0].value
        if s[0] in ('"', "'"):
            s = s[1:-1]
        return s
        
    def constant_list(self, args):
        return args
        
    def dataset_spec_list(self, args):
        return Node("dataset_spec_list", [], meta=[a.M for a in args])
        
    def dataset(self, args):
        return 

    def dataset_spec(self, args):
        assert len(args) == 1
        return Node("dataet_spec", meta=(args[0].T == "dataset_pattern", args[0].M))
            
    def dataset_pattern(self, args):
        if len(args) == 1:
            return Node("dataset_pattern", meta=[None, args[0].value[1:-1]])
        else:
            return Node("dataset_pattern", meta=[args[0].value, args[1].value[1:-1]])
        
    def named_query(self, args):
        assert len(args) == 1
        out = Node("named_query", meta = args[0].M)       # value = (namespace, name) - tuple
        #print("Converter.named_query(%s): returning %s" % (args, out))
        return out
        
    def exp_list(self, args):
        return args

    def __default__(self, type, children, meta):
        #print("__default__:", data, children)
        return Node(type, children)
        
    def param_def(self, args):
        return (args[0].value, args[1])

    def param_def_list(self, args):
        return dict(args)
        
    def parents_of(self, args):
        assert len(args) == 1
        return Node("parents_of", args)
        
    def children_of(self, args):
        assert len(args) == 1
        return Node("children_of", args)
        
    def add(self, args):
        assert len(args) == 2
        left, right = args
        if isinstance(left, Node) and left.T == "union":
            return left + [right]
        else:
            return Node("union", [left, right])

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
        
    def mult(self, args):
        assert len(args) == 2
        left, right = args
        if isinstance(left, Node) and left.T == "join":
            return left + [right]
        else:
            return Node("join", [left, right])
            
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
        
    def subtract(self, args):
        assert len(args) == 2
        left, right = args
        #print("subtract:")
        #print("  left:", left.pretty())
        #print("  righgt:", right.pretty())
        return Node("file_query", [Node("minus", [left, right])], meta={"limit":None})
        
    def qualified_name(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            out = Node("qualified_name", meta=[None, args[0].value])      # no namespace
        else:
            out = Node("qualified_name", meta=[args[0].value, args[1].value])
        #print("Converter.qualified_name: returning: %s" % (out.pretty(),))
        return out

    def dataset(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            return Node("dataset", [args[0], None])       # dataset without meta filter
        else:
            return Node("dataset", [args[0], args[1]])
            
    def filter(self, args):
        assert len(args) == 3
        #print("filter: args:", type(args[0]), args[0], type(args[1]), args[1], type(args[2]), args[2])
        query_list = args[2].C
        return Node("filter", query_list, meta = (args[0].value, args[1]))
        
    def filter_params(self, args):
        #print("filter_params:", args)
        return args
        
    def cmp_op(self, args):
        return Node(args[1].value, [args[0].value, args[2]])
        
    def in_op(self, args):
        return Node("in", [args[1].value, args[0]])

    def contains_op(self, args):
        return Node("in", [args[0].value, args[1]])
        
    def subscript_cmp_op(self, args):
        # ANAME "[" index "]" CMPOP constant 
        return Node("subscript_cmp", [args[0].value, args[1], args[2].value, args[3]])
        
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
        
    def present_op(self, args):
        assert len(args) == 1
        return Node("present", [args[0].value, None])

    def _apply_not(self, node):
        if node.T in ("meta_and", "meta_or") and len(node.C) == 1:
            return self._apply_not(node.C[0])
        if node.T == "meta_and":
            return Node("meta_or", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_or":
            return Node("meta_and", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_not":
            return node.C[0]
        elif node.T in CMP_OPS:
            new_op = {
                "~~":   "!~~",
                "!~~":  "~~",
                "~~*":   "!~~*",
                "!~~*":  "~~*",
                ">":    "<=",
                "<":    ">=",
                ">=":    "<",
                "<=":    ">",
                "=":    "!=",
                "==":    "!=",
                "!=":    "=="
            }[node.T]
            return Node(new_op, node.C)
        elif node.T == "present":
            assert len(node.C) == 1 or (len(node.C) == 2 and node.C[1] is None)
            return Node("not_present", node.C)
        elif node.T == "not_present":
            assert len(node.C) == 1 or (len(node.C) == 2 and node.C[1] is None)
            return Node("present", node.C)
        else:
            raise ValueError("Unknown node type %s while trying to apply NOT operation" % (node.T,))
            
    def meta_not(self, children):
        assert len(children) == 1
        return self._apply_not(children[0])
        
class _Assembler(Ascender):

    def __init__(self, db, default_namespace):
        Ascender.__init__(self)
        self.DB = db
        self.DefaultNamespace = default_namespace
        
    def walk(self, inp):
        #print("_Assembler.walk(): in:", inp.pretty() if isinstance(inp, Node) else repr(inp))
        out = Ascender.walk(self, inp)
        #print("_Assembler.walk(): out:", out.pretty() if isinstance(out, Node) else repr(out))
        return out
        
    def named_query(self, children, query_name):
        #print("_Assembler.named_query()")
        namespace, name = query_name
        namespace = namespace or self.DefaultNamespace
        parsed = MQLQuery.from_db(self.DB, namespace, name)
        assert parsed.Type == "file"
        tree = parsed.Tree
        tree = _ParamsApplier().walk(tree, {"namespace":namespace})
        #print("_Assembler.named_query: returning:", tree.pretty())
        return tree

class _ProvenancePusher(Descender):

    def parents_of(self, node, _):
        children = node.C
        assert len(children) == 1
        child = children[0]
        if isinstance(child, Node) and child.T == "union":
            return Node("union", [self.walk(Node("parents_of", [cc])) for cc in child.C])

    @pass_node
    def children_of(self, node, _):
        children = node.C
        assert len(children) == 1
        child = children[0]
        if isinstance(child, Node) and child.T == "union":
            return Node("union", [self.walk(Node("children_of", [cc])) for cc in child.C])

class _LimitApplier(Descender):
    
    def limit(self, node, limit):
        #print("_LimitPusher.limit: node:", node)
        assert len(node.C) == 1
        limit = node.M if limit is None else min(limit, node.M)
        return self.walk(node.C[0], limit)
        
    def file_query(self, node, limit):
        node_limit = node.M.get("limit")
        #print("_LimitApplier.file_query(): node_limit:", node_limit)
        if node_limit is not None:
            if limit is not None:
                limit = min(limit, node_limit)
            else:
                limit = node_limit
        node.C[0] = self.walk(node.C[0], limit)
        return node
        
    def union(self, node, limit):
        if limit is not None:
            return Node("limit", 
                [Node("union", 
                    [self.walk(c, limit) for c in node.C]
                    )
                ], meta=limit)
        else:
            return node
            
    def basic_file_query(self, node, limit):
        node.M.addLimit(limit)
        return node
        
    def _default(self, node, limit):
        #print("_LimitPusher._default: node:", node.pretty())
        if limit is not None:
            new_node = Node(node.T, node.C, node.M)
            self.visit_children(new_node, None)
            return Node("limit", [new_node], meta=limit)
        else:
            return self.visit_children(node, None)
            
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
        assert isinstance(node.M, BasicFileQuery)
        if meta_exp is not None:    
            node.M.addWhere(meta_exp)
            node.M.WithMeta = True
            #print("_MetaExpPusher.basic_file_query: added WithMeta=True")
        #print("_MetaExpPusher.DataSource: out: ", node.pretty())
        return node
        
    def children_of(self, node, meta_exp):
        if meta_exp is None:
            return self.visit_children(node, None)
        else:
            #
            # meta_filter node is created when we can not push the meta_exp down any further
            #
            return Node("meta_filter", [self.visit_children(node, None)], meta=meta_exp)
        
    parents_of = children_of
    
    def meta_filter(self, node, meta_exp):
        node_exp = node.M
        if node_exp is None:
            new_exp = meta_exp
        elif meta_exp is None:
            new_exp = node_exp
        else:
            new_exp = Node("meta_or", [Node("meta_and", [meta_exp, node_exp])])
        node.M = None
        return self.visit_children(node, new_exp)

class _DNFConverter(Descender):

# find all DataSource nodes and apply DNF converter to their Wheres

    def basic_file_query(self, node, _):
        #print("_DNFConverter.DataSource: node:", node, type(node))
        exp = node.M.Wheres
        if exp is not None:
            assert isinstance(exp, Node)
            exp = _MetaRegularizer().walk(exp)
            node.M.WheresDNF = _MetaRegularizer._make_DNF_lists(exp)
        
class _SQLGenerator(Ascender):

    @pass_node
    def basic_file_query(self, node):
        keep_meta = True
        return Node("SQL", meta=node.M.sql())

class _DatasetEvaluator(Ascender):
    
    def __init__(self, db, with_meta, limit):
        Ascender.__init__(self)
        self.DB = db
        self.WithMeta = with_meta
        self.Limit = limit
        
    def dataset_query(self, args, meta):
        return args[0]
    
    def datasets_selector(self, args, meta):
        assert isinstance(meta, DatasetSelector)
        out = limited(meta.datasets(self.DB, self.Limit), self.Limit)
        #print("_DatasetEvaluator.datasets_selector: out:", out)
        return out
        
class _FileEvaluator(Ascender):

    def __init__(self, db, filters, with_meta, limit):
        Ascender.__init__(self)
        self.Filters = filters
        self.DB = db
        self.WithMeta = with_meta
        self.Limit = limit
        
    def file_query(self, args, meta):
        limit = meta.get("limit")
        if limit is None:
            return args[0]
        else:
            return limited(args[0], limit)
        
    def meta_filter(self, args, meta):
        assert len(args) == 1
        #print("meta_filter: args:", args)
        files = args[0]
        meta_exp = meta
        #print("Evaluator.meta_filter: files:", files, "   meta_exp:", None if meta_exp is None else meta_exp.pretty())
        if meta_exp is not None:
            out = []
            for f in files:
                #print("_FileEvaluator.meta_filter: f:", f, f.Metadata)
                if self.evaluate_meta_expression(f, meta_exp):
                    out.append(f)
            return DBFileSet(self.DB, out)
            return DBFileSet(self.DB, (f for f in files if self.evaluate_meta_expression(f, meta_exp)))
        else:
            return files

    def parents_of(self, args, meta):
        assert len(args) == 1
        arg = args[0]
        #print("parents_of: arg:", arg)
        return arg.parents(with_metadata=True)

    def children_of(self, args, meta):
        assert len(args) == 1
        arg = args[0]
        #print("children_of: arg:", arg)
        return arg.children(with_metadata=True)

    def limit(self, args, meta):
        #print("FileEvaluator.limit(): args:", args)
        assert len(args) == 1 and isinstance(args[0], DBFileSet)
        if meta is not None:
            return args[0].limit(meta)
        else:
            return args[0]
            
    def basic_file_query(self, args, meta):
        assert isinstance(meta, BasicFileQuery)
        #print("_FileEvaluator:basic_file_query: q.WithMeta:", meta.WithMeta)
        return DBFileSet.from_basic_query(self.DB, meta, self.WithMeta or meta.WithMeta, self.Limit)
        
    def union(self, args, meta):
        #print("Evaluator.union: args:", args)
        return DBFileSet.union(self.DB, args)
        
    def join(self, args, meta):
        return DBFileSet.join(self.DB, args)
        
    def minus(self, expressions, meta):
        assert len(expressions) == 2
        left, right = expressions
        #print("minus:", left, right)
        return left - right

    def filter(self, args, meta):
        name, params = meta
        inputs = args
        #print("Evaluator.filter: inputs:", inputs)
        filter_function = self.Filters[name]
        return DBFileSet(self.DB, filter_function(inputs, params))
        
    def _eval_meta_bool(self, f, bool_op, parts):
        assert len(parts) > 0
        p0 = parts[0]
        rest = parts[1:]
        ok = self.evaluate_meta_expression(f, p0)
        if bool_op in ("and", "meta_and"):
            if len(rest) and ok:
                ok = self._eval_meta_bool(f, bool_op, rest)
            return ok
        elif bool_op in ("or", "meta_or"):
            if len(rest) and not ok:
                ok = self._eval_meta_bool(f, bool_op, rest)
            return ok
        elif bool_op == "not":
            assert len(rest) == 0
            return not ok
        else:
            raise ValueError("Unrecognized boolean operation '%s'" % (op,))
            
    BOOL_OPS = ("and", "or", "not")

    def evaluate_meta_expression(self, f, meta_expression):
        #print("evaluate_meta_expression: meta_expression:", meta_expression.pretty())
        op, args = meta_expression.T, meta_expression.C
        #print("evaluate_meta_expression:", op, args)
        if op in ("meta_and", "meta_or") and len(args) == 1:
            return self.evaluate_meta_expression(f, args[0])
        if op == "meta_and":    op = "and"
        if op == "meta_or":     op = "or"
        if op in self.BOOL_OPS:
            return self._eval_meta_bool(f, op, args)
        elif op == "present":
            return f.has_attribute(args[0])
        else:
            # 
            name, value = args
            attr_value = f.get_attribute(name, None)
            if attr_value is None:
                return False
            if op == "<":          return attr_value < value
            elif op == ">":    
                #print("evaluate_meta_expression: > :", attr_value, value)    
                return attr_value > value
            elif op == "<=":       return attr_value <= value
            elif op == ">=":       return attr_value >= value
            elif op in ("==",'='): 
                #print("evaluate_meta_expression:", repr(attr_value), repr(value))
                return attr_value == value
            elif op == "!=":       return attr_value != value
            elif op == "in":       return value in attr_value       # exception, e.g.   123 in event_list
            else:
                raise ValueError("Invalid comparison operator '%s' in %s" % (op, meta_expression))

    def meta_exp_to_sql(self, meta_expression):
        op, args = meta_expression.T, meta_expression.C
        if op in self.BOOL_OPS:
            bool_op = op
            exps = args
        else:
            bool_op = "and"
            
        if op in self.BOOL_OPS:
            if op in ('or','and'):
                sql_op = op
                return (' ' + sql_op + ' ').join([
                    '(' + self.meta_exp_to_sql(part) + ')' for part in args])
            elif op == 'not':
                return ' not (' + self.meta_exp_to_sql(args[1]) + ')'
            else:
                raise ValueError("Unrecognized boolean operation '%s'" % (op,))
        else:
            name, value = args
            if op in ('<', '>', '<=', '>=', '==', '=', '!='):
                sql_op = '=' if op == '==' else op
                if isinstance(value, bool): colname = "bool_value"
                elif isinstance(value, int): colname = "int_value"
                elif isinstance(value, float): colname = "float_value"
                elif isinstance(value, str): colname = "string_value"
                else:
                        raise ValueError("Unrecognized value type %s for attribute %s" % (type(value), name))
                return "attr.name='%s' and attr.%s %s '%s'" % (name, colname, sql_op, value)
            elif op == 'in':
                value, _, name = meta_expression
                if isinstance(value, bool): colname = "bool_array"
                elif isinstance(value, int): colname = "int_array"
                elif isinstance(value, float): colname = "float_array"
                elif isinstance(value, str): colname = "string_array"
                else:
                        raise ValueError("Unrecognized value type %s for attribute %s" % (type(value), name))
                return "attr.name='%s' and '%s' in attr.%s" % (name, value, colname)
            else:
                raise ValueError("Invalid comparison operator '%s' in %s" % (op, meta_expression))
        


def parse_query(text):
    # remove comments
    out = []
    for l in text.split("\n"):
        l = l.split('#', 1)[0]
        out.append(l)
    text = '\n'.join(out)
    
    parsed = _Parser.parse(text)
    print("parsed:---\n", parsed.pretty())
    return _Converter().convert(parsed)

class MQLQuery(object):
    
    @staticmethod
    def parse(text):
        out = []
        for l in text.split("\n"):
            l = l.split('#', 1)[0]
            out.append(l)
        text = '\n'.join(out)
    
        parsed = _Parser.parse(text)
        #print("parsed:---\n", parsed)
        return _Converter().convert(parsed)
        
    @staticmethod
    def from_db(db, namespace, name):
        q = DBNamedQuery.get(db, namespace, name)
        if q is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        text = q.Source
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
            try:    q = parse_query(qtext)
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
    
