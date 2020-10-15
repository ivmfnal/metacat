from metacat.db import DBDataset, DBFile, DBNamedQuery, DBFileSet, limited
from .trees import Node, pass_node, Ascender, Descender, Visitor, PostParser
import json, time

from lark import Lark
from lark import Transformer, Tree, Token
import pprint

CMP_OPS = [">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*"]

from .grammar10 import MQL_Grammar
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

    def meta_or(self, *children):
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

    def meta_and(self, *children):
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
            
def _make_DNF(exp, op=None, exp1=None):
    if op == "or":
        exp = Node("meta_or", [exp, exp1])
    elif op == "and":
        exp = Node("meta_and", [exp, exp1])

    return _MetaRegularizer().walk(exp)
            
class BasicFileQuery(object):
    
    def __init__(self, dataset_selector, where=None, relationship=None):
        #
        # Encapsulates query like:
        #   select files from datasets where wheres
        # or, if relationship == "parent"
        #   select parents of (files from datasets where wheres)
        # or, if relationship == "children"
        #   select children of (files from datasets where wheres)
        #
        
        self.Relationship = relationship
        self.DatasetSelector = dataset_selector
        self.Wheres = where 
        self.Limit = None
        self.WithMeta = False       # this is set to True if the query has "where" clouse
        
    def __str__(self):
        return "BasicFileQuery(selector:%s, limit:%s, rel:%s)" % (self.DatasetSelector, self.Limit, self.Relationship or "")
        
    def _pretty(self, indent="", headline_indent=None):
        #print(f"BasicFileQuery._pretty(indent='{indent}', headline_indent='{headline_indent}')")
        if headline_indent is None: headline_indent = indent
        lines = ["%s%s" % (headline_indent, self)]
        if self.Wheres is not None:
            lines += self.Wheres._pretty(indent + ". ", headline_indent = indent + ". where: ")
        return lines

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
        self.Wheres = _make_DNF(wheres)
        #print("BasicFileQuery.addWhere() result:")
        #print(self.Wheres.pretty("    "))
            
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
    __repr__ = line
                
    def setHaving(self, having):
        self.Having = having
        
    def datasets(self, db, limit=None):
        return DBDataset.apply_dataset_selector(db, self, limit)
        
    def apply_params(self, params):
        # apply params from "with ..."
        default_namespace = params.get("namespace")
        if default_namespace:
            for p in self.Patterns:
                p["namespace"] = p["namespace"] or default_namespace
        
class _ParamsApplier(Descender):
    
    # applies params from "with...", including default namespace

    def basic_file_query(self, node, params):
        bfq = node["query"]
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
        
    def optimize(self, debug=False):
        #print("Query.optimize: entry")
        assert self.Assembled is not None
        if self.Optimized is None:
            #print("Query.optimize: assembled:----\n", self.Assembled.pretty())
            
            optimized = self.Assembled
            
            #print("starting _MetaExpPusher...")
            optimized = _MetaExpPusher().walk(optimized, None)
            if debug:
                print("Query.optimize: after _MetaExpPusher:----")
                print(optimized.pretty("    "))
            
            optimized = _ProvenancePusher().walk(optimized, None)
            if debug:
                print("Query.optimize: after _ProvenancePusher:----")
                print(optimized.pretty("    "))

            self.Optimized = optimized
        return self.Optimized

    def run(self, db, filters={}, limit=None, with_meta=True, default_namespace=None, debug=False):
        #print("Query.run: DefaultNamespace:", self.DefaultNamespace)
        
        #print("assemble()...")
        self.assemble(db, default_namespace = default_namespace)
        #print("Query.run: assemled:", self.Assembled.pretty())
        
        #print("optimize()...")
        optimized = self.optimize(debug=debug)
        #print("Query.run: optimized: ----\n", optimized.pretty())
        
        if default_namespace is not None:
            optimized = _ParamsApplier().walk(optimized, {"namespace":default_namespace})
        
        #print("starting _LimitApplier...")
        optimized = _LimitApplier().walk(optimized, limit)
        #print("Limit %s applied: ----\n" % (limit,), optimized.pretty())
        
        out = _FileEvaluator(db, filters, with_meta, None).walk(optimized)
        #print ("run: out:", out)
        return out
        
class _Converter(Transformer):
    
    def query(self, args):
        if len(args) == 2:
            params, query = args
            #print("_Converter.query(): applying params:", params)
            q = _ParamsApplier().walk(query, params)
            #print("_Converter.query(): after applying params:", q.pretty())
        else:
            q = args[0]
        
        #print("_Converter.query(): q=", q.pretty())
        
        if q.T == "top_file_query": out = FileQuery(q.C[0])
        elif q.T == "top_dataset_query": out = DatasetQuery(q.C[0])
        else:
            raise ValueError("Unrecognozed top level node type: %s" % (q.T,))
        #print("_Converter.query() -> ", out)
        return out
    
    def convert(self, tree):
        return self.transform(tree)
        
    def __default__(self, typ, children, meta):
        return Node(typ, children, _meta=meta)

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
        return Node("limit", [args[0]], limit = int(args[1].value))
        #return Node("file_query", [args[0]], meta = {"limit":int(args[1].value)})

    def meta_filter(self, args):
        q, meta_exp = args
        return Node("meta_filter", [q, _make_DNF(meta_exp)])
        
    def datasets_selector(self, args):
        spec_list = args[0]["specs"]
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
        ds = DatasetSelector(spec_list, with_children, recursively, having)
        return Node("datasets_selector", selector = ds)

    def basic_file_query(self, args):
        assert len(args) == 0 or len(args) == 1 and args[0].T == "datasets_selector"
        dataset_selector = args[0]["selector"] if args else None
        return Node("basic_file_query", query=BasicFileQuery(dataset_selector))

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
        
    def string_constant(self, args):
        v = args[0]
        s = v.value
        if s[0] in ('"', "'"):
            s = s[1:-1]
        return Node("string", value=s)
        
    def constant_list(self, args):
        return [n["value"] for n in args]
        
    def dataset_spec(self, args):
        (x,) = args
        return Node("dataet_spec", wildcard = (x.T == "dataset_pattern"), **x.D)
            
    def dataset_spec_list(self, args):
        return Node("dataset_spec_list", specs=[a.D for a in args])
        
    def _____dataset(self, args):
        return 

    def dataset_pattern(self, args):
        # either (name_pattern) or (namespace_pattern, name_pattern)
        if len(args) == 1:
            name_pattern = args[0].value
            namespace = None
        else:
            name_pattern = args[1].value
            namespace = args[0].value
        return Node("dataset_pattern", name_pattern=name_pattern, namespace=namespace)
        
    def named_query(self, args):
        (q,) = args
        out = Node("named_query", **q.D)       # value = (namespace, name) - tuple
        #print("Converter.named_query(%s): returning %s" % (args, out))
        return out
        
    def param_def_list(self, args):
        return dict([(a.C[0].value, a.C[1]["value"]) for a in args])
        
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
        
    def ___subtract(self, args):
        assert len(args) == 2
        left, right = args
        #print("subtract:")
        #print("  left:", left.pretty())
        #print("  righgt:", right.pretty())
        #return Node("file_query", [Node("minus", [left, right])], meta={"limit":None})
        return Node("minus", [left, right])
        
    def qualified_name(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            out = Node("qualified_name", namespace=None, name=args[0].value)      # no namespace
        else:
            out = Node("qualified_name", namespace=args[0].value, name=args[1].value)
        #print("Converter.qualified_name: returning: %s" % (out.pretty(),))
        return out
        
    def ___parents_of(self, args):
        return self.parentage(args, "parents_of")
        
    def ___children_of(self, args):
        return self.parentage(args, "children_of")
        
    def parentage(self, args, relationship):
        assert len(args) == 1
        c = args[0]
        if c.T == "basic_file_query":
            q = c["query"]
            if q.Relationship is None:
                q.Relationship = relationship
                return c
        return Node(relationship, args)

    def filter(self, args):
        name, params, queries = args
        return Node("filter", queries = query_list.C, name = name.value, args=params.C)
        
    def scalar(self, args):
        (t,) = args
        return Node("scalar", name=t.value)
    
    def array_any(self, args):
        (n,) = args
        return Node("array_any", name=n.value)
        
    def array_length(self, args):
        (n,) = args
        return Node("array_length", name=n.value)
        
    def array_subscript(self, args):
        name, inx = args
        return Node("array_subscript", name=name.value, index=inx)

    def cmp_op(self, args):
        return Node("cmp_op", [args[0], args[2]], op=args[1].value)
        
    def in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float") and args[2].T in ("string", "int", "float")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        return Node("in_range", [args[0]], type=args[1].T, low=args[1]["value"], high=args[2]["value"])
        
    def in_set(self, args):
        assert len(args) == 2
        return Node("in_set", [args[0]], set=args[1])
        
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
        return Node("present", name = args[0].value)

    def _apply_not(self, node):
        if node.T in ("meta_and", "meta_or") and len(node.C) == 1:
            return self._apply_not(node.C[0])
        if node.T == "meta_and":
            return Node("meta_or", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_or":
            return Node("meta_and", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_not":
            return node.C[0]
        elif node.T == "cmp_op":
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
            }[node["op"]]
            return Node("cmp_op", node.C, op=new_op)
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
        
    def named_query(self, *children, name=None, namespace=None):
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

    @pass_node
    def parents_of(self, node, _):
        return self.parentage(node, "parents_of")
        
    @pass_node
    def children_of(self, node, _):
        return self.parentage(node, "children_of")
        
    def parentage(self, node, relationship):
        children = node.C
        assert len(children) == 1
        child = children[0]
        if isinstance(child, Node):
            if child.T == "union":
                n = Node("union", [self.walk(Node(relationship, [cc])) for cc in child.C])
                return self.visit_children(n, None)
            elif child.T == "basic_file_query":
                bfq = child["query"]
                #print(f"_ProvenancePusher.parentage({relationship}): bfq:", bfq)
                if bfq.Relationship is None:
                    bfq.Relationship = relationship
                    return child
                else:
                    return None         # can not apply parentage: return same BFQ node without changes

class _LimitApplier(Descender):
    
    def limit(self, node, limit):
        #print("_LimitPusher.limit: node:", node)
        assert len(node.C) == 1
        node_limit = node["limit"]
        limit = node_limit if limit is None else min(limit,node_limit)
        return self.walk(node.C[0], limit)
        
    def union(self, node, limit):
        if limit is not None:
            return Node("limit", 
                [Node("union", 
                    [self.walk(c, limit) for c in node.C]
                    )
                ], limit=limit)
        else:
            return node
            
    def basic_file_query(self, node, limit):
        #print("LimitApplier: applying limit", limit)
        node["query"].addLimit(limit)
        return node
        
    def _default(self, node, limit):
        #print("_LimitApplier._default: node:", node.pretty())
        if limit is not None:
            new_node = Node(node.T, node.C, node.M)
            self.visit_children(new_node, None)
            return Node("limit", [new_node], limit=limit)
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
        if meta_exp is not None:    
            bfq = node["query"]
            assert isinstance(bfq, BasicFileQuery)
            assert bfq.Relationship is None             # this will be added by ProvenancePusher, as the next step of the optimization
            bfq.addWhere(meta_exp)
            bfq.WithMeta = True
        return node
        
    def children_of(self, node, meta_exp):
        if meta_exp is None:
            return self.visit_children(node, None)
        else:
            #
            # meta_filter node is created when we can not push the meta_exp down any further
            #
            return Node("meta_filter", [self.visit_children(node, None), meta_exp])
        
    parents_of = children_of
    
    def meta_filter(self, node, meta_exp):
        assert len(node.C) == 2
        child, node_exp = node.C
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
        
    def dataset_query(self, datasets_selector):
        return dataset_selector
    
    def datasets_selector(self, *args, selector = None):
        assert isinstance(selector, DatasetSelector)
        out = limited(selector.datasets(self.DB, self.Limit), self.Limit)
        #print("_DatasetEvaluator.datasets_selector: out:", out)
        return out
        
class _FileEvaluator(Ascender):

    def __init__(self, db, filters, with_meta, limit):
        Ascender.__init__(self)
        self.Filters = filters
        self.DB = db
        self.WithMeta = with_meta
        self.Limit = limit
        
    def file_query(self, query, limit=None):
        return query if limit is None else limited(query, limit)
        
    def meta_filter(self, files, meta_exp):
        #print("meta_filter: args:", args)
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

    def parents_of(self, files):
        return files.parents(with_metadata=True)

    def children_of(self, files,):
        return files.children(with_metadata=True)

    def limit(self, files, limit=None):
        #print("FileEvaluator.limit(): args:", args)
        assert isinstance(files, DBFileSet)
        return files if limit is None else files.limit(limit)
            
    def basic_file_query(self, *args, query=None):
        assert isinstance(query, BasicFileQuery)
        #print("_FileEvaluator:basic_file_query: q.WithMeta:", meta.WithMeta)
        return DBFileSet.from_basic_query(self.DB, query, self.WithMeta or query.WithMeta, self.Limit)
        
    def union(self, *args):
        #print("Evaluator.union: args:", args)
        return DBFileSet.union(self.DB, args)
        
    def join(self, *args):
        return DBFileSet.join(self.DB, args)
        
    def minus(self, left, right):
        assert isinstance(left, DBFileSet)
        assert isinstance(right, DBFileSet)
        return left - right

    def filter(self, *inputs, name=None, params=[]):
        #print("Evaluator.filter: inputs:", inputs)
        assert name is not None
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
    
    @staticmethod
    def do_cmp_op(x, op, y):
        if op == "<":          return x < y
        elif op == ">":    
            #print("evaluate_meta_expression: > :", attr_value, value)    
            return x > y
        elif op == "<=":       return x <= y
        elif op == ">=":       return x >= y
        elif op in ("==",'='): 
            #print("evaluate_meta_expression:", repr(attr_value), repr(value))
            return x == y
        elif op == "!=":       return x != y
        # - fix elif op == "in":       return value in attr_value       # exception, e.g.   123 in event_list
        else:
            raise ValueError("Invalid comparison operator '%s'" % (op,))
        
    BOOL_OPS = ("and", "or", "not")

    def evaluate_meta_expression(self, f, meta_expression):
        #print("evaluate_meta_expression: meta_expression:", meta_expression.pretty())
        metadata = f.metadata()
        #print("    meta:", metadata)
        op, args = meta_expression.T, meta_expression.C
        #print("evaluate_meta_expression:", op, args)
        if op in ("meta_and", "meta_or") and len(args) == 1:
            return self.evaluate_meta_expression(f, args[0])
        if op == "meta_and":    op = "and"
        if op == "meta_or":     op = "or"
        if op in self.BOOL_OPS:
            return self._eval_meta_bool(f, op, args)
        elif op == "present":
            return args[0] in metadata
        elif op == "in_set":
            left, right = args
            vset = set(list(right))
            if left.T == "scalar":
                aname = left["name"]
                return aname in metadata and metadata[aname] in vset
            elif left.T == "array_any":
                aname = left["name"]
                if not aname in metadata:  return False
                lst = metadata[aname]
                if not isinstance(lst, list):   return False
                for x in lst:
                    if x in vset:  return True
                else:
                    return False
            elif left.T == "array_subscript":
                aname = left["name"]
                inx = left["index"]
                if not aname in metadata:  return False
                lst = metadata[aname]
                try:    v = lst[inx]
                except: return False
                return v in vset
        elif op == "in_range":
            left, right = args
            low, high = right["low"], right["high"]
            if left.T == "scalar":
                aname = left["name"]
                try:    return aname in metadata and metadata[aname] >= low and metadata[aname] <= high
                except: return False
            elif left.T == "array_any":
                aname = left["name"]
                if not aname in f:  return False
                lst = metadata[aname]
                if isinstance(lst, dict):
                    attr_values = lst.values()
                elif isinstance(lst, list):
                    attr_values = lst
                else:
                    return False
                for x in attr_values:
                    if x >= low and x <= high:  return True
                else:
                    return False
            elif left.T == "array_subscript":
                aname = left["name"]
                inx = left["index"]
                if not aname in metadata:  return False
                lst = metadata[aname]
                try:    v = lst[inx]
                except: return False
                return v >= low and v <= high                    
        elif op == "cmp_op":
            cmp_op = meta_expression["op"]
            left, right = args
            #print("cmp_op: left:", left.pretty())
            value = right["value"]
            if left.T == "scalar":
                aname = left["name"]
                try:    
                    result = aname in metadata and self.do_cmp_op(metadata[aname], cmp_op, value)
                    #print("result:", result)
                    return result
                except: return False
            elif left.T == "array_any":
                aname = left["name"]
                lst = metadata.get(aname)
                #print("lst:", lst)
                if lst is None:  return False
                if isinstance(lst, dict):
                    attr_values = lst.values()
                elif isinstance(lst, list):
                    attr_values = lst
                else:
                    return False
                for av in attr_values:
                    #print("comparing", av, cmp_op, value)
                    if self.do_cmp_op(av, cmp_op, value):
                        return True
                else:
                    return False
            elif left.T == "array_subscript":
                aname = left["name"]
                inx = left["index"]
                lst = metadata.get(aname)
                if lst is None:  return False
                try:    av = lst[inx]
                except: return False
                return  self.do_cmp_op(av, cmp_op, value)                
        raise ValueError("Invalid expression:\n"+meta_expression.pretty())

def parse_query(text, debug=False):
    # remove comments
    out = []
    for l in text.split("\n"):
        l = l.split('#', 1)[0]
        out.append(l)
    text = '\n'.join(out)
    
    parsed = _Parser.parse(text)
    if debug:
        print("--- parsed ---\n", PostParser().transform(parsed).pretty())
    converted = _Converter().convert(parsed)
    if debug:
        print("--- converted ---\n", converted)
    return converted

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
    
