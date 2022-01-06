from metacat.db import DBDataset, DBFile, DBNamedQuery, DBFileSet, limited
from .trees import Node, pass_node, Ascender, Descender, Visitor, Converter
from .sql_converter import SQLConverter
from .meta_evaluator import MetaEvaluator
import json, time

from lark import Lark
from lark import Tree, Token
import pprint

CMP_OPS = [">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*"]

from .grammar import MQL_Grammar
_Parser = Lark(MQL_Grammar, start="query")

class SyntaxError(Exception):
    
    def __init__(self, message):
        self.Message = messge
        
    def __str__(self):
        return f"MQL Syntax Error: {self.Message}"
        
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

    def meta_or(self, node, *children):
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

    def meta_and(self, node, *children):
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

class DatasetSelector(object):

    def __init__(self, namespace, name, pattern, with_children, recursively, having):
        self.Namespace = namespace
        self.Name = name
        self.Pattern = pattern
        self.WithChildren = with_children
        self.Recursively = recursively
        self.Having = having
        
    def is_explicit(self):
        return not self.Pattern and not self.WithChildren

    def line(self):
        return "DatasetSelector(%s:%s%s%s%s%s)" % (
                self.Namespace, self.Name, 
                "[*]" if self.Pattern else "[]",
                " with children" if self.WithChildren else "",
                " recursively" if self.Recursively else "",
                " " + self.Having if self.Having else "")

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
            self.Namespace = self.Namespace or default_namespace
                
    def filter_by_having(self, datasets):
        if self.Having is None:
            yield from datasets
        evaluator = MetaEvaluator()
        for ds in datasets:
            if evaluator(ds.Metadata, self.Having):
                yield ds
                
class BasicFileQuery(object):
    
    def __init__(self, dataset_selectors, where=None):
        assert all(isinstance(ds, DatasetSelector) for ds in dataset_selectors)
        self.DatasetSelectors = dataset_selectors
        self.Wheres = where
        self.Limit = None
        self.WithMeta = False       
        self.WithProvenance = False
        self.Skip = 0
        
    def __str__(self):
        return "BasicFileQuery(selectors:%s, limit:%s, skip:%s, %smeta, %sprovenance)" % (self.DatasetSelectors, 
            self.Limit, self.Skip,
            "+" if self.WithMeta else "-",
            "+" if self.WithProvenance else "-",
            )
        
    def _pretty(self, indent="", headline_indent=None):
        #print(f"BasicFileQuery._pretty(indent='{indent}', headline_indent='{headline_indent}')")
        if headline_indent is None: headline_indent = indent
        lines = ["%s%s" % (headline_indent, self)]
        if self.Wheres is not None:
            lines += self.Wheres._pretty(indent + "| ", headline_indent = indent + "| where=")
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
        if limit is not None:
            if self.Limit is None:   self.Limit = limit
            else:   self.Limit = min(self.Limit, limit)

    def addSkip(self, nskip):
        self.Skip += nskip
        if self.Limit is not None:
            self.Limit = max(0, self.Limit-nskip)

    def apply_params(self, params):
        default_namespace = params.get("namespace")
        for ds in self.DatasetSelectors:
            ds.apply_params(params)
        
class DatasetQuery(object):
    
    Type = "dataset"
    
    def __init__(self, tree):
        self.Tree = tree

    def run(self, db, limit=None, with_meta=True, with_provenance=True, filters={}, default_namespace=None, debug=False):
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
        
    def optimize(self, debug=False, default_namespace=None, skip=0, limit=None):
        #print("Query.optimize: entry")
        assert self.Assembled is not None
        if self.Optimized is None:
            #print("Query.optimize: assembled:----\n", self.Assembled.pretty())
            
            optimized = self.Assembled
            
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
            
            optimized = _SkipLimitApplier().walk(optimized, skip, limit)
            if debug:
                print("Query.optimize: after 2nd _SkipLimitApplier:----")
                print(optimized.pretty("    "))
            
            if default_namespace is not None:
                optimized = _WithParamsApplier().walk(optimized, {"namespace":default_namespace})
                
                
            self.Optimized = optimized
        return self.Optimized

    def run(self, db, filters={}, skip=0, limit=None, with_meta=True, with_provenance=True, default_namespace=None, debug=False):
        #print("Query.run: DefaultNamespace:", self.DefaultNamespace)
        
        #print("assemble()...")
        self.assemble(db, default_namespace = default_namespace)
        #print("Query.run: assemled:", self.Assembled.pretty())
        
        #print("optimize()...")
        optimized = self.optimize(debug=debug, default_namespace=default_namespace, skip=skip, limit=limit)
        #print("Query.run: optimized: ----\n", optimized.pretty())
        
        optimized = _QueryOptionsApplier().walk(optimized, 
            dict(
                with_provenance = with_provenance,
                with_meta = with_meta
        ))
        if debug:
            print("after _QueryOptionsApplier:", optimized.pretty())
        #print("Limit %s applied: ----\n" % (limit,), optimized.pretty())
        
        #out = _FileEvaluator(db, filters, with_meta, None).walk(optimized)
        #print ("run: out:", out)
        #print("FileQuery: with_meta:", with_meta)
        out = SQLConverter(db, filters, debug=debug).convert(optimized)
        
        if debug:
            print("Query:\n%s" % (optimized.pretty(),))
            #print("SQL:\n%s" % (out.SQL,))

        return out

class QueryConverter(Converter):
    
    #
    # converts parsed query (eiher file or dataset) from Lark tree structure to my own Node
    #
    
    def query(self, args):
        if len(args) == 2:
            params, query = args
            q = _WithParamsApplier().walk(query, params)
            #print("_Converter.query(): after applying params:", q.pretty())
        else:
            q = args[0]
        
        
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
        child, limit = args
        limit = int(limit)
        if child.T == "filter":
            child["limit"] = limit
        return Node("limit", [child], limit = limit)
        #return Node("file_query", [args[0]], meta = {"limit":int(args[1].value)})
        
    def limit(self, args):
        assert len(args) == 2
        child = args[0]
        limit = int(args[1])
        return Node("limit", [args[0]], limit=limit)

    def skip(self, args):
        assert len(args) == 2
        skip=int(args[1])
        if skip == 0:   return args[0]
        else:   return Node("skip", [args[0]], skip=skip)

    def meta_filter(self, args):
        q, meta_exp = args
        return Node("meta_filter", query=q, meta_exp=_make_DNF(meta_exp))
                
    def basic_file_query(self, args):
        assert len(args) == 1, str(args)
        assert args[0].T == "dataset_selector_list"
        return Node("basic_file_query", query=BasicFileQuery(args[0]["selectors"]))
        
    def file_list(self, args):
        return Node("file_list", specs=[a.value[1:-1] for a in args], with_meta=False, with_provenance=False, limit=None)

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
        
    def dataset_selector(self, args):
        name_or_pattern = args[0]
        assert name_or_pattern.T in ("dataset_pattern", "qualified_name")
        pattern = name_or_pattern.T == "dataset_pattern"
        name = name_or_pattern["name"]
        namespace = name_or_pattern["namespace"]
        having_exp = None
        with_children = False
        recursively = False
        args_ = args[1:]
        i = 0
        while i < len(args_):
            #print(i, a)
            a = args_[i]
            if a.value == "with":
                pass
            elif a.value == "children":
                with_children = True
            elif a.value == "recursively":
                recursively = True
            elif a.value == "having":
                having_exp = args_[i+1]
                i += 1
            i += 1
        selector = DatasetSelector(namespace, name, pattern, with_children, recursively, having_exp)
        return Node("dataset_selector", selector=selector)
        
    def dataset_selector_list(self, args):
        return Node("dataset_selector_list", selectors=[ds["selector"] for ds in args])

    def qualified_name(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            out = Node("qualified_name", namespace=None, name=args[0].value)      # no namespace
        else:
            out = Node("qualified_name", namespace=args[0].value, name=args[1].value)
        #print("Converter.qualified_name: returning: %s" % (out.pretty(),))
        return out
        
    def dataset_pattern(self, args):
        assert len(args) in (1,2)
        namespace = None
        if len(args) == 1:
            name = args[0].value
        else:
            namespace, name = args[0].value, args[1].value
        # unquote the string
        if name.startswith("'") or name.startswith('"'):
            name = name[1:-1]
        #print("Converter.qualified_name: returning: %s" % (out.pretty(),))
        out = Node("dataset_pattern", namespace=namespace, name=name)
        return out
        
    def named_query(self, args):
        (q,) = args
        out = Node("named_query", **q.D)        # copy namespace, name from the qualified name
        return out

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
        
    def ___subtract(self, args):
        assert len(args) == 2
        left, right = args
        #print("subtract:")
        #print("  left:", left.pretty())
        #print("  righgt:", right.pretty())
        #return Node("file_query", [Node("minus", [left, right])], meta={"limit":None})
        return Node("minus", [left, right])
        
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
        node = Node("filter", queries, name = name.value, params=params, kv=kv)
        #print("filter created:", node.pretty())
        return node
        
    def scalar(self, args):
        (t,) = args
        return Node("scalar", name=t.value)

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
            node["neg"] = not node["neg"]
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
        
    def array_subscript(self, args):
        name, inx = args
        if inx.type == "STRING":
            inx = inx.value[1:-1]
        else:
            inx = int(inx.value)
        return Node("array_subscript", name=name.value, index=inx)

    def cmp_op(self, args):
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
        
    def in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float") and args[2].T in ("string", "int", "float")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        return self._convert_array_all(Node("in_range", [args[0]], low=args[1]["value"], high=args[2]["value"], neg=False, type=args[1].T))
    
    def not_in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float") and args[2].T in ("string", "int", "float")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        return self._convert_array_all(Node("in_range", [args[0]], low=args[1]["value"], high=args[2]["value"], neg=True, type=args[1].T))

    def in_set(self, args):
        assert len(args) == 2
        return self._convert_array_all(Node("in_set", [args[0]], neg=False, set=args[1]))
        
    def not_in_set(self, args):
        assert len(args) == 2
        return self._convert_array_all(Node("in_set", [args[0]], neg=True, set=args[1]))
        
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
        elif node.T in ("cmp_op", "in_set", "in_range"):
            node["neg"] = not node["neg"]
            return node
        elif node.T == "cmp_op":
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

class _Assembler(Ascender):

    def __init__(self, db, default_namespace):
        Ascender.__init__(self)
        self.DB = db
        self.DefaultNamespace = default_namespace
        
    def ____walk(self, inp, debug=False):
        #print("_Assembler.walk(): in:", inp.pretty() if isinstance(inp, Node) else repr(inp))
        out = Ascender.walk(self, inp, debug)
        #print("_Assembler.walk(): out:", out.pretty() if isinstance(out, Node) else repr(out))
        return out
        
    def named_query(self, node, name=None, namespace=None):
        namespace = namespace or self.DefaultNamespace
        parsed = MQLQuery.from_db(self.DB, namespace, name)
        assert parsed.Type == "file"
        tree = parsed.Tree
        tree = _WithParamsApplier().walk(tree, {"namespace":namespace})
        #print("_Assembler.named_query: returning:", tree.pretty())
        return tree

class _WithParamsApplier(Descender):
    
    # applies params from "with...", including default namespace
    
    def file_list(self, node, params):
        namespace = params.get("namespace")
        if not namespace:
            return node
        new_specs = []
        for s in node["specs"]:
            parts = s.split(":",1)
            if len(parts) == 2 and not parts[0]:
                s = namespace + ":" + parts[1]
            new_specs.append(s)
        node["specs"] = new_specs

    def basic_file_query(self, node, params):
        bfq = node["query"]
        assert isinstance(bfq, BasicFileQuery)
        bfq.apply_params(params)
        return node

    def dataset_selector(self, node, params):
        #print("_ParamsApplier.dataset_selector: applying params:", params)
        selector = meta
        assert isinstance(selector, DatasetSelector)
        selector.apply_params(params)
        return node
        
    def named_query(self, node, params):
        #print("_ParamsApplier:named_query %s %s" % (node, params))
        if params is not None:
            assert isinstance(params, dict)
            if node["namespace"] is None:
                node["namespace"] = params.get("namespace")
        return self.visit_children(node, params)

    def qualified_name(self, node, params):
        if params is not None:
            assert isinstance(params, dict)
            if node["namespace"] is None:   node["namespace"] = params.get("namespace")
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
    
    def combine_limits(self, l1, l2):
        if l1 is None:  return l2
        if l2 is None:  return l1
        return min(l1, l2)

    def skip(self, node, skip_limit):
        skip, limit = skip_limit
        node_skip = node.get("skip", 0)
        return self.walk(node.C[0], node_skip + skip, limit)

    def limit(self, node, skip_limit):
        skip, limit = skip_limit
        node_limit = node.get("limit")
        
        if node_limit is not None:
            node_limit = node_limit - skip
            if node_limit <= 0:
                return Node("empty")
            if limit is not None:
                limit = min(node_limit, limit)
            else:
                limit = node_limit
        return self.walk(node.C[0], skip, limit)
        
    def basic_file_query(self, node, skip_limit):
        query = node["query"]
        #print("_SkipLimitApplier: applying skip_limit", skip_limit, " to BFQ:", query)
        skip, limit = skip_limit
        query.addSkip(skip)
        query.addLimit(limit)
        return node

    def union(self, node, skip_limit):
        #print("_SkipLimitApplier: skip_limit:", skip_limit, "  children:", node.C)
        skip, limit = skip_limit
        node = Node("union", [self.walk(c) for c in node.C])
        return self._default(node, skip_limit)

    def filter(self, node, skip_limit):
        skip, limit = skip_limit
        node["limit"] = limit
        node["skip"] = skip
        node.C = [self.walk(c) for c in node.C]
        return node

    def file_list(self, node, skip_limit):
        skip, limit = skip_limit
        node["limit"] = limit
        node["skip"] = skip
        return node
    
    def empty(self, node, skip_limit):
        return node

    def _default(self, node, skip_limit):
        #print("_LimitApplier._default: node:", node.pretty())
        skip, limit = skip_limit
        if skip:
            node = Node("skip", [self.walk(node)], skip=skip)
        if limit:
            node = Node("limit", [self.walk(node)], limit=limit)
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
            
class _____QueryLimitApplier(Descender):
    
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
        
    def filter(self, node, limit):
        node["limit"] = limit
        return Node("limit", [node], limit=limit)

    def file_list(self, node, limit):
        node["limit"] = limit
        return node
        
    def _default(self, node, limit):
        #print("_LimitApplier._default: node:", node.pretty())
        if limit is not None:
            return Node("limit", [node], limit=limit)
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
        new_params = params.copy()
        new_params["with_provenance"] = True
        new_params["with_meta"] = True
        node["with_provenance"] = params.get("with_provenance", False)
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
        
    def dataset_query(self, node, dataset_selector):
        return dataset_selector
    
    def datasets_selector(self, node, *args, selector = None):
        assert isinstance(selector, DatasetSelector)
        evaluator = MetaEvaluator()
        out = limited(
            (x for x in selector.datasets(self.DB, self.Limit)
                if selector.Having is None or evaluator(x.Metadata, selector.Having)
            ), 
            self.Limit
        )
        #print("_DatasetEvaluator.datasets_selector: out:", out)
        return out
        
def parse_query(text, debug=False):
    # remove comments
    out = []
    for l in text.split("\n"):
        l = l.split('#', 1)[0]
        out.append(l)
    text = '\n'.join(out)
    
    parsed = _Parser.parse(text)

    if debug:
        print("--- parsed ---\n", LarkToNodes()(parsed).pretty())
    converted = QueryConverter()(parsed)
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
        return QueryConverter().convert(parsed)
        
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
    
