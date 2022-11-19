from .trees import Node, pass_node, Ascender, Descender, Visitor, Converter, LarkToNodes

import pprint

CMP_OPS = [">" , "<" , ">=" , "<=" , "==" , "=" , "!=", "~~", "~~*", "!~~", "!~~*"]

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
    
def _merge_skip_limit(existing_skip, existing_limit, skip=0, limit=None):
    if existing_limit is None:
        return existing_skip+skip, limit
    elif limit is None:
        return existing_skip + skip, max(0, existing_limit - skip)
    else:
        return existing_skip + skip, max(0, min(existing_limit - skip, limit))

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
                " " + (self.Where.pretty() if self.Where is not None else ""))

    __str__ = line
    __repr__ = line
                
    def setWhere(self, where):
        self.Where = where
        
    def datasets(self, db, limit=None):
        return DBDataset.datasets_for_bdf(db, self, limit)
        
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
        return "BasicFileQuery(selectors:%s, limit:%s, skip:%s, %smeta, %sprovenance, %sordered)" % (self.DatasetSelectors, 
            self.Limit, self.Skip,
            "with " if self.WithMeta else "no ",
            "with " if self.WithProvenance else "no ",
            "" if self.Ordered else "not ",
            )

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
        self.Wheres = _make_DNF(wheres)
        #print("BasicFileQuery.addWhere() result:")
        #print(self.Wheres.pretty("    "))
            
    def addLimit(self, limit):
        self.add_skip_limit(0, limit)

    def addSkip(self, nskip):
        self.add_skip_limit(skip, None)
            
    def add_skip_limit(self, skip, limit):
        self.Skip, self.Limit = _merge_skip_limit(self.Skip, self.Limit, skip, limit)

    def apply_params(self, params):
        default_namespace = params.get("namespace")
        if self.DatasetSelectors:
            for ds in self.DatasetSelectors:
                ds.apply_params(params)

class QueryConverter(Converter):
    
    #
    # converts parsed query (eiher file or dataset) from Lark tree structure to my own Node
    #

    def __init__(self, db=None, default_namespace=None):
        self.DB = db
        self.DefaultNamespace = default_namespace

    def convert(self, tree):
        q = self.transform(tree)
        q.Parsed = tree
        return q
        
    def query(self, args):
        if len(args) == 2:
            params, query = args
            q = _WithParamsApplier().walk(query, params)
            #print("_Converter.query(): after applying params:", q.pretty())
        else:
            q = args[0]

        if q.T == "top_file_query":         out = FileQuery(q.C[0])
        elif q.T == "top_dataset_query":    out = DatasetQuery(q.C[0])
        else:
            raise ValueError("Unrecognozed top level node type: %s" % (q.T,))
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

    def make_ordered(self, node):
        if node.T in ("basic_file_query", "basic_dataset_query"):
            q = node["query"]
            q.Ordered = True
        elif node.T == "filter":
            node["ordered"] = True
        elif node.T in ("file_list", "skip_limit"):
            pass    # already fixed order
        else:
            node = Node("ordered", [node])
        return node

    def skip(self, args):
        assert len(args) == 2
        child, skip = args
        skip=int(skip)
        if skip == 0:   return child
        if child.T == "basic_file_query":
            q = child["query"]
            skip, limit = _merge_skip_limit(q.Skip, q.Limit, skip=skip)
            q.Skip = skip
            q.Limit = limit
            q.Ordered = True
            return child
        else:   
            return Node("skip_limit", [self.make_ordered(child)], skip=skip)

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
            q.Ordered = True
            return child
        else:
            return Node("skip_limit", [self.make_ordered(child)], limit=limit)
        
    def ordered(self, args):
        child = args[0]
        if child.T == "basic_file_query":
            print("Converter: ordered over bfq")
            child["query"].Ordered = True
            return child
        else:
            return Node("ordered", [child])

    def meta_filter(self, args):
        q, meta_exp = args
        meta_exp=_make_DNF(meta_exp)
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
                 with_meta=False, with_provenance=False, limit=None)

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
        assert v.type in ("STRING", "UNQUOTED_STRING")
        s = v.value
        if v.type == "STRING":
            if s[0] in ('"', "'"):
                s = s[1:-1]
        if '"' in s or "'" in s:        # sanitize
            raise ValueError("Unsafe string constant containing double or single quote: %s" % (repr(s),))
        return Node("string", value=s)

    def constant_list(self, args):
        return [n["value"] for n in args]

    def qualified_name(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            out = Node("qualified_name", namespace=self.DefaultNamespace, name=args[0].value)      # no namespace
        else:
            out = Node("qualified_name", namespace=args[0].value, name=args[1].value)
        #print("Converter.qualified_name: returning: %s" % (out.pretty(),))
        return out
        
    def named_query(self, args):
        if self.DB is None:
            raise RuntimeError("Can not load named query without access to the database")
        (q,) = args
        namespace = q["namespace"] or self.DefaultNamespace
        name = q["name"]
        parsed = MQLQuery.from_db(self.DB, namespace, name)

        loaded = DBNamedQuery.get(self.DB, namespace, name)
        if loaded is None:
            raise ValueError("Named query %s:%s not found" % (namespace, name))
        tree = MQLQuery.parse(loaded.Source, convert=False)
        print("named_query: parsed tree:", tree.pretty())
        converter = QueryConverter(self.DB, namespace)
        tree = converter(tree)
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
        node = Node("filter", queries, name = name.value, params=params, kv=kv, skip=0, limit=None, ordered=False)
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

    def json_path(self, args):
        node = Node("json_path", [args[0]], neg=False)

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
        elif node.T in ("cmp_op", "in_set", "in_range", "json_path"):   # why cmp_op is here ??? 
            node["neg"] = not node["neg"]
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
    
    def did_pattern(self, args):
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
        out = Node("did_pattern", namespace=namespace, name=name)
        return out

    def sql_pattern(self, args):
        assert len(args) in (1,2)
        namespace = self.DefaultNamespace
        if len(args) == 1:
            name = args[0].value
        else:
            namespace, name = args[0].value, args[1].value
        name = name.replace("*", "%").replace("?", "_")
        out = Node("did_pattern", namespace=namespace, name=name)
        return out

    def dataset_spec(self, args):
        name_or_pattern = args[-1]
        pattern = "matching" in args
        regexp = "regexp" in args
        assert name_or_pattern.T in ("did_pattern", "qualified_name")
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

    def add_subsets(self, children):
        assert len(children) == 2
        bdq, subsets = children
        q = bdq["query"]
        q.WithChildren = True
        q.Recursively = subsets["recursive"]
        return Node("basic_dataset_query", query=q)

    def dataset_provenance_op(self, children):
        return Node("subsets", recursive=any(c.value == "recursively" for c in children))

