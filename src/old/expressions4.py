from dbobjects import DBDataset, DBFile, DBNamedQuery, DBFileSet
import json, time

from lark import Lark
from lark import Transformer, Tree, Token
from lark.visitors import Interpreter
import pprint

meta_to_sql = False


grammar = """
exp:  add_exp                                   -> f_

add_exp : add_exp "+" mul_exp                   -> add
        | add_exp "-" mul_exp                   -> subtract
        | mul_exp                               -> f_
    
mul_exp : mul_exp "*" term_with_params          -> mult
        | term_with_params                      -> f_
    
term_with_params    : with_clause term2         
                    | term2                     -> f_
        
with_clause :  "with" param_def ("," param_def)*

param_def: CNAME "=" constant

term2   : term                                      -> f_
        | filterable_term "where" meta_exp          -> metafilter_exp

?term   : dataset_exp                               -> f_
        | filterable_term                           -> f_
    
?filterable_term: union                                    -> f_
    | join                                      -> f_
    | "filter" CNAME "(" filter_params ")" "(" exp_list ")"         -> filter
    | "parents" "(" exp ")"                     -> parents_of
    | "children" "(" exp ")"                    -> children_of
    | "query" namespace_name                    -> named_query
    | "(" exp ")"                               -> f_

union: "union" "(" exp_list ")"
    | "[" exp_list "]"

join: "join" "(" exp_list ")"
    | "{" exp_list "}"

exp_list: exp ("," exp)*                             

filter_params:    ( constant ("," constant)* )?                    -> filter_params

dataset_exp: "dataset" namespace_name ("where" meta_exp)?           -> dataset

namespace_name: (CNAME ":")? CNAME

?meta_exp:   and_meta                                
    | meta_exp "or" and_meta                        -> meta_or
    
?and_meta:   term_meta                               
    | and_meta "and" term_meta                      -> meta_and
    
term_meta:  CNAME CMPOP constant                    -> cmp_op
    | constant "in" CNAME                           -> in_op
    | "(" meta_exp ")"                              -> f_
    | "!" term_meta                                 -> meta_not
    
constant : SIGNED_FLOAT                             -> float_constant                      
    | STRING                                        -> string_constant
    | SIGNED_INT                                    -> int_constant
    | BOOL                                          -> bool_constant

CMPOP: ">" | "<" | ">=" | "<=" | "==" | "=" | "!="

BOOL: "true" | "false"                              


%import common.CNAME
%import common.SIGNED_INT
%import common.SIGNED_FLOAT
%import common.ESCAPED_STRING       -> STRING
%import common.WS
%ignore WS
"""

class Node(object):
    def __init__(self, typ, children=[], value=None):
        self.T = typ
        self.V = value
        self.C = children[:]

    def __str__(self):
        return "<Node %s v=%s c:%d>" % (self.T, self.V, len(self.C))

    __repr__ = __str__

    def __add__(self, lst):
        return Node(self.T, self.C + lst, self.V)

    def as_list(self):
        out = [self.T, self.V]
        for c in self.C:
                if isinstance(c, Node):
                        out.append(c.as_list())
                else:
                        out.append(c)
        return out
        
    def _pretty(self, indent=0):
        out = []
        out.append("%s%s %s" % (" "*indent, self.T, '' if self.V is None else repr(self.V)))
        for c in self.C:
            if isinstance(c, Node):
                out += c._pretty(indent+2)
            else:
                out.append("%s%s" % (" "*(indent+2), repr(c)))
        return out
        
    def pretty(self):
        return "\n".join(self._pretty())
        
    def jsonable(self):
        d = dict(T=self.T, V=self.V, C=[c.jsonable() if isinstance(c, Node) else c
                        for c in self.C]
        )
        d["///class///"] = "node"
        return d
        
    def to_json(self):
        return json.dumps(self.jsonable())

    @staticmethod
    def from_jsonable(data):
        if isinstance(data, dict) and data.get("///class///") == "node":
            return Node(data["T"],
                children = [Node.from_jsonable(c) for c in data.get("C", [])],
                value = data.get("V")
            )
        else:
            return data

    @staticmethod
    def from_json(text):
        return Node.from_jsonable(json.loads(text))

class Ascender(object):

    def __init__(self):
        self.Indent = ""

    def walk(self, node):
        if not isinstance(node, Node):
            return node
        node_type, children = node.T, node.C
        #print("Ascender.walk:", node_type, children)
        assert isinstance(node_type, str)
        #print("walk: in:", node.pretty())
        saved = self.Indent 
        self.Indent += "  "
        children = [self.walk(c) for c in children]
        self.Indent = saved
        #print("walk: children->", children)
        if hasattr(self, node_type):
            method = getattr(self, node_type)
            if hasattr(method, "pass_node") and getattr(method, "pass_node"):
                out = method(node)
            else:
                out = method(children, node.V)
        else:
            out = self.__default(node, children)
        return out
        
    def __default(self, node, children):
        return Node(node.T, children=children, value=node.V)

class _Converter(Transformer):
    
    def convert(self, tree, default_namespace):
        tree = self.transform(tree)
        return self._apply_params({"namespace":default_namespace}, tree)

    def f_(self, args):
        assert len(args) == 1
        return args[0]
    
    def int_constant(self, args):
        return int(args[0].value)
        
    def float_constant(self, args):
        return float(args[0].value)

    def bool_constant(self, args):
        #print("bool_constant:", args, args[0].value)
        return args[0].value == "true"
        
    def string_constant(self, args):
        s = args[0].value
        if s[0] == '"':
            s = s[1:-1]
        return s
        
    def named_query(self, args):
        assert len(args) == 1
        return Node("named_query", value = args[0].V)       # value = (namespace, name) - tuple
        
    def exp_list(self, args):
        return args

    def __default__(self, data, children, meta):
        #print("__default__:", data, children)
        return Node(data, children)
        
    def param_def(self, args):
        return (args[0].value, args[1])

    def _apply_params(self, params, node):
        if isinstance(node, Node):
            #print("_apply_params:", node)
            if node.T == "namespace_name":
                assert len(node.V) == 2
                if node.V[0] is None and "namespace" in params:
                    node.V[0] = params["namespace"]
                    #print("_apply_params: applied namespace:", params["namespace"])
            else:
                for n in node.C:
                    self._apply_params(params, n)        
        return node    
        
    def term_with_params(self, args):
        assert len(args) == 2
        params, term = args
        return self._apply_params(params, term)
        
    def with_clause(self, args):
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
        args = args[0]
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
        assert len(args) == 1
        args = args[0]
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
        return Node("minus", [left, right])
        
    def namespace_name(self, args):
        assert len(args) in (1,2)
        if len(args) == 1:
            return Node("namespace_name", value=[None, args[0].value])      # no namespace
        else:
            return Node("namespace_name", value=[args[0].value, args[1].value])

    def dataset(self, args):
        assert len(args) in (1,2)
        if meta_to_sql:
                if len(args) == 1:
                    return Node("dataset", [args[0], None])       # dataset without meta_filter
                else:
                    return Node("dataset", [args[0], args[1]])
        else:
                if len(args) == 1:
                    return Node("dataset", [args[0], None])
                if not args[1].T in ( "and", "or", "not" ):
                    return Node("dataset", [args[0], args[1]])
                n = Node("dataset", [args[0], None])
                return Node("meta_filter", [n, args[1]])
        
    def filter(self, args):
        assert len(args) == 3
        return Node("filter", args[2], value = (args[0].value, args[1]))
        
    def metafilter_exp(self, args):
        assert len(args) == 2
        return Node("meta_filter", args)
        
    def filter_params(self, args):
        #print("filter_params:", args)
        return args
        
    def cmp_op(self, args):
        return Node(args[1].value, [args[0].value, args[2]])
        
    def in_op(self, args):
        return Node("in", [args[1].value, args[0]])
        
    def meta_not(self, args):
        assert len(args) == 1
        #print("meta_not: arg:", args[0])
        return Node("not", [args[0]])
        
    def meta_and(self, args):
        assert len(args) == 2
        left, right = args
        #print("meta_and:", left, right)
        if isinstance(left, Node) and left.T == "and":
            return left + [right]
        else:
            return Node("and", [left, right])
        
    def meta_or(self, args):
        assert len(args) == 2
        left, right = args
        if isinstance(left, Node) and left.T == "or":
            return left + [right]
        else:
            return Node("or", [left, right])
            
class _Assembler(Ascender):

    def __init__(self, db, default_namespace):
        Ascender.__init__(self)
        self.DB = db
        self.DefaultNamespace = default_namespace
        
    def walk(self, inp):
        #print(self.Indent, "Assembler.walk(): in:", inp.pretty() if isinstance(inp, Node) else repr(inp))
        out = Ascender.walk(self, inp)
        #print(self.Indent, "Assembler.walk(): out:", out.pretty() if isinstance(out, Node) else repr(out))
        return out
        
    def named_query(self, children, query_name):
        namespace, name = query_name
        namespace = namespace or self.DefaultNamespace
        out = Query.from_db(self.DB, namespace, name).parse()
        #print("Assembler: named_query: out=", out.pretty())
        return out
        
class _Optimizer(Ascender):

    def parents_of(self, node):
        children = node.C
        assert len(children) == 1
        child = children[0]
        if isinstance(child, Node) and child.T in ("union", "join"):
            return Node(child.T, [Node("parents_of", cc) for cc in child.C])
        else:
            return node 

    parents_of.pass_node = True
            
    def children_of(self, node):
        children = node.C
        assert len(children) == 1
        child = children[0]
        if isinstance(child, Node) and child.T in ("union", "join"):
            # parents (union(x,y,z)) = union(parents(x), parents(y), parents(z))
            return Node(child.T, [Node("children_of", cc) for cc in child.C])
        else:
            return node

    children_of.pass_node = True

    def meta_filter(self, children, value):
        assert len(children) == 2
        query, meta_exp = children
        return self.apply_meta_exp(query, meta_exp)
        
    def apply_meta_exp(self, node, exp):
        if not meta_to_sql:
            # do nothing
            return Node("meta_filter", [node, exp])
            
        t = node.T
        if t in ("join", "union"):
            new_children = [self.apply_meta_exp(c, exp) for c in node.C]
            return Node(t, new_children)
        elif t == "minus":
            assert len(node.C) == 2
            left, right = node.C
            return Node(t, [self.apply_meta_exp(left, exp), right])
        elif t == "filter":
            return Node("meta_filter", [node, exp])
        elif t == "dataset":
            assert len(node.C) == 2
            ds, meta_exp = node.C
            if meta_exp is None:
                new_exp = exp 
            elif meta_exp.T == "and":
                new_exp = meta_exp + [exp]
            else:
                new_exp = Node("and", [meta_exp, exp])
            return Node("dataset", [ds, new_exp])
        else:
            raise ValueError("Unknown node type in Optimizer.apply_meta_exp: %s" % (node,))
            
class _Evaluator(Ascender):

    def __init__(self, db, filters):
        Ascender.__init__(self)
        self.Filters = filters
        self.DB = db

    def parents_of(self, args, value):
        assert len(args) == 1
        arg = args[0]
        if False and arg.T == "dataset":      # not implemented yet
            return self.dataset(arg.C, arg.V, "parents")
        else:
            return arg.parents(with_metadata=True)

    def children_of(self, args, value):
        assert len(args) == 1
        arg = args[0]
        #print("_Evaluator.children_of: arg:", arg)
        if False and arg.T == "dataset":      # not implemented yet
            return self.dataset(arg.C, arg.V, "children")
        else:
            #print("children_of: calling children()...")
            return arg.children(with_metadata=True)

    def dataset(self, args, value, provenance=None):
        assert len(args) == 2
        dataset_name, meta_exp = args
        namespace, name = dataset_name.V
        dataset = DBDataset.get(self.DB, namespace, name)
        condition = None if meta_exp is None else self.meta_exp_to_sql(meta_exp)
        #print("dataset: condition:", condition)
        files = dataset.list_files(condition=condition, 
            relationship="self" if provenance is None else provenance, 
            with_metadata=True)
        #print ("Evaluator.dataset: files:", files)
        assert isinstance(files, DBFileSet)
        return files
        
    def union(self, args, value):
        return DBFileSet.union(self.DB, args)
        
    def join(self, args, value):
        return DBFileSet.join(self.DB, args)
        
    def minus(self, expressions, value):
        assert len(expressions) == 2
        left, right = expressions
        return left - right

    def filter(self, args, value):
        name, params = value
        inputs = args
        #print("Evaluator.filter: inputs:", inputs)
        filter_function = self.Filters[name]
        return DBFileSet(self.DB, filter_function(inputs, params))
        
    def meta_filter(self, args, value):
        assert len(args) == 2
        files, meta_exp = args
        return DBFileSet(self.DB, (f for f in files if self.evaluate_meta_expression(f, meta_exp)))
        
    def _eval_meta_bool(self, f, bool_op, parts):
        assert len(parts) > 0
        p0 = parts[0]
        rest = parts[1:]
        ok = self.evaluate_meta_expression(f, p0)
        if bool_op == "and":
            if len(rest) and ok:
                ok = self._eval_meta_bool(f, bool_op, rest)
            return ok
        elif bool_op == "or":
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
        op, args = meta_expression.T, meta_expression.C
        if op in self.BOOL_OPS:
            return self._eval_meta_bool(f, op, args)
        else:
            # 
            name, value = args
            attr_value = f.get_attribute(name, None)
            if op == "<":          return attr_value < value
            elif op == ">":        return attr_value > value
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
                elif isinstance(value, int): 
                     return "attr.name='%s' and (attr.int_value %s '%s' or attr.float_value %s '%s')" % (
                                     name, op, value, op, value)
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
        

class Query(object):

    _Parser = Lark(grammar, start="exp")

    def __init__(self, source, default_namespace=None):
        self.Source = source
        self.DefaultNamespace = default_namespace
        self.Parsed = self.Optimized = self.Assembled = None
        
    def remove_comments(self, text):
        out = []
        for l in text.split("\n"):
            l = l.split('#', 1)[0]
            out.append(l)
        return '\n'.join(out)
        
    def parse(self):
        if self.Parsed is None:
            tree = self._Parser.parse(self.remove_comments(self.Source))
            self.Parsed = _Converter().convert(tree, self.DefaultNamespace)
        return self.Parsed
        
    def assemble(self, db, default_namespace = None):
        if self.Assembled is None:
            parsed = self.parse()
            #print("Query.assemble(): parsed:", parsed.pretty())
            self.Assembled = _Assembler(db, default_namespace).walk(parsed)
            #print("Query.assemble: self.Assembled:", self.Assembled.pretty())
        return self.Assembled
        
    def skip_assembly(self):
        if self.Assembled is None:
            self.Assembled = self.parse()
        return self.Assembled
        
    def optimize(self):
        #print("Query.optimize: entry")
        assert self.Assembled is not None
        self.Optimized = _Optimizer().walk(self.Assembled)
        #print("Query.optimize: optimized:", self.Optimized)
        return self.Optimized

    def run(self, db, filters={}):
        self.assemble(db, self.DefaultNamespace)
        return _Evaluator(db, filters).walk(self.optimize())
        
    @property
    def code(self):
        return self.parse().to_json()
        
    @staticmethod
    def from_db(db, namespace, name):
        return Query(DBNamedQuery.get(db, namespace, name).Source)

    def to_db(self, db, namespace, name):
        return DBNamedQuery(db, namespace, name, self.Source).save()

if __name__ == "__main__":
    import sys
    tree = Query.Parser.parse(open(sys.argv[1], "r").read())
    converted = _Converter().transform(tree)
    pprint.pprint(converted)

        
