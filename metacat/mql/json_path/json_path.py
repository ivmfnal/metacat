from lark import Lark
import json

from trees import Ascender, Converter, Node, LarkToNodes

class PathConverter(LarkToNodes):
    
    def dot(self, args):
        #print("dot(): args:", args, dir(args[1]))
        member_name = args[1].V
        if args[1].T == "STRING":
            member_name = member_name[1:-1]
        return Node("dot", [args[0]], member=member_name)
    
    def islice(self, args):
        return (int(args[0].V), int(args[2].V))
    
    def index(self, args):
        arg = args[0]
        if arg.V == "*":
            return "*"
        else:
            return int(arg.V)
    
    def subscript(self, args):
        node, index_list = args[0], args[1:]
        return Node("subscript", [node], index_list=index_list)
    
    def value(self, args):
        v = args[0]
        if v.T == "SIGNED_INT":
            return int(v.V)
        elif v.T == "FLOAT":
            return float(v.V)
        elif v.T == "STRING":
            return v.V[1:-1]
        elif v.T == "BOOL":
            return v.V.lower() == "true"
        elif v.T == "null":
            return None
        
    def negate(self, args):
        assert len(args) == 1
        child = args[0]
        if child.T == "binary_expression":
            try:
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
                }[child["op"]]
            except KeyError:
                pass
            else:
                return child.clone(op=new_op)
        return Node("negate", args)
    
    def binary_expression(self, args):
        return Node("binary_expression", [args[0], args[2]], op=args[1].V)

    def __default__(self, typ, children, meta):
        return Node(typ, children, _meta=meta)

class PathEvaluator(Ascender):
    
    # evaluates the path expression to a list of nodes
    
    def evaluate(self, path_expression, root, current=None):
        #print("PathEvaluator.evaluate: path:", path_expression.pretty())
        #print("                        current:", current)
        self.Root = root
        self.Current = current
        out = self.walk(path_expression, debug=False)
        assert out.T == "values"
        return out["values"]
        
    def root(self, t):
        #print("root")
        out = Node("values", values=[self.Root])
        #print("root: returning:", out.pretty())
        return out
    
    def current(self, t):
        if self.Current is None:
            return t.clone()
        else:
            return Node("values", values=self.Current)
    
    def dot(self, t, node, member=None):
        if node.T != "values":
            return t.clone()   # return the node without changes
        values = node["values"]
        valid = [v for v in values if isinstance(v, dict) and member in v]
        members = [n[member] for n in valid]
        node = Node("values", values=members)
        #print("\ndot: member=", type(member), member)
        #print("  input values:", values)
        #print("  valid:", valid)
        #print("  members:", members)
        #print("  returning:", node["values"])
        return node

    def subscript(self, t, node, index_list=None):
        if node.T != "values":
            return t.clone()
        out = []
        if index_list:
            for n in node["values"]:
                if "*" in index_list:
                    if isinstance(n, dict):
                        out.extend(list(n.values()))
                    elif isinstance(n, list):
                        out.extend(n)
                elif len(index_list) == 1 and isinstance(index_list[0], int):
                    j = index_list[0]
                    if isinstance(n, dict) and index in n \
                        or isinstance(n, list):
                            try:   out.append(n[j])
                            except (IndexError, KeyError):
                                pass
                else:
                    this_out = []
                    for index in index_list:
                        if isinstance(index, tuple):
                            if isinstance(n, dict):
                                for j in range(index[0], index[1]):
                                    try: this_out.append(n[j])
                                    except KeyError: pass
                            else:
                                this_out.extend(n[index[0]:index[1]])
                        else:
                            # assume int
                            try: this_out.append(n[index])
                            except (IndexError, KeyError):
                                pass
                    out.append(this_out)
        return Node("values", values=out)

    def predicate(self, t, node, expression):
        if node.T != "values":
            return t.clone()   # filter node without change
        values = node["values"]
        #print("PathEvaluator.predicate: input values:", values)
        evaluator = PredicateEvaluator(expression)
        #print("PathEvaluator.predicate: evaluating values:", values)
        passed = evaluator.evaluate(values)
        #print("PathEvaluator.predicate:            passed:", passed)
        return Node("values", values=passed)

class PredicateEvaluator(Ascender):
    
    def __init__(self, expression):
        self.ExpressionTree = expression
        self.Current = None   # the value of "@"
    
    def evaluate_single(self, value):
        #print("PredicateEvaluator.evaluate_single(): tree:", "\n", self.ExpressionTree.pretty(indent="    "))
        #print("  value:", value)
        self.Current = [value]
        result = self.walk(self.ExpressionTree)
        #print("PredicateEvaluator.evaluate(): result:\n", result)
        return result
    
    def evaluate(self, values):
        return [v for v in values if self.evaluate_single(v)]
    
    def current(self, t):
        #print("PredicateEvaluator.current(): returning values:", self.Current)
        return Node("values", values=self.Current)
    
    def or_expression(self, t, left, right):
        return left or right
    
    def and_expression(self, t, left, right):
        return left and right
    
    def value(self, v):
        if v.T == "SIGNED_INT":
            return int(v.V)
        elif v.T == "STRING":
            return v.V[1:-1]
        elif v.T == "BOOL":
            return v.V.lower() == "true"
        elif v.T == "FLOAT":
            return float(v.V)

    def evaluate_path(self, path):
        values = PathEvaluator().evaluate(path, None, self.Current)
        return Node("values", values=values)
    
    def negate(self, t, exp):
        return not exp

    def exists(self, t, values):
        return len(values["values"]) > 0
        
    def binary_expression(self, t, path_tree, value, op=None):
        values = self.evaluate_path(path_tree)["values"]
        #print("PredicateEvaluator.cmp_expression: input values:", values)
        if op == "<":
            values = [v for v in values if isinstance(v, (str, int, float)) and v < value]
        elif op == "<=":
            values = [v for v in values if isinstance(v, (str, int, float)) and v <= value]
        elif op == ">":
            values = [v for v in values if isinstance(v, (str, int, float)) and v > value]
        elif op == ">=":
            values = [v for v in values if isinstance(v, (str, int, float)) and v >= value]
        elif op == "==":
            values = [v for v in values if isinstance(v, (str, int, float, bool, None)) and v == value]
        elif op == "!=":
            values = [v for v in values if isinstance(v, (str, int, float, bool, None)) and v != value]
        elif op == "like_regex":
            pattern = re.compile(value)
            values = [v for v in values if isinstance(v, str) and pattern.match(v)]
        else:
            raise NotImplementedError(f"Binary operation {op} is not implemented")
        result = not not values
        #print("                                   returning:", result)
        return result

class JSONPathEvaluator(object):

    Grammar = """
    ?json_path_expression : path

    path :    "$"                                     -> root
        | "@"                                         -> current
        | path "." (CNAME | STRING)                   -> dot
        | path "." CNAME "()"                         -> method
        | path "?" "(" expression ")"                 -> predicate
        | path "[" _index_list "]"                    -> subscript

    _index_list : index ("," index)*
    
    !index : SIGNED_INT
        | "*"
        | SIGNED_INT "to" SIGNED_INT                  -> islice                    

    ?expression : or_expression

    ?or_expression : and_expression
        | or_expression "||" and_expression
    
    ?and_expression : term_expression
        | and_expression "&&" term_expression

    ?term_expression : binary_expression
        | "(" expression ")"
        | "!" term_expression                         -> negate

    binary_expression : path OP value

    !value : STRING
        | SIGNED_INT
        | FLOAT
        | BOOL
        | "null"

    OP:  "<" "="? | "!"? "=" "="? | "<" ">" | "!"? "~" "*"? | ">" "="? | "like_regex"

    BOOL: "true"i | "false"i
    STRING : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i
    CNAME: ("_"|LETTER) ("_"|LETTER|DIGIT)*

    %import common.LETTER
    %import common.FLOAT
    %import common.DIGIT
    %import common.SIGNED_INT
    %import common.SIGNED_FLOAT
    %import common.WS
    %ignore WS


    """

    Parser = Lark(Grammar, start="json_path_expression")

    def __init__(self, expression):
        parsed = self.Parser.parse(expression)
        self.Converted = PathConverter()(parsed)
        self.PathEvaluator = PathEvaluator()

    def evaluate(self, data):
        return self.PathEvaluator.evaluate(self.Converted, data)
        
    __call__ = evaluate
    
    def exists(self, data):
        return not not self.evaluate(data)
    
def compile(expression):
    return JSONPathEvaluator(expression)
