#!/usr/bin/env python
# coding: utf-8

# In[75]:


from lark import Lark
import json

Grammar = """
json_path_expression : path

path :    "$"                                     -> root
    | "@"                                         -> current
    | path "." CNAME                              -> step
    | path "." CNAME "()"                         -> method
    | path "?" "(" expression ")"                 -> condition
    | path "[" index "]"                          -> subscript

!index :   SIGNED_INT
    | "*"

?expression : or_expression

?or_expression : and_expression
    | or_expression "||" and_expression
    
?and_expression : term_expression
    | and_expression "&&" term_expression

?term_expression : cmp_expression
    | "(" expression ")"

cmp_expression : path CMPOP value

value : STRING
    | SIGNED_INT
    | FLOAT
    | BOOL

CMPOP:  "<" "="? | "!"? "=" "="? | "<" ">" | "!"? "~" "*"? | ">" "="? | "like_regex"

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

parser = Lark(Grammar, start="json_path_expression")


# In[67]:


example = "$.track.segments[*].HR ? (@.down < 12)"
parsed = parser.parse(example)
print(parsed.pretty())


# In[77]:


from trees import Ascender, Converter, Node, LarkToNodes

class PathConverter(LarkToNodes):
    
    def step(self, args):
        return Node("step", [args[0]], member=args[1].V)
    
    def index(self, args):
        value = args[0].V
        typ = args[0].T
        if typ == "SIGNED_INT":
            value = int(value)
        return Node("index", [], value=value)
    
    def subscript(self, args):
        return Node("subscript", [args[0]], index=args[1]["value"])

    def __default__(self, typ, children, meta):
        return Node(typ, children, _meta=meta)
    
converted = PathConverter()(parsed)
print(converted.pretty())


# In[78]:


class PathEvaluator(Ascender):
    
    # evaluates the path expression to a list of nodes
    
    def evaluate(self, path_expression, root, current=None):
        self.Root = root
        self.Current = None
        self.walk(path_expression)
        
    def root(self, _):
        return Node("values", values=[self.Root])
    
    def current(self, node):
        if self.Current is None:
            return Node("reevaluate")
        else:
            return Node("values", values=self.Current)
    
    def step(self, nodes, member=None):
        if nodes.T != "values":
            return nodes
        valid = [n for n in nodes["values"] if isinstance(n, dict) and member in n]
        return Node("values", values=[n[member] for n in valid])

    def subscript(self, nodes, index=None):
        if nodes.T != "values":
            return nodes
        out = []
        for n in nodes["values"]:
            if index == "*":
                if isinstance(n, dict):
                    out.extend(list(n.values()))
                elif isinstance(n, list):
                    out.extend(n)
            else:
                # assume index is integer
                if isinstance(n, dict) and index in n \
                    or isinstance(n, list):
                        try:   out.append(n[index])
                        except (IndexError, KeyError):
                            pass
        return Node("values", values=out)

    def condition(self, nodes, expression):
        if isinstance(nodes, Node):
            return nodes
        exp_evaluator = ExpressionEvaluator(expression, self.Root)
        return [node for node in nodes if exp_evaluator.evaluate(node)]
    
class ExpressionEvaluator(Ascender):
    
    def __init__(self, expression, root):
        self.Root = root
        self.Current = None
        self.ExpressionTree = expression
        
    def evaluate(self, nodes):
        if not isinstance(nodes, list):
            nodes = [nodes]
        self.Current = nodes
        return self.walk(self.ExpressionTree)
    
    def or_expression(self, left, right):
        return left or right
    
    def and_expression(self, left, right):
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

    def evaluate_with_current(self, path, current):
        return PathEvaluator().evaluate(path, self.Root, current)
        
    def cmp_expression(self, nodes, cmp_op, value):
        if isinstance(nodes, Node):
            nodes = PathEvaluator().evaluate(nodes, self.Root, self.Current)
        op = cmp_op.V
        if op == "<":
            return [node for node in nodes if isinstance(node, (str, int, float)) and node < value]
        elif op == "<=":
            return [node for node in nodes if isinstance(node, (str, int, float)) and node <= value]
        elif op == ">":
            return [node for node in nodes if isinstance(node, (str, int, float)) and node > value]
        elif op == ">=":
            return [node for node in nodes if isinstance(node, (str, int, float)) and node >= value]
        elif op == "==":
            return [node for node in nodes if isinstance(node, (str, int, float, bool)) and node == value]
    
        
        
        
        


# In[79]:


data = json.loads("""
{
  "track": {
    "segments": [
      {
        "location":   [ 47.763, 13.4034 ],
        "start time": "2018-10-14 10:05:14",
        "HR": 73
      },
      {
        "location":   [ 47.706, 13.2635 ],
        "start time": "2018-10-14 10:39:21",
        "HR": 135
      }
    ]
  }
}""")
PathEvaluator().evaluate(converted, data)


# In[ ]:




