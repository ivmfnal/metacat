#!/usr/bin/env python
# coding: utf-8

# In[1]:


from lark import Lark
import json

Grammar = """
?json_path_expression : path

path :    "$"                                     -> root
    | "@"                                         -> current
    | path "." CNAME                              -> dot
    | path "." CNAME "()"                         -> method
    | path "?" "(" expression ")"                 -> condition
    | path "[" index "]"                          -> subscript

!index : "*"
    | SIGNED_INT

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


# In[2]:


from trees import Ascender, Converter, Node, LarkToNodes

class PathConverter(LarkToNodes):
    
    def dot(self, args):
        return Node("dot", [args[0]], member=args[1].V)
    
    def index(self, args):
        arg = args[0]
        if arg.V == "*":
            return "*"
        else:
            return int(arg.V)
    
    def subscript(self, args):
        node, index = args
        return Node("subscript", [node], index=index)
    
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
        
    def cmp_expression(self, args):
        return Node("cmp_expression", [args[0], args[2]], op=args[1].V)

    def __default__(self, typ, children, meta):
        return Node(typ, children, _meta=meta)
    



# In[62]:


class PathEvaluator(Ascender):
    
    # evaluates the path expression to a list of nodes
    
    def evaluate(self, path_expression, root, current=None):
        #print("PathEvaluator.evaluate: path:", path_expression.pretty())
        #print("                        current:", current)
        self.Root = root
        self.Current = current
        out = self.walk(path_expression, debug=False)
        assert out.T == "values"
        return out
        
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

    def subscript(self, t, node, index=None):
        if node.T != "values":
            return t.clone()
        out = []
        for n in node["values"]:
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

    def condition(self, t, node, expression):
        if node.T != "values":
            return t.clone()   # filter node without change
        values = node["values"]
        print("PathEvaluator.condition: input values:", values)
        exp_evaluator = ConditionEvaluator(expression)
        print("PathEvaluator.condition: evaliating values:", values)
        passed = exp_evaluator.evaluate(values)
        return Node("values", values=passed)


# In[63]:


class ConditionEvaluator(Ascender):
    
    def __init__(self, expression):
        self.ExpressionTree = expression
        self.Current = None   # the value of "@"
    
    def evaluate_single(self, value):
        print("ConditionEvaluator.evaluate_single(): tree:", "\n", self.ExpressionTree.pretty(indent="    "))
        print("  value:", value)
        self.Current = [value]
        result = self.walk(self.ExpressionTree)
        print("ConditionEvaluator.evaluate(): result:\n", result.pretty(indent="    "))
        return not not result["values"]
    
    def evaluate(self, values):
        return [v for v in values if self.evaluate_single(v)]
    
    def current(self, t):
        print("ConditionEvaluator.current(): returning values:", self.Current)
        return Node("values", values=self.Current)
    
    def or_expression(self, t, left, right):
        left or right
    
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
        return PathEvaluator().evaluate(path, None, self.Current)
        
    def cmp_expression(self, t, path_tree, value, op=None):
        values = self.evaluate_path(path_tree)["values"]
        print("ConditionEvaluator.cmp_expression: input values:", values)
        if op == "<":
            values = [v for v in values if isinstance(v, (str, int, float)) and v < value]
        elif op == "<=":
            values = [v for v in values if isinstance(v, (str, int, float)) and v <= value]
        elif op == ">":
            values = [v for v in values if isinstance(v, (str, int, float)) and v > value]
        elif op == ">=":
            values = [v for v in values if isinstance(v, (str, int, float)) and v >= value]
        elif op == "==":
            values = [v for v in values if isinstance(v, (str, int, float, bool)) and v == value]
        print("                                   returning values:", values)
        return not not values


# In[64]:


# In[67]:


example = '$.track.segments[*] ? (@.HR > 70 && @.location[0] > 47.706)'
#example = "$.track ? (@ < 12)"
parsed = parser.parse(example)
print(parsed.pretty())




# In[65]:


converted = PathConverter()(parsed)
print(converted.pretty())



# In[66]:


PathEvaluator().evaluate(converted, data)


# In[ ]:





# In[ ]:




