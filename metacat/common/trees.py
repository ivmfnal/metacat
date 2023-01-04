from lark import Lark
from lark import Transformer, Tree, Token
import pprint
from textwrap import dedent

class SyntaxTreeConversionError(Exception):
    pass

class Token(object):
    
    JSON_CLASS = "token"

    def __init__(self, typ, value):
        # print("Token created:", typ, value)
        self.T = typ
        self.V = value

    def __str__(self):
        return "Token(%s, %s)" % (self.T, self.V)

    def pretty(self):
        #print("pretty---")
        return self._pretty()
        
    def _pretty(self, indent="", headline_indent=None):
        return 'token %s "%s"' % (self.T, self.V), []

    def jsonable(self):
        return dict(T=self.T, V=self.V)
        
    def to_json(self):
        d = self.jsonable()
        d["///class///"] = self.JSON_CLASS
        return json.dumps(d)

    @staticmethod
    def from_jsonable(data):
        if isinstance(data, dict) and data.get("///class///") == Token.JSON_CLASS:
            typ = data["T"]
            val = data["V"]
            return Token(typ, val)
        else:
            return data

    @staticmethod
    def from_json(text):
        return Token.from_jsonable(json.loads(text))

class Node(object):

    JSON_CLASS = "node"

    def __init__(self, typ, children=[], _data=None, _meta=None, **kw):
        self.T = typ
        self.M = _meta
        self.C = children[:]
        self.D = {}
        self.D.update(_data or {})
        self.D.update(kw)

    def __str__(self):
        return "Node(%s (%d children) (%d data) meta:%s)" % (self.T, len(self.C), len(self.D), self.M)
        
    __repr__ = __str__
    
    def __getitem__(self, name):
        return self.D[name]

    def clone(self, children=None, **kw):
        if children is None:    children = self.C[:]
        d = {}
        d.update(self.D)
        d.update(kw)
        return Node(self.T, children, _data=d, _meta=self.M)
        
    def get(self, name, default=None):
        return self.D.get(name, default)

    def __setitem__(self, name, value):
        self.D[name] = value
        
    def _pretty(self, indent=""):
        out = []
        items = list(self.D.items())
        nitems = len(items)
        nc = len(self.C)
        for i, (k, v) in enumerate(items):
            key = f"{k} = "
            if isinstance(v, (Node, Token)):
                key_len = len(key)
                prefix = "|  " if i < len(items) - 1 else "   "
                head, lines = v._pretty(indent + prefix + " "*key_len + " ")
                out.append(indent + "+   " + key + head)
                out += lines
            else:
                str_v = dedent(str(v))
                lines = str_v.split("\n")
                if len(lines) > 1:
                    out.append(indent + "+   " + key)
                    prefix = "|   " if i < nitems-1 or nc else "    "
                    for l in lines:
                        out.append(indent + prefix + " " + l)
                else:
                    out.append(indent + "+   " + key + str(v))

        for i, c in enumerate(self.C):
            child_indent = indent + (' ' if i == nc-1 else '|') + "  "
            if isinstance(c, (Node, Token)):
                head, lines = c._pretty(child_indent)
                out.append(indent + "+- " + head)
                out.extend(lines)
            else:
                out.append(indent + "+- " + str(c))

        head = self.T
        if self.M is not None:
            head += f" m:{self.M}"

        return head, out

    def pretty(self, indent=""):
        #print("pretty---")
        head, lines = self._pretty(indent)
        return indent + head + "\n" + "\n".join(lines)
        
    def jsonable(self):
        d = dict(T=self.T, M=self.M, C=[c.jsonable() if isinstance(c, Node) else c
                        for c in self.C]
        )
        return d
        
    def to_json(self):
        d = self.jsonable()
        d["///class///"] = self.JSON_CLASS
        return json.dumps(d)

    @staticmethod
    def from_jsonable(data):
        if isinstance(data, dict) and data.get("///class///") == Node.JSON_CLASS:
            typ = data["T"]
            if typ == "DataSource":
                return DataSource.from_jsonable(data)
            elif typ == "MetaExp":
                return MetaExp.from_jsonable(data)
            else:
                return Node(data["T"],
                    children = [Node.from_jsonable(c) for c in data.get("C", [])],
                    meta = data.get("M")
            )
        else:
            return data

    @staticmethod
    def from_json(text):
        return Node.from_jsonable(json.loads(text))
        
    def find_all(self, node_type=None, predicate=None, top_down=True):
        def match(c):
            return (node_type is not None and c.T == node_type) or (predicate is not None and predicate(c))
        if top_down and match(self):
            yield self
        for c in self.C:
            if match(c):
                yield(c)
        if not top_down and match(self):
            yield self

def pass_node(method):
    def decorated(self, *params, **args):
        return method(self, *params, **args)
    decorated.__pass_node__ = True
    return decorated

class Visitor(object):	# deprecated

    #
    # Visits each node top-down. Calls corresponding method of each node passing the "context". 
    # If the method returns True, recursively visits the node children
    # Can be used to re-calculate metadata
    #

    def walk(self, node, context=None):
        if not isinstance(node, Node):
            return
        node_type, children = node.T, node.C

        try:
            if hasattr(self, node_type):
                method = getattr(self, node_type)
                visit_children = method(node, context)
            else:
                visit_children = self._default(node, context)
        except Exception as e:
            raise SyntaxTreeConversionError(f"Error while processing node {node_type} with children {children}") from e

        if visit_children:
            for c in children:
                self.walk(c, context)
        return node

    def visit_children(self, node, context):
        for c in node.C:
            self.walk(c, context)
        
    def _default(self, node, context):
        return True
        
class Traveler(object):

    def indent(self, text, rel_level=0, level=None, pad='  ', indent=None):
        if level is None:
            level = self.WalkLevel+rel_level          # concrete subclass must set it
        if indent is None:
            indent = level * pad
        min_indent = None
        lines = text.split("\n")
        for line in lines:
            if line:
                line = line.expandtabs()
                l = len(line) - len(line.lstrip())
                if min_indent is None or l < min_indent:
                    min_indent = l
        #print("min_indent:", min_indent)
        if min_indent:
            out_lines = []
            for line in lines:
                out_lines.append(indent + line[min_indent:])
                #print(f"line -> [{line}]")
            text = '\n'.join(out_lines)
        return text

    def __call__(self, *params, **args):
        return self.walk(*params, **args)

class Descender(Traveler):

    #
    # Descends nodes top to bottom, possibly replacing them
    # If a user method is defined for the node type, it has to explicitly call visit_children(self, context)
    # If the user method returns None, it is equivalent to returning the node itself
    # Default method does visit children
    #

    def walk(self, tree, context=None, level=0):
        self.WalkLevel = level
        try:
            return self._walk(tree, context)
        finally:
            self.WalkLevel = level

    def _walk(self, node, context=None):
        self.WalkLevel += 1
        try:
            if not isinstance(node, Node):
                return node

            node_type = node.T
        
            try:
                if hasattr(self, node_type):
                    method = getattr(self, node_type)
                    new_node = method(node, context)
                else:
                    new_node = self._default(node, context)
            except Exception as e:
                raise SyntaxTreeConversionError(f"Error while processing node {node_type}") from e
            
            return new_node

        finally:
            self.WalkLevel -= 1

    def visit_children(self, node, context):
        node.C = [self._walk(c, context) for c in node.C]
        node.D = {
            key:self._walk(n, context)
            for key, n in node.D.items()
        }
        return node

    def _default(self, node, context):
        #print("Descender._default: node:", node.pretty())
        return self.visit_children(node, context)
        
class Ascender(Traveler):
    
    def walk(self, tree, debug=False, level=0):
        self.WalkLevel = level
        try:
            return self._walk(tree, debug)
        finally:
            self.WalkLevel = level
            
    def _walk(self, node, debug=False):
        self.WalkLevel += 1
        try:
            if not isinstance(node, Node):
                return node
            node_type, children = node.T, node.C
            assert isinstance(node_type, str)

            method = self._default
            pass_node = False

            if hasattr(self, node_type):
                method = getattr(self, node_type)
                if hasattr(method, "__pass_node__") and getattr(method, "__pass_node__"):
                    pass_node = True
        
            if pass_node:
                out = method(node)
            else:
                named_children = {
                    name:(self.walk(c, debug) if isinstance(c, Node) else c) 
                    for name, c in node.D.items()
                }
                children = [self._walk(c, debug) for c in children]
                node = node.clone(children)
                out = method(node, *children, **named_children)
                if debug:
                    me = self.__class__.__name__
                    #print(f"{me}: method {node_type} returned:", out.pretty("      ") if isinstance(out, Node) else out)
        finally:
            self.WalkLevel -= 1
        return out
        
    def _default(self, node, *children, **named):
        return Node(node.T, children, _meta=node.M, _data=named)
        
class Converter(Transformer):
    
    #
    # Base class for converters from Lark tree to Node tree
    #
    
    def __call__(self, tree):
        return self.transform(tree)

class LarkToNodes(Converter):
    
    #
    # Converts from Tree structure returned by Lark to Nodes
    #
    
    def __default__(self, data, children, meta):
        #print("LarkToNodes.__default(", data, children, meta, ")")
        return Node(data, children, meta=meta)
        
    def __default_token__(self, token):
        #print("LarkToNodes.__default_token__: token:", token)
        return Token(token.type, token.value)

        
if __name__ == "__main__":
    
    x = Node("leaf", [], value="hello there", meta=(1,2,3))
    y = Node("leaf", [], value=5)
    
    pi = Node("float", 
        [
            Node("sub_float", [], value=1234),
            Node("sub_float", [], meta=1234)
        ], value=3.14)
    e = Node("float", 
        [
            Node("sub_float", [], value=1234),
            Node("sub_float", [], meta=1234)
        ], value=(2.718, 2818)
    )
    
    tree = Node("top", [x,y], value="some value", data=dict(
                pi = pi, e_extended_name = e, x = ("just","a","tuple")
    ))

    print(tree.pretty())
    
