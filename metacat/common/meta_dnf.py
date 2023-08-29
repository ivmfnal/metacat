from .trees import Ascender, Node
from .attributes import FileAttributes, DatasetAttributes

#FileAttributes = [      # file attributes which can be used in queries
#    "creator", "created_timestamp", "name", "namespace", "size"
#]  

class _MetaRegularizer(Ascender):
    # converts the meta expression into DNF form:
    #
    #   Node(or, [Node(and, [exp, ...])])
    #

    def _flatten_bool(self, op, nodes):
        #print("_flatten_bool: input:", nodes)
        new_nodes = []
        for c in nodes:
            #print("_flatten_bool: c:", c)
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

    def __default__(self, typ, children, meta):
        return Node("meta_or", [
            Node("meta_and", [Node(typ, children, _meta=meta)])
        ])

class MetaExpressionDNF(object):
    
    ObjectAttributes = []
    
    def __init__(self, exp):
        #
        # meta_exp is a nested list representing the query filter expression in DNF:
        #
        # meta_exp = [meta_or, ...]
        # meta_or = [meta_and, ...]
        # meta_and = [(op, aname, avalue), ...]
        #
        self.Exp = None
        self.DNF = None
        if exp is not None:
            #
            # converts canonic Node expression (meta_or of one or more meta_ands) into nested or-list or and-lists
            #
            #assert isinstance(self.Exp, Node)
            assert exp.T in ("meta_or", "meta_and")
            exp = self.regularize(exp)
            assert exp.T == "meta_or" and all (c.T == "meta_and" for c in exp.C or [])
            self.DNF = [and_item.C for and_item in exp.C]

        #print("MetaExpressionDNF: exp:", self.DNF)
        #self.validate_exp(meta_exp)
    
    @staticmethod
    def regularize(exp):
        return _MetaRegularizer()(exp)

    def sql_and(self, and_terms, table_name, meta_column_name="metadata"):
        
        def sql_literal(v):
            if isinstance(v, str):       
                v = "'%s'" % (v,)
            elif isinstance(v, bool):    v = "true" if v else "false"
            elif v is None:              v = "null"
            else:   v = str(v)
            return v
            
        def json_literal(v):
            if isinstance(v, str):       v = '"%s"' % (v,)
            else:   v = sql_literal(v)
            return v
            
        def pg_type(v):
            if isinstance(v, bool):   pgtype='boolean'
            elif isinstance(v, str):   pgtype='text'
            elif isinstance(v, int):   pgtype='bigint'
            elif isinstance(v, float):   pgtype='double precision'
            else:
                raise ValueError("Unrecognized literal type: %s %s" % (v, type(v)))
            return pgtype
            
        contains_items = []
        parts = []
        
        for exp in and_terms:
            
            op = exp.T
            args = exp.C
            negate = False
            
            #print("exp: T:", exp.T, "  C:", exp.C, "  C0 T:", args[0].T, "  C0 name:", args[0]["name"])

            if args and args[0].T == "object_attribute" and args[0]["name"] not in self.ObjectAttributes:
                raise ValueError("Unrecognized attribute name %s" % (args[0]["name"],))

            term = "true"

            if op in ("present", "not_present"):
                aname = exp["name"]
                term = f"{table_name}.{meta_column_name} ? '{aname}'"
                if op == "not_present":
                    negate = not negate

            else:
                assert op in ("cmp_op", "in_range", "in_set", "not_in_range", "not_in_set"), f"Unexpected expression type: {op}, exp:\n" + exp.pretty()
                arg = args[0]
                assert arg.T in ("array_any", "subscript", "array_length", "object_attribute", "meta_attribute")

                negate = not not exp.get("neg")
                aname = arg["name"]
                
                if arg.T == "subscript":
                    # a[i] = x
                    aname, inx = arg["name"], arg["index"]
                    inx_json = json_literal(inx)
                    if isinstance(inx, str):
                        subscript = f".{inx_json}"
                    else:
                        subscript = f"[{inx_json}]"
                elif arg.T == "array_any":
                    aname = arg["name"]
                    subscript = "[*]"
                elif arg.T in ("meta_attribute", "object_attribute"):
                    aname = arg["name"]
                    subscript = ""
                elif arg.T == "array_length":
                    aname = arg["name"]
                else:
                    raise ValueError(f"Unrecognozed argument type \"{arg.T}\"")

                # - query time slows down significantly if this is addded
                #if arg.T in ("array_subscript", "array_any", "array_all"):
                #    # require that "aname" is an array, not just a scalar
                #    parts.append(f"{table_name}.{meta_column_name} @> '{{\"{aname}\":[]}}'")
                
                if op == "in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    if typ == "date_constant":
                        high = float(high + 3600*24 - 0.0001)
                    if arg.T == "object_attribute":
                        low = sql_literal(low)
                        high = sql_literal(high)
                    else:
                        low = json_literal(low)
                        high = json_literal(high)
                    if arg.T == "object_attribute":
                        term = f"{table_name}.{aname} between {low} and {high}"
                    elif arg.T in ("subscript", "scalar", "array_any"):
                        term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? (@ >= {low} && @ <= {high})'"
                    elif arg.T == "array_length":
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.{meta_column_name} -> '{aname}') {n} between {low} and {high}"
                        
                if op == "not_in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    if typ == "date_constant":
                        high = float(high + 3600*24 - 0.0001)
                    if arg.T == "object_attribute":
                        low = sql_literal(low)
                        high = sql_literal(high)
                    else:
                        low = json_literal(low)
                        high = json_literal(high)
                    if arg.T == "object_attribute":
                        term = f"not ({table_name}.{aname} between {low} and {high})"
                    elif arg.T in ("subscript", "scalar", "array_any"):
                        term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? (@ < {low} || @ > {high})'"
                    elif arg.T == "array_length":
                        n = "" if negate else "not"
                        negate = False
                        term = f"jsonb_array_length({table_name}.{meta_column_name} -> '{aname}') {n} between {low} and {high}"
                        
                elif op == "in_set":
                    if arg.T == "object_attribute":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"{table_name}.{aname} in ({value_list})"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.{meta_column_name} -> '{aname}') {n} in ({value_list})"
                    else:           # arg.T in ("array_any", "subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        or_parts = [f"@ == {v}" for v in values]
                        predicate = " || ".join(or_parts)
                        term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "not_in_set":
                    if arg.T == "object_attribute":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"not ({table_name}.{aname} in ({value_list}))"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "" if negate else "not"
                        negate = False
                        term = f"not(jsonb_array_length({table_name}.{meta_column_name} -> '{aname}') {n} in ({value_list}))"
                    else:           # arg.T in ("array_any", "subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        and_parts = [f"@ != {v}" for v in values]
                        predicate = " && ".join(and_parts)
                        term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "cmp_op":
                    cmp_op = exp["op"]
                    if cmp_op == '=': cmp_op = "=="
                    sql_cmp_op = "=" if cmp_op == "==" else cmp_op
                    value = args[1]
                    value_type, value = value.T, value["value"]
                    sql_value = sql_literal(value)
                    value = json_literal(value)
                    
                    if arg.T == "object_attribute":
                        term = f"{table_name}.{aname} {sql_cmp_op} {sql_value}"
                    elif arg.T == "array_length":
                        term = f"jsonb_array_length({table_name}.{meta_column_name} -> '{aname}') {sql_cmp_op} {value}"
                    elif value_type == "date_constant":
                        assert cmp_op in ("<", "<=", ">", ">=", "=", "==", "!=")
                        if cmp_op in ("=", "=="):
                            v1 = value + 3600*24
                            term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? (@ < {v1} && @ >= {value})'"
                        elif cmp_op == "!=":
                            v1 = value + 3600*24
                            term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? (@ >= {v1} || @ < {value})'"
                        else:
                            if cmp_op == ">":
                                value += 3600*24
                                cmp_op = ">="
                            elif cmp_op == "<=":
                                value += 3600*24
                                cmp_op = "<"
                            term = f"{table_name}.{meta_column_name} @@ '$.\"{aname}\"{subscript} {cmp_op} {value}'"
                    elif cmp_op in ("~", "~*", "!~", "!~*"):
                        negate_predicate = False
                        if cmp_op.startswith('!'):
                            cmp_op = cmp_op[1:]
                            negate_predicate = not negate_predicate
                        flags = ' flag "i"' if cmp_op.endswith("*") else ''
                        cmp_op = "like_regex"
                        value = f"{value}{flags}"
                    
                        predicate = f"@ like_regex {value} {flags}"
                        if negate_predicate: 
                            predicate = f"!({predicate})"
                        term = f"{table_name}.{meta_column_name} @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                    else:
                        # scalar, subscript, array_any
                        term = f"{table_name}.{meta_column_name} @@ '$.\"{aname}\"{subscript} {cmp_op} {value}'"
                    
            if negate:  term = f"not ({term})"
            parts.append(term)

        if contains_items:
            parts.append("%s.{meta_column_name} @> '{%s}'" % (table_name, ",".join(contains_items )))
        
        return parts

    def sql(self, table_name, meta_column_name="metadata"):
        if not self.DNF:
            return None
        else:
            out = []
            for i, or_part in enumerate(self.DNF):
                and_parts = self.sql_and(or_part, table_name, meta_column_name)
                for j, and_part in enumerate(and_parts):
                    #print("and_part:", and_part)
                    if i == 0:
                        prefix = "" if j == 0 else "and "
                    else:
                        prefix = "or  " if j == 0 else "    and "
                    out.append(f"{prefix}( {and_part} )")
            out = "\n".join(out)
            #print("returning:\n", out)
            return out


class FileMetaExpressionDNF(MetaExpressionDNF):
    ObjectAttributes = FileAttributes
    

class DatasetMetaExpressionDNF(MetaExpressionDNF):
    ObjectAttributes = DatasetAttributes
    

    