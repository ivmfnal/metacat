class MetaEvaluator(object):

    BOOL_OPS = ("and", "or", "not")

    def evaluate_meta_expression(self, metadata, meta_expression):
        #print("evaluate_meta_expression: meta_expression:", meta_expression.pretty())
        #print("    meta:", metadata)
        op, args = meta_expression.T, meta_expression.C
        #print("evaluate_meta_expression:", op, args)
        if op in ("meta_and", "meta_or") and len(args) == 1:
            return self.evaluate_meta_expression(metadata, args[0])
        #if meta_expression["neg"]:
        #    return not self.evaluate_meta_expression(metadata, meta_expression.clone(neg=False))
        if op == "meta_and":    op = "and"
        if op == "meta_or":     op = "or"
        if op in self.BOOL_OPS:
            return self.eval_meta_bool(metadata, op, args)
        elif op == "present":
            return meta_expression["name"] in metadata
        elif op == "not_present":
            return not meta_expression["name"] in metadata
        elif op in ("in_set", "not_in_set"):
            neg = meta_expression.get("neg", False) != (op == "not_in_set")
            vset = set(meta_expression.get("set", []))
            left = args[0]
            if left.T == "scalar":
                aname = left["name"]
                result = aname in metadata and metadata[aname] in vset
            elif left.T == "array_any":
                aname = left["name"]
                if not aname in metadata:  return neg
                lst = metadata[aname]
                if not isinstance(lst, list):   return neg
                for x in lst:
                    if x in vset:  
                        result = True
                        break
                else:
                    result = False
            elif left.T == "array_subscript":
                aname = left["name"]
                inx = left["index"]
                if not aname in metadata:  return neg
                lst = metadata[aname]
                try:    v = lst[inx]
                except: result = False
                else: result = v in vset
            elif left.T == "array_length":
                aname = left["name"]
                if not aname in metadata:  return neg
                lst = metadata[aname]
                if not isinstance(lst, list):
                    result = False
                else:
                    result = len(lst) in vset
            return result != neg        # negate if neg
        elif op in ("in_range", "not_in_range"):
            low, high = meta_expression["low"], meta_expression["high"]
            left = args[0]
            neg = meta_expression.get("neg", False) != (op == "not_in_range")
            if left.T == "scalar":
                aname = left["name"]
                try:    return (aname in metadata and metadata[aname] >= low and metadata[aname] <= high) != neg
                except: return neg
            elif left.T == "array_any":
                aname = left["name"]
                if not aname in metadata:  return neg
                lst = metadata[aname]
                if isinstance(lst, dict):
                    attr_values = lst.values()
                elif isinstance(lst, list):
                    attr_values = lst
                else:
                    return neg
                for x in attr_values:
                    if x >= low and x <= high:  return not neg
                else:
                    return neg
            elif left.T == "array_subscript":
                aname = left["name"]
                inx = left["index"]
                if not aname in metadata:  return neg
                lst = metadata[aname]
                try:    v = lst[inx]
                except: return neg
                return (v >= low and v <= high) != neg                    
            elif left.T == "array_length":
                aname = left["name"]
                if not aname in metadata:  return neg
                lst = metadata[aname]
                if not isinstance(lst, list):
                    return neg
                l = len(lst)
                return (l >= low and l <= high) != neg
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
            elif left.T == "array_length":
                aname = left["name"]
                lst = metadata.get(aname)
                if lst is None:  return False
                if not isinstance(lst, list):
                    return False
                l = len(lst)
                result = self.do_cmp_op(l, cmp_op, value)  
                return result
        raise ValueError("Invalid expression:\n"+meta_expression.pretty())
        
    __call__ = evaluate_meta_expression

    def eval_meta_bool(self, f, bool_op, parts):
        assert len(parts) > 0
        p0 = parts[0]
        rest = parts[1:]
        ok = self.evaluate_meta_expression(f, p0)
        if bool_op in ("and", "meta_and"):
            if len(rest) and ok:
                ok = self.eval_meta_bool(f, bool_op, rest)
            return ok
        elif bool_op in ("or", "meta_or"):
            if len(rest) and not ok:
                ok = self.eval_meta_bool(f, bool_op, rest)
            return ok
        elif bool_op == "not":
            assert len(rest) == 0
            return not ok
        else:
            raise ValueError("Unrecognized boolean operation '%s'" % (op,))
    
    def do_cmp_op(self, x, op, y):
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
        elif op in ("~", "!~", "~*", "!~*"):
            negated = op[0] == '!'
            flags = re.IGNORECASE if op[-1] == '*' else 0
            r = re.compile(y, flags)
            match = r.search(x) is not None
            return negated != match
        else:
            raise ValueError("Invalid comparison operator '%s'" % (op,))
        
    @staticmethod
    def evaluate(meta, exp):
        return _MetaEvaluator().evaluate_meta_expression(meta, exp)
    
