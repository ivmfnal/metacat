class MetaExpressionDNF(object):
    
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
            assert exp.T == "meta_or"
            for c in exp.C:
                assert c.T == "meta_and"
    
            or_list = []
            for and_item in exp.C:
                or_list.append(and_item.C)
            self.DNF = or_list

        #print("MetaExpressionDNF: exp:", self.DNF)
        #self.validate_exp(meta_exp)
        
    def __str__(self):
        return self.file_ids_sql()
        
    __repr__= __str__
    
    def sql_and(self, and_terms, table_name):
        

        def sql_literal(v):
            if isinstance(v, str):       v = "'%s'" % (v,)
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

            term = ""

            if op == "present":
                aname = exp["name"]
                if not '.' in aname:
                    term = "true" if aname in DBFile.ColumnAttributes else "false"
                else:
                    term = f"{table_name}.metadata ? '{aname}'"

            elif op == "not_present":
                aname = exp["name"]
                if not '.' in aname:
                    term = "false" if aname in DBFile.ColumnAttributes else "true"
                else:
                    term = f"{table_name}.metadata ? '{aname}'"
            
            else:
                assert op in ("cmp_op", "in_range", "in_set", "not_in_range", "not_in_set")
                arg = args[0]
                assert arg.T in ("array_any", "array_subscript","array_length","scalar")
                negate = exp["neg"]
                aname = arg["name"]
                if not '.' in aname:
                    assert arg.T == "scalar", f"File attribute {aname} value must be a scalar. Got {arg.T} instead"
                    if not aname in DBFile.ColumnAttributes:
                        raise ValueError(f"Unrecognized file attribute \"{aname}\"\n" +
                            "  Allowed file attibutes: " +
                            ", ".join(DBFile.ColumnAttributes)
                        )

                if arg.T == "array_subscript":
                    # a[i] = x
                    aname, inx = arg["name"], arg["index"]
                    inx = json_literal(inx)
                    subscript = f"[{inx}]"
                elif arg.T == "array_any":
                    aname = arg["name"]
                    subscript = "[*]"
                elif arg.T == "scalar":
                    aname = arg["name"]
                    subscript = ""
                elif arg.T == "array_length":
                    aname = arg["name"]
                else:
                    raise ValueError(f"Unrecognozed argument type \"{arg.T}\"")

                # - query time slows down significantly if this is addded
                #if arg.T in ("array_subscript", "array_any", "array_all"):
                #    # require that "aname" is an array, not just a scalar
                #    parts.append(f"{table_name}.metadata @> '{{\"{aname}\":[]}}'")
                
                if op == "in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if not '.' in aname:
                        low = sql_literal(low)
                        high = sql_literal(high)
                        term = f"{table_name}.{aname} between {low} and {high}"
                    elif arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ >= {low} && @ <= {high})'"
                    elif arg.T == "array_length":
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                if op == "not_in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if not '.' in aname:
                        low = sql_literal(low)
                        high = sql_literal(high)
                        term = f"not ({table_name}.{aname} between {low} and {high})"
                    elif arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ < {low} || @ > {high})'"
                    elif arg.T == "array_length":
                        n = "" if negate else "not"
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                elif op == "in_set":
                    if not '.' in aname:
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"{table_name}.{aname} in ({value_list})"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} in ({value_list})"
                    else:           # arg.T in ("array_any", "array_subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        or_parts = [f"@ == {v}" for v in values]
                        predicate = " || ".join(or_parts)
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "not_in_set":
                    if not '.' in aname:
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"not ({table_name}.{aname} in ({value_list}))"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "" if negate else "not"
                        negate = False
                        term = f"not(jsonb_array_length({table_name}.metadata -> '{aname}') {n} in ({value_list}))"
                    else:           # arg.T in ("array_any", "array_subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        and_parts = [f"@ != {v}" for v in values]
                        predicate = " && ".join(and_parts)
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "cmp_op":
                    cmp_op = exp["op"]
                    if cmp_op == '=': cmp_op = "=="
                    sql_cmp_op = "=" if cmp_op == "==" else cmp_op
                    value = args[1]
                    value_type, value = value.T, value["value"]
                    sql_value = sql_literal(value)
                    value = json_literal(value)
                    
                    if not '.' in aname:
                        term = f"{table_name}.{aname} {sql_cmp_op} {sql_value}"
                    elif arg.T == "array_length":
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {sql_cmp_op} {value}"
                    else:
                        if cmp_op in ("~", "~*", "!~", "!~*"):
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
                            term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"

                        else:
                            # scalar, array_subscript, array_any
                            term = f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} {cmp_op} {value}'"
                    
            if negate:  term = f"not ({term})"
            parts.append(term)

        if contains_items:
            parts.append("%s.metadata @> '{%s}'" % (table_name, ",".join(contains_items )))
        return parts

    def ___sql_and(self, and_term, table_name):
        

        def sql_literal(v):
            if isinstance(v, str):       v = "'%s'" % (v,)
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
        
        for exp in and_term:
            
            op = exp.T
            args = exp.C
            negate = False

            term = ""

            if op == "present":
                aname = exp["name"]
                if not '.' in aname:
                    term = "true" if aname in DBFile.ColumnAttributes else "false"
                else:
                    term = f"{table_name}.metadata ? '{aname}'"

            elif op == "not_present":
                aname = exp["name"]
                if not '.' in aname:
                    term = "false" if aname in DBFile.ColumnAttributes else "true"
                else:
                    term = f"{table_name}.metadata ? '{aname}'"
            
            else:
                assert op in ("cmp_op", "in_range", "in_set", "not_in_range", "not_in_set")
                arg = args[0]
                assert arg.T in ("array_any", "array_subscript","array_length","scalar")
                negate = exp["neg"]
                aname = arg["name"]
                if not '.' in aname:
                    assert arg.T == "scalar", f"File attribute {aname} value must be a scalar. Got {arg.T} instead"
                    if not aname in DBFile.ColumnAttributes:
                        raise ValueError(f"Unrecognized file attribute \"{aname}\"\n" +
                            "  Allowed file attibutes: " +
                            ", ".join(DBFile.ColumnAttributes)
                        )

                if arg.T == "array_subscript":
                    # a[i] = x
                    aname, inx = arg["name"], arg["index"]
                    inx = json_literal(inx)
                    subscript = f"[{inx}]"
                elif arg.T == "array_any":
                    aname = arg["name"]
                    subscript = "[*]"
                elif arg.T == "scalar":
                    aname = arg["name"]
                    subscript = ""
                elif arg.T == "array_length":
                    aname = arg["name"]
                else:
                    raise ValueError(f"Unrecognozed argument type \"{arg.T}\"")

                # - query time slows down significantly if this is addded
                #if arg.T in ("array_subscript", "array_any", "array_all"):
                #    # require that "aname" is an array, not just a scalar
                #    parts.append(f"{table_name}.metadata @> '{{\"{aname}\":[]}}'")
                
                if op == "in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if not '.' in aname:
                        low = sql_literal(low)
                        high = sql_literal(high)
                        term = f"{table_name}.{aname} between {low} and {high}"
                    elif arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ >= {low} && @ <= {high})'"
                    elif arg.T == "array_length":
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                if op == "not_in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if not '.' in aname:
                        low = sql_literal(low)
                        high = sql_literal(high)
                        term = f"not ({table_name}.{aname} between {low} and {high})"
                    elif arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ < {low} || @ > {high})'"
                    elif arg.T == "array_length":
                        n = "" if negate else "not"
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                elif op == "in_set":
                    if not '.' in aname:
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"{table_name}.{aname} in ({value_list})"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "not" if negate else ""
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} in ({value_list})"
                    else:           # arg.T in ("array_any", "array_subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        or_parts = [f"@ == {v}" for v in values]
                        predicate = " || ".join(or_parts)
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "not_in_set":
                    if not '.' in aname:
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        term = f"not ({table_name}.{aname} in ({value_list}))"
                    elif arg.T == "array_length":
                        values = [sql_literal(v) for v in exp["set"]]
                        value_list = ",".join(values)
                        n = "" if negate else "not"
                        negate = False
                        term = f"not(jsonb_array_length({table_name}.metadata -> '{aname}') {n} in ({value_list}))"
                    else:           # arg.T in ("array_any", "array_subscript","scalar")
                        values = [json_literal(x) for x in exp["set"]]
                        and_parts = [f"@ != {v}" for v in values]
                        predicate = " && ".join(and_parts)
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"
                        
                elif op == "cmp_op":
                    cmp_op = exp["op"]
                    if cmp_op == '=': cmp_op = "=="
                    sql_cmp_op = "=" if cmp_op == "==" else cmp_op
                    value = args[1]
                    value_type, value = value.T, value["value"]
                    sql_value = sql_literal(value)
                    value = json_literal(value)
                    
                    if not '.' in aname:
                        term = f"{table_name}.{aname} {sql_cmp_op} {sql_value}"
                    elif arg.T == "array_length":
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {sql_cmp_op} {value}"
                    else:
                        if cmp_op in ("~", "~*", "!~", "!~*"):
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
                            term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? ({predicate})'"

                        else:
                            # scalar, array_subscript, array_any
                            term = f"{table_name}.metadata @@ '$.\"{aname}\"{subscript} {cmp_op} {value}'"
                    
            if negate:  term = f"not ({term})"
            parts.append(term)

        if contains_items:
            parts.append("%s.metadata @> '{%s}'" % (table_name, ",".join(contains_items )))
        
        parts = [("and " if i > 0 else "") + f"({p})" for i, p in enumerate(parts)]
        return "\n".join(parts)

    def sql(self, table_name):
        if not self.DNF:
            return None
        else:
            out = []
            for i, or_part in enumerate(self.DNF):
                and_parts = self.sql_and(or_part, table_name)
                for j, and_part in enumerate(and_parts):
                    print("and_part:", and_part)
                    if i == 0:
                        prefix = "" if j == 0 else "and "
                    else:
                        prefix = "or  " if j == 0 else "    and "
                    out.append(f"{prefix}( {and_part} )")
            out = "\n".join(out)
            print("returning:\n", out)
            return out