from .trees import Ascender

class MetaExpressionDNF(object):
    
    def __init__(self, exp):
        #
        # meta_exp is a nested list representing the query filter expression in DNF:
        #
        # meta_exp = [meta_or, ...]
        # meta_or = [meta_and, ...]
        # meta_and = [(op, aname, avalue), ...]
        #
        debug("===MetaExpressionDNF===")
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
    
    def sql_and(self, and_term, table_name):
        

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
            debug("sql_and:")
            debug(exp.pretty("    "))
            
            op = exp.T
            args = exp.C
            negate = False


            term = ""

            if op == "present":
                aname = exp["name"]
                term = f"{table_name}.metadata ? '{aname}'"

            elif op == "not_present":
                aname = exp["name"]
                term = f"not ({table_name}.metadata ? '{aname}')"
            
            else:
                assert op in ("cmp_op", "in_range", "in_set", "not_in_range", "not_in_set")
                arg = args[0]
                assert arg.T in ("array_any", "array_subscript","array_length","scalar")
                negate = exp["neg"]
                
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
                    raise ValueError(f"Unrecognozed argument type={arg.T}")

                parts.append(f"{table_name}.metadata ? '{aname}'")

                    
                # - query time slows down significantly if this is addded
                #if arg.T in ("array_subscript", "array_any", "array_all"):
                #    # require that "aname" is an array, not just a scalar
                #    parts.append(f"{table_name}.metadata @> '{{\"{aname}\":[]}}'")
                
                if op == "in_range":
                    assert len(args) == 1
                    typ, low, high = exp["type"], exp["low"], exp["high"]
                    low = json_literal(low)
                    high = json_literal(high)
                    if arg.T in ("array_subscript", "scalar", "array_any"):
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
                    if arg.T in ("array_subscript", "scalar", "array_any"):
                        term = f"{table_name}.metadata @? '$.\"{aname}\"{subscript} ? (@ < {low} || @ > {high})'"
                    elif arg.T == "array_length":
                        n = "" if negate else "not"
                        negate = False
                        term = f"jsonb_array_length({table_name}.metadata -> '{aname}') {n} between {low} and {high}"
                        
                elif op == "in_set":
                    if arg.T == "array_length":
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
                    if arg.T == "array_length":
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
                    value = json_literal(value)
                    
                    if arg.T == "array_length":
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
            
        if Debug:
            print("sql_and():")
            print(" and_terms:")
            for t in and_term:
                print(t.pretty("    "))
            print("output parts:")
            for p in parts:
                print("      ", p)
            
        return " and ".join([f"({p})" for p in parts])
        
    def sql(self, table_name):
        if self.DNF:
            return " or ".join([self.sql_and(t, table_name) for t in self.DNF])
        else:
            return " true "

class SQL(object):
    
    def sql(self):  # virtual
        raise NotImplementedError
        
    def __and__(self, other):
        my_sql = self.sql
        



class BasicQuery(SQL):
    
    TempID = 1
    
    def __init__(self, namespace, dataset_pattern, with_meta, meta_exp, limit):
        self.Namespace = namespace
        self.DatasetPattern = dataset_pattern
        self.MetaExp = meta_exp
        self.WithMeta = with_meta
        self.Limit = limit
    
    @staticmethod
    def next_id():
        i = SQLQuery.TempID
        SQLQuery.TempID += 1
        return i
        
    @staticmethod
    def temp_name():
        i = SQLQuery.next_id()
        return f"t_{i}"
            
    def sql(self):
        meta = "f.metadata" if self.WithMeta else "null"
        temp_f_name = self.temp_name("f")
        temp_fd_name = self.temp_name("fd")
        meta_where_clause = MetaExpressionDNF(condition).sql(temp_f_name)
        limit = f"limit {self.Limit}" if self.Limit else ""
        sql = f"""select {temp_f_name}.id, {temp_f_name}.namespace, {temp_f_name}.name, {meta} as metadata
                    from files {temp_f_name}
                    inner join files_datasets {temp_fd_name} on {temp_fd_name}.file_id = {temp_fd_name}.id
                    where fd.dataset_namespace='{self.Namespace}' and {temp_fd_name}.dataset_name like '{self.DatasetPattern}' and
                        {meta_where_clause}
                    {limit}
                    """
        return sql
        
class SQLInterpreter(Ascender):
    
    def __init__(self, db, with_meta = True):
        self.DB = db
        self.WithMeta = False
        
    NextID = 1
    
    @staticmethod
    def next_id():
        i = SQLInterpreter.NextID
        SQLInterpreter.NextID += 1
        return i
        
    @staticmethod
    def temp_name(prefix=t):
        i = SQLInterpreter.next_id()
        return f"{prefix}_{i}"
            
    def basic_file_query(self, node, *args, query=None):
        assert isinstance(query, BasicFileQuery)
        #print("_FileEvaluator:basic_file_query: query:", query.pretty())
        #print("FileEvaluator.basic_file_query: query.WithMeta:", query.WithMeta)
        #return DBFileSet.from_basic_query(self.DB, query, self.WithMeta or query.WithMeta, self.Limit)
        return Node("sql", sql=query.sql())
    
    def union(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set") for n in args)
        sqls = [n for n in args if n.T == "sql"]
        file_sets = [n for n in args if n.T == "file_set"]
        from_file_sets = DBFileSet.union(self.DB, [n["file_set"] for n in file_sets]) if file_sets else None
        u_sql = None if not sqls else """
            select id, namespace, name, metadata from
            (
                %s
            )
            """ % ("\nunion\n".join([n["sql"] for n in sqls]))
        
        if not file_sets:
            return Node("sql", sql=u_sql)        
        elif not u_sql:
            return Node("file_set", file_set=from_file_sets)
        else:
            from_sql = DBFileSet.from_sql(u_sql)
            return DBFileSet.union(self.DB, [from_sql, from_file_sets])


    def join(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert all(n.T in ("sql","file_set") for n in args)
        sqls = [n for n in args if n.T == "sql"]
        file_sets = [n for n in args if n.T == "file_set"]
        from_file_sets = DBFileSet.union(self.DB, [n["file_set"] for n in file_sets]) if file_sets else None
        u_sql = None if not sqls else """
            select id, namespace, name, metadata from
            (
                %s
            )
            """ % ("\nintersect\n".join([n["sql"] for n in sqls]))
        
        if not file_sets:
            return Node("sql", sql=u_sql)        
        elif not u_sql:
            return Node("file_set", file_set=from_file_sets)
        else:
            from_sql = DBFileSet.from_sql(u_sql)
            return DBFileSet.join(self.DB, [from_sql, from_file_sets])

    def minus(self, node, *args):
        #print("Evaluator.union: args:", args)
        assert len(args) == 2
        assert all(n.T in ("sql","file_set") for n in args)
        left, right = args
        if left.T == "sql" and right.T == "sql":
            s1 = left["sql"]
            s2 = right["sql"]
            sql = f"""
            select id, namespace, name, metadata from
            (
                {s1} except {s2}
            )
            """
            return Node("sql", sql=sql)
        else:
            left_set = left["file_set"] if left.T == "file_set" else DBFileSet.from_sql(left["sql"])
            right_set = right["file_set"] if right.T == "file_set" else DBFileSet.from_sql(right["sql"])
            return left_set - right_set

    def parents_of(self, node, *args):
        assert len(args) == 1
        arg = args[0]
        assert arg.T in ("sql","file_set")
        if arg.T == "sql":
            arg_sql = arg["sql"]
            p = self.temp_name("p")
            c = self.temp_name("c")
            pc = self.temp_name("pc")
            meta = "{p}.metadata" if self.WithMeta else "null"
            new_sql = f"""
                select {p}.id, {p}.namespace, {p}.name, {meta} as metadata
                from files {p}
                    inner join parent_child {pc} on {p}.id = {pc}.parent_id
                    inner join ({arg_sql}) as {c} on {c}.id = {pc}.child_id
            """
            return Node("sql", sql=new_sql)
        else:
            return arg["file_set"].parents(with_metadata=self.WithMeta)

    def children_of(self, node, *args):
        assert len(args) == 1
        arg = args[0]
        assert arg.T in ("sql","file_set")
        if arg.T == "sql":
            arg_sql = arg["sql"]
            p = self.temp_name("p")
            c = self.temp_name("c")
            pc = self.temp_name("pc")
            meta = "{c}.metadata" if self.WithMeta else "null"
            new_sql = f"""
                select {c}.id, {c}.namespace, {c}.name, {meta} as metadata
                from files {c}
                    inner join parent_child {pc} on {c}.id = {pc}.child_id
                    inner join ({arg_sql}) as {p} on {p}.id = {pc}.parent_id
            """
            return Node("sql", sql=new_sql)
        else:
            return arg["file_set"].children(with_metadata=self.WithMeta)


