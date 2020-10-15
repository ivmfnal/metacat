from dbobjects import DBDataset, DBFile
import json

"""
dataset_exp:
    [ 'from' <name> <meta_exp> ]
    | [ 'from' <name> ]

meta_exp:
    [ <attr_name> <cmp_op> <value> ]
    | [ <bool_op> <meta_exp> ... ]

expression:
    <dataset_exp>
    | [ <bool_op> <expression> ... ]
    | [ <expression> <filter> ]

filter:
    [ 'filter' <name> <param> ... ]
    | [ 'filter' <name> ]

cmp_op: '>' '<' '>=' '<=' '==' '!=' 'in'

bool_op: 'and' 'or' 'minus' 'not'


JSON representation:

expression:
    ["bool_op", expression, ...]
    ["from", "dataset", meta_expression]
    ["filter", "name", [params], expression, ... ]
    
meta_expression:
    ["bool_op", meta_expression, ...]
    ["attr_name", "cmp_op", value]
    
"""

class Expression(object):

    BOOL_OPS = set(["and", "or", "minus", "not"])
    CMP_OPS = set(["<", ">", "<=", ">=", "==", "!=", "in"])

    def __init__(self, db, exp, filters={}):
        assert isinstance(exp, list)
        self.Exp = exp
        self.DB = db
        self.Filters = filters
        
    @staticmethod
    def from_json(json_text):
        return Expression(json.loads(json_text))
        
    def evaluate(self):
        return self.evaluate_expression(self.Exp)
        
    __call__ = evaluate

    def import_filter(self, name):
        mod = __import__("filters", globals(), locals(), [], 0)
        return getattr(mod, name)
        
    def evaluate_expression(self, exp):
        assert isinstance(exp, list) and len(exp) > 0
        if exp[0] == "from":
            assert len(exp) in (2, 3)
            namespace, name = exp[1].split(":")
            if len(exp) == 3:
                return self.evaluate_dataset_expression(namespace, name, exp[2])
            else:
                return self.evaluate_dataset_expression(namespace, name, [])
        elif exp[0] in self.BOOL_OPS:
            assert len(exp) > 1
            lst = self.combine_lists(exp[0], exp[1:])
            return lst
        elif exp[0] == "filter":
            name, params = exp[1:3]
            inputs = [self.evaluate_expression(x) for x in exp[3:]]
            filter_function = self.Filters[name]
            return filter_function(inputs, params)
        else:
            raise ValueError("Unknown expression header %s" % (exp[0],))

    def combine_lists(self, op, expressions):
        assert op in self.BOOL_OPS and op != "not"
        assert len(expressions) > 0

        if op == "minus":
            assert len(expressions) == 2
            left, right = expressions
            left_files = list(self.evaluate_expression(left))
            left_ids = set(f.FID for f in left_files)
            right_ids = set(f.FID for f in self.evaluate_expression(right))
            result_ids = left_ids - right_ids
            return (f for f in left_files if f.FID in result_ids)

        elif op == "and":
            first = self.evaluate_expression(expressions[0])
            if len(expressions) == 1:
                return first
            file_list = list(first)
            file_ids = set(f.FID for f in file_list)
            for another in expressions[1:]:
                another_ids = set(f.FID for f in self.evaluate_expression(another))
                file_ids &= another_ids
            return (f for f in file_list if f.FID in file_ids)

        elif op == "or":
            if len(expressions) == 1:
                return self.evaluate_expression(expressions[0])
            def union(file_lists):
                first = file_lists[0]
                if len(file_lists) == 1:
                    return first
                file_ids = set()
                for lst in file_lists:
                    for f in lst:
                        if not f.FID in file_ids:
                            file_ids.add(f.FID)
                            yield f
            return union([self.evaluate_expression(exp) for exp in expressions])
        else:
            raise ValueError("Invalid list combining operation '%s'" % (op,))
            
    def evaluate_dataset_expression(self, namespace, name, meta_expression):
        #print("evaluate_dataset_expression:", namespace, name, meta_expression)
        dataset = DBDataset.get(self.DB, namespace, name)
        condition = None if not meta_expression else self.meta_exp_to_sql(meta_expression)
        files = dataset.list_files(condition=condition, with_metadata=bool(meta_expression))
        return files
        if meta_expression:
            return (f for f in files if self.evaluate_meta_expression(f, meta_expression))
        else:
            return files
        
    def evaluate_meta_expression(self, f, meta_expression):
        op = meta_expression[0]
        if op in self.BOOL_OPS:
            args = meta_expression[1:]
            if op == 'and':
                ok = self.evaluate_meta_expression(f, args[0])
                if ok and args[1:]:
                    ok = self.evaluate_meta_expression(f, ["and"] + args[1:])
                return ok
            elif op == 'or':
                ok = self.evaluate_meta_expression(f, args[0])
                if not ok and args[1:]:
                    ok = self.evaluate_meta_expression(f, ["or"] + args[1:])
                return ok
            elif op == 'not':
                return not self.evaluate_meta_expression(f, args[1])
            else:
                raise ValueError("Unrecognized boolean operation '%s'" % (op,))
        else:
            # <attr> <cmp> <value>
            name, op, value = meta_expression
            attr_value = f.get_attribute(name, None)
            if op == "<":          return attr_value < value
            elif op == ">":        return attr_value > value
            elif op == "<=":       return attr_value <= value
            elif op == ">=":       return attr_value >= value
            elif op in ("==",'='):       return attr_value == value
            elif op == "!=":       return attr_value != value
            elif op == "in":       return value in attr_value       # exception, e.g.   123 in event_list
            else:
                raise ValueError("Invalid comparison operator '%s' in %s" % (op, meta_expression))

    @staticmethod                
    def meta_exp_to_sql(meta_expression):
        op = meta_expression[0]
        if op in Expression.BOOL_OPS:
            args = meta_expression[1:]
            if op in ('or','and'):
                return (' ' + op + ' ').join([
                    '(' + Expression.meta_exp_to_sql(part) + ')' for part in args])
            elif op == 'not':
                return ' not (' + self.meta_exp_to_sql(args[1]) + ')'
            else:
                raise ValueError("Unrecognized boolean operation '%s'" % (op,))
        else:
            # <attr> <cmp> <value>
            name, op, value = meta_expression
            if op in ('<', '>', '<=', '>=', '==', '=', '!='):
                sql_op = '=' if op == '==' else op
                if isinstance(value, bool): colname = "bool_value"
                elif isinstance(value, int): colname = "int_value"
                elif isinstance(value, float): colname = "float_value"
                elif isinstance(value, str): colname = "string_value"
                else:
                        raise ValueError("Unrecognized value type %s for attribute %s" % (type(value), name))
                return "attr.name='%s' and attr.%s %s '%s'" % (name, colname, op, value)
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
        
                            
        
        
        
        
            

    
            
