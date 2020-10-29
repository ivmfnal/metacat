#
# f(A * (x + y)) = A*f(x) + A*f(y)
# f(a * (x + y)) = 
# f(x + y) = f(x) + f(y)
# 

import pprint


def term(x):
    return isinstance(x, str)

def flatten(exp):
    if term(exp): return exp
    op = exp[0]
    parts = []
    for p in exp[1:]:
        flat_p = flatten(p)
        if flat_p[0] == op:
            parts += flat_p[1:]
        else:
            parts.append(flat_p)
    return [op] + parts

def negate(exp):
    if term(exp):
        if exp[0] == "!":   return exp[1:]
        else:               return "!" + exp
    if exp[0] == "!":
        return exp[1]
    elif exp[0] == "+":
        return ["*"] + [negate(x) for x in exp[1:]]
    elif exp[0] == "*":
        return ["+"] + [negate(x) for x in exp[1:]]

def apply_nots(exp):
    if term(exp):   return exp
    if exp[0] == "!":
        return negate(exp[1])
    else:
        return [exp[0]] + [apply_nots(x) for x in exp[1:]]


def walk_down(exp, fcn):
    print("walk_down: input: ", pprint.pformat(exp))
    if term(exp):   return exp
    children = []
    for e in exp[1:]:
        children.append(walk_down(e, fcn))
    print("walk_down: children:", pprint.pformat(children))
    exp = fcn(exp[0], children)
    print("walk_down: output: ", pprint.pformat(exp))
    return exp
    
def simplify(exp):
    if term(exp):
        out = exp
    if len(exp) == 2:
        out = exp[1]
    else:
        #if len(exp) == 1:
        #    return "1"
        terms = [x for x in exp[1:] if term(x)]
        terms = set(terms)
        for x in terms:
            if '!'+x in terms:
                if exp[0] == '*':   return '0'
                elif exp[0] == '+': return '1'
        out = [exp[0]] + list(terms) + [x for x in exp[1:] if not term(x)]
    print("simplify:", exp, "->", out)
    return out

def cvt(op, children):
    print("cvt: input:", op, pprint.pformat(children))
    assert len(children) > 0    
    if len(children) == 1:
        return dnf(children[0])
    if op == "*":
        terms = []
        lst = children[:]
        while lst:
            c = lst.pop()
            if term(c):
                terms.append(c)
            else:
                assert c[0] == "+", f"Expetced flattenend expression. Got: {op} {children}"
                assert len(c) >= 2, f"Expression is too short: {op} {children}"
                head = c[1]
                tail = simplify(['+'] + c[2:])
                rest = simplify(['*'] + terms + lst)
                print("cvt: head:", head)
                print("     tail:", tail)
                print("     rest:", rest)
                
                if rest:
                    if tail:
                        new_exp = ["+",
                            ["*", head, rest],
                            ["*", tail, rest]
                        ]
                    else:
                        new_exp = ["+",
                            ["*", head, rest],
                            rest
                        ]
                else:
                    if tail:
                        new_exp = ["+", head, tail]
                    else:
                        new_exp = head
                print("     new_exp:", new_exp)
                return dnf(new_exp)
        terms = set(terms)
        for x in terms:
            if '!' + x in terms:
                terms = ['0']
                break
        terms = list(terms)
        if len(terms) == 1:
            out = terms[0]
        else:
            out = ["*"] + terms
    elif op == "+":
        out = ["+"] + children
    print("cvt: output: ", pprint.pformat(out))
    return out

def dnf(exp):
    exp = flatten(exp)
    exp = walk_down(exp, cvt)
    exp = flatten(exp)        
    assert term(exp) or exp[0] == '+'
    #if "1" in exp[1:]:
    #    return "1"        
    #exp = ["+"] + [simplify(x) for x in exp[1:]]
    #exp = ["+"] + [x for x in exp[1:] if x != "0"]
    #if "1" in exp:  exp = "1"
    return exp

exp = ["*",
    #["+", "A", "B"],
    ["+", "A", "!A"],
    ["+", "x", 
        ["*", "p", "q"]
    ]
]

print("Expression:", pprint.pformat(exp))

exp = apply_nots(exp)

print("After nots:", pprint.pformat(exp))

exp = dnf(exp)

print("DNF:       ", pprint.pformat(exp))
