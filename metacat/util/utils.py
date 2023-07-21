from textwrap import dedent, indent


def insert_sql_one(outer, tag, text):
    lines = dedent(outer).split("\n")
    text = dedent(text)
    out_lines = []
    marker = "$" + tag
    for line_no, line in enumerate(lines):
        line = line.expandtabs(4)
        if marker not in line:
            out_lines.append(line)
            continue
        i = line.find(marker)
        text = indent(text, " "*i)
        out_lines += text.split("\n")
        break
    out_lines += lines[line_no+1:]
    return "\n".join(out_lines)
    
def insert_sql(outer, **tags):
    out = outer
    for tag, value in tags.items():
        out = insert_sql_one(out, tag, value)
    return out

def first_not_empty(lst):
    val = None
    for v in lst:
        val = v
        if v is not None and not (isinstance(v, list) and len(v) == 0):
            return v
    else:
        return val
        
