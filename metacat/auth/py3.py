import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    import types

    def to_bytes(s):    
        if isinstance(s, bytes):        return s
        elif isinstance(s, str):        return s.encode("utf-8")
        elif isinstance(s, list):       return [to_bytes(x) for x in s]
        elif isinstance(s, tuple):      return tuple(to_bytes(x) for x in s)
        elif isinstance(s, types.GeneratorType):  return (to_bytes(x) for x in s)
        else:
            raise ValueError("Unrecognized type to be converted to bytes: %s %s" % (type(s), s))

    def to_str(s):    
        if isinstance(s, str):          return s
        elif isinstance(s, bytes):      return s.decode("utf-8", "ignore")
        elif isinstance(s, list):       return [to_str(x) for x in s]
        elif isinstance(s, tuple):      return tuple(to_str(x) for x in s)
        elif isinstance(s, types.GeneratorType):  return (to_str(x) for x in s)


else:
    def to_bytes(s):    
        return bytes(s)
    def to_str(b):    
        return str(b)
    
