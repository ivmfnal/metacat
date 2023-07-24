import itertools, io, csv, json
from psycopg2 import IntegrityError

Debug = False

def debug(*parts):
    if Debug:
        print(*parts)
        
Aliases = {}
def alias(prefix="t"):
    global Aliases
    i = Aliases.get(prefix, 1)
    Aliases[prefix] = i+1
    return f"{prefix}_{i}"

class AlreadyExistsError(Exception):
    pass

class DatasetCircularDependencyDetected(Exception):
    pass


class NotFoundError(Exception):
    def __init__(self, msg):
        self.Message = msg

    def __str__(self):
        return "Not found error: %s" % (self.Message,)


def parse_name(name, default_namespace=None):
    words = (name or "").split(":", 1)
    if not words or not words[0]:
        assert not not default_namespace, "Null default namespace"
        ns = default_namespace
        name = words[-1]
    else:
        assert len(words) == 2, "Invalid namespace:name specification:" + name
        ns, name = words
    return ns, name


class MetaValidationError(Exception):
    
    def __init__(self, message, errors):
        self.Errors = errors
        self.Message = message
        
    def as_json(self):
        return json.dumps(
            {
                "message":self.Message,
                "metadata_errors":self.Errors
            }
        )
        
def make_list_if_short(iterable, limit):
    # convert iterable to list if it is short. otherwise return another iterable with the same elements
    
    if isinstance(iterable, (list, tuple)):
        return iterable, None
    
    head = []
    if len(head) < limit:
        for x in iterable:
            head.append(x)
            if len(head) > limit:
                return None, itertools.chain(head, iterable)
        else:
            return head, None
    else:
        return None, iterable
