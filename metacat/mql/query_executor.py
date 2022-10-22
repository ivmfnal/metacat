from .trees import Ascender, Node

class FileQueryExecutor(Ascender):
    
    def __init__(self, db, filters, debug=False):
        self.DB = db
        self.Filters = filters
        self.Debug = False
        
    def debug(self, *params, **args):
        if self.Debug:
            parts = ["FileQueryExecutor:"]+list(params)
            print(*parts, **args)
            
    def __call__(self, tree):
        result = self.walk(tree)
        return result
        
    def empty(self, node, *args):
        return DBFileSet(self.DB)    # empty file set
        
    def sql(self, node, sql=None):
        return DBFileSet.from_sql(self.DB, sql)
        
    def meta_filter(self, node, query=None, meta_exp=None, with_meta=False, with_provenance=False):
        #print("meta_filter: args:", args)
        assert query.T == "file_set"
        evaluator = MetaEvaluator()
        out = (f for f in self.node_to_file_set(query)
                if evaluator(f.metadata(), meta_exp)
        )
        return DBFileSet(self.DB, out)

    def union(self, node, *args):
        return DBFileSet.union(self.DB, args)

    def join(self, node, *args, **kv):
        return DBFileSet.join(self.DB, args)

    def minus(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        assert len(args) == 2
        assert all(n.T in ("sql", "file_set", "empty") for n in args)
        left, right = args
        return left - right

    def parents_of(self, node, arg, with_meta=False, with_provenance=False):
        arg.parents(with_metadata=with_meta, with_provenance=with_provenance))

    def children_of(self, node, *args, with_meta=False, with_provenance=False):
        arg.children(with_metadata=with_meta, with_provenance=with_provenance))

    def skip_limit(self, node, arg, skip=0, limit=None, **kv):
        arg.skip(skip).limit(limit))
            
    def filter(self, node, *queries, name=None, params=[], **kv):
        #print("Evaluator.filter: inputs:", inputs)
        assert name is not None
        filter_object = self.Filters[name]
        queries = [self.node_to_file_set(q) for q in queries]
        limit = node.get("limit")
        skip = node.get("skip", 0)
        kv = node.get("kv", {})
        #print("filter: returning:", node.pretty())
        return DBFileSet(self.DB, filter_object.run(queries, params, kv, limit=limit, skip=skip))
