from metacat.common.trees  import Ascender, Node
from metacat.db import DBFileSet
from .meta_evaluator import MetaEvaluator

class FileQueryExecutor(Ascender):
    
    # the assumption is that the entire tree consists of:
    # Node(T="sql") and DBFileSet objects
    
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
        #print("sql:", sql)
        return DBFileSet(self.DB, sql=sql)
        
    def meta_filter(self, node, query=None, meta_exp=None, with_meta=False, with_provenance=False):
        evaluator = MetaEvaluator()
        filtered_files = (f for f in query if evaluator(f, meta_exp))
        return DBFileSet(self.DB, filtered_files)

    def union(self, node, *args):
        return DBFileSet.union(self.DB, args)

    def join(self, node, *args, **kv):
        return DBFileSet.join(self.DB, args)
        
    def ordered(self, node):
        assert isinstance(node, DBFileSet)
        return node.ordered()

    def minus(self, node, *args, **kv):
        #print("Evaluator.union: args:", args)
        assert len(args) == 2
        assert all(isinstance(n, DBFileSet) or n.T == "sql" for n in args)
        left, right = args
        return left - right

    def parents_of(self, node, arg, with_meta=False, with_provenance=False):
        return arg.parents(as_files=True, with_metadata=with_meta, with_provenance=with_provenance)

    def children_of(self, node, *args, with_meta=False, with_provenance=False):
        return args[0].children(as_files=True, with_metadata=with_meta, with_provenance=with_provenance)

    def skip_limit(self, node, arg, skip=0, limit=None, **kv):
        return arg.skip(skip).limit(limit)
            
    def filter(self, node, *queries, name=None, params=[], kw={}, 
                skip=0, limit=None,
                with_meta=False, ordered=False):
        #print("Evaluator.filter: inputs:", inputs)
        assert name is not None
        filter_object = self.Filters[name]
        #print("filter: queries:", queries)
        return DBFileSet(self.DB, filter_object.run(queries, params, kw, 
                limit=limit, skip=skip, with_meta=with_meta, ordered=ordered))
