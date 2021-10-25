#
# Replaced witn SQLConverter
#


class _FileEvaluator(Ascender):

    def __init__(self, db, filters, with_meta, limit):
        Ascender.__init__(self)
        self.Filters = filters
        self.DB = db
        self.WithMeta = with_meta
        self.Limit = limit
        
    def file_query(self, node, query, limit=None):
        return query if limit is None else limited(query, limit)
        
    def meta_filter(self, node, files=None, meta_exp=None):
        #print("meta_filter: args:", args)
        evaluator = MetaEvaluator()
        if meta_exp is not None:
            out = []
            for f in files:
                if evaluator(f.metadata(), meta_exp):
                    out.append(f)
            return DBFileSet(self.DB, out)
            return DBFileSet(self.DB, (f for f in files if evaluator(f.metadata(), meta_exp)))
        else:
            return files

    def parents_of(self, node, files):
        return files.parents(with_metadata=True)

    def children_of(self, node, files):
        return files.children(with_metadata=True)

    def limit(self, node, files, limit=None):
        #print("FileEvaluator.limit(): args:", args)
        assert isinstance(files, DBFileSet)
        return files if limit is None else files.limit(limit)
            
    def basic_file_query(self, node, *args, query=None):
        assert isinstance(query, BasicFileQuery)
        #print("_FileEvaluator:basic_file_query: query:", query.pretty())
        #print("FileEvaluator.basic_file_query: query.WithMeta:", query.WithMeta)
        return DBFileSet.from_basic_query(self.DB, query, self.WithMeta or query.WithMeta, self.Limit)
        
    def union(self, node, *args):
        #print("Evaluator.union: args:", args)
        return DBFileSet.union(self.DB, args)
        
    def join(self, node, *args):
        return DBFileSet.join(self.DB, args)
        
    def minus(self, node, left, right):
        assert isinstance(left, DBFileSet)
        assert isinstance(right, DBFileSet)
        return left - right

    def filter(self, node, *queries, name=None, params=[]):
        #print("Evaluator.filter: inputs:", inputs)
        assert name is not None
        filter_function = self.Filters[name]
        return DBFileSet(self.DB, filter_function(queries, params))
