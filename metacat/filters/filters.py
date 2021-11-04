from metacat.db import DBFileSet

#
# Common filters
#

def limited(it, limit):
    if limit is None or limit > 0:
        for x in it:
            yield x
            if limit is not None:
                limit -= 1
                if limit <= 0:
                    break
                    
def skip(it, n):
    for x in it:
        if n is not None and n > 0:
            n -= 1
        else:
            yield n
        
def implement_limit(filter):
    def decorated(self, inputs, params, limit=None, **args):
        unlimited = filter(self, inputs, params, limit=None, **args)
        if limit is not None:
            return limited(unlimited, limit)
        else:
            return unlimited
    return decorated

def implement_skip(filter):
    def decorated(self, inputs, params, skip=None, **args):
        results = filter(self, inputs, params, skip=None, **args)
        if skip is not None:
            return skip(results, skip)
        else:
            return results
    return decorated

class MetaCatFilter(object):

    def __init__(self, config=None):
        pass

    def apply_selection(self, inp, skip, limit, stride=None):
        # stride is not used
        stride_n = stride_i = None
        if stride is not None:
            stride_n, stride_i = stride
        i = 0
        for f in inp:
            if skip is not None and skip > 0:
                skip -= 1
            else:
                if limit == 0:  break
                if stride_n is not None:
                    if i % stride_n == stride_i:
                        yield f
                    i += 1
                else:
                    yield f
                if limit is not None:
                    limit -= 1
    
    def run(self, inputs, params, kv, limit=None, skip=None):
        #
        # selection application order: skip -> limit -> stride
        #
        yield from self.apply_selection(self.filter(inputs, *params, **kv), skip, limit)

class Sample(MetaCatFilter):
    
    def filter(self, inputs, fraction, **ignore):
        file_set = inputs[0]
        x = 0.0
        for f in file_set:
            x += fraction
            if x >= 1.0:
                x -= 1.0
                yield f

class Limit(MetaCatFilter):
    
    def filter(inputs, limit, **ignore):
        file_set = inputs[0]
        return limited(file_set, limit)

class EveryNth(MetaCatFilter):
    
    def filter(self, inputs, modulo, remainder, **ignore):
        file_set = inputs[0]
        i = 0
        for f in file_set:
            if i % modulo == remainder:
                yield f
            i += 1
            
class Hash(MetaCatFilter):
    
    def filter(self, inputs, modulo, remainder, **ignore):
        from zlib import adler32
        file_set = inputs[0]
        for f in file_set:
            r = adler32(f.FID.encode("utf-8")) % modulo
            if r == remainder:
                yield f
            
class Mix(MetaCatFilter):
    
    def filter(self, inputs, *ratios, **ignore):
        import types
        assert len(inputs) == len(ratios)
        assert all(isinstance(inp, DBFileSet) for inp in inputs)
        N = NActive = len(inputs)
        scores = [(0.0, i, iter(fs)) for i, fs in enumerate(inputs)]
        stop = False
        sent_files = set()      # if the same file appears in multiple input sets, make sure it is sent out only once
        while scores and not stop:
            scores = [(s+ratios[i], i, inp) for s, i, inp in scores]
            scores = sorted(scores, reverse=True, key=lambda x: (x[0], x[1]))
            #print("sorted scores:")
            #for s, i, _ in scores:
            #    print(s,i)
            s0, i, it = scores[0]
            scores = [(s-s0, i, inp) for s, i, inp in scores]       # renormalize
            s0 = 0.0
            sent = False
            while not sent:
                try:
                    if isinstance(it, types.GeneratorType):
                        f = it.send(None)
                    else:
                        f = next(it)
                except StopIteration:
                    stop = True
                    break
                else:
                    if not f.FID in sent_files:
                        yield f
                        #print("yielding from", i, f.FID)
                        scores[0] = (s0-1.0, i, it)
                        sent_files.add(f.FID)
                        sent = True

standard_filters = {
    "sample":       Sample(),
    "limit":        Limit(),
    "every_nth":    EveryNth(),
    "mix":          Mix(),
    "hash":         Hash()
}
            
