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
        
def accepts_limit(filter):
    def decorated(self, inputs, params, limit=None, **args):
        unlimited = filter(self, inputs, params, limit=None, **args)
        if limit is not None:
            return limited(unlimited, limit)
        else:
            return unlimited
    return decorated

def accepts_skip(filter):
    def decorated(self, inputs, params, skip=None, **args):
        results = filter(self, inputs, params, skip=None, **args)
        if skip is not None:
            return skip(results, skip)
        else:
            return results
    return decorated

class MetaCatFilter(object):
    
    def filter(inputs, params, limit=None, skip=None):
        raise NotImplementedError()

class Sample(MetaCatFilter):
    
    @accepts_limit
    @accepts_skip
    def filter(self, inputs, params, **ignore):
        file_set = inputs[0]
        fraction = params[0]
        x = 0.0
        for f in file_set:
            x += fraction
            if x >= 1.0:
                x -= 1.0
                yield f

class Limit(MetaCatFilter):
    
    def filter(inputs, params, **ignore):
        file_set = inputs[0]
        limit = params[0]
        return limited(file_set, limit)

class EveryNth(MetaCatFilter):
    
    @accepts_limit
    @accepts_skip
    def filter(self, inputs, params, **ignore):
        from zlib import adler32
        file_set = inputs[0]
        modulo, remainder = params
        i = 0
        for f in file_set:
            if i % modulo == remainder:
                yield f
            i += 1
            
class Hash(MetaCatFilter):
    
    @accepts_limit
    @accepts_skip
    def filter(self, inputs, params, **ignore):
        from zlib import adler32
        file_set = inputs[0]
        modulo, remainder = params
        for f in file_set:
            r = adler32(f.FID.encode("utf-8")) % modulo
            if r == remainder:
                yield f
            
class Mix(MetaCatFilter):
    
    @accepts_limit
    @accepts_skip

    def filter(self, inputs, ratios, **ignore):
        assert len(inputs) == len(ratios)
        N = NActive = len(inputs)
        scores = [(0.0, i, it) for i, it in enumerate(inputs)]
        stop = False
        sent_files = set()      # if the same file appears in multiple input sets, make sure it is sent out only once
        while scores and not stop:
            scores = [(s+ratios[i], i, inp) for s, i, inp in scores]
            scores = sorted(scores, reverse=True)
            s, i, it = scores[0]
            sent = False
            while not sent:
                try:  f = next(it)
                except StopIteration:
                    stop = True
                    break
                else:
                    if not f.FID in sent_files:
                        yield f
                        scores[0] = (s-1.0, i, it)
                        sent_files.add(f.FID)
                        sent = True

standard_filters = {
    "sample":       Sample,
    "limit":        Limit,
    "every_nth":    EveryNth,
    "mix":          Mix,
    "hash":         Hash
}
            
