from metacat.db import DBFileSet
import random
from metacat.util import strided, limited, skipped

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
        self.Config = config

    def run(self, inputs, params, kv, limit=None, skip=None, stride=None):
        #
        # selection application order: skip -> limit -> stride
        #
        return strided(
            limited(
                skipped(
                    self.filter(inputs, *params, **kv), 
                    skip
                ),
                limit
            ),
            stride
        )

class Sample(MetaCatFilter):
    """
    Inputs: single file set
    
    Parameters:
        fraction: floating point number from 0.0 to 1.0
    
    Output: Randomly picked subset of the input file set
    """
    
    def filter(self, inputs, fraction, **ignore):
        file_set = inputs[0]
        x = 0.0
        for f in file_set:
            x += fraction
            if x >= 1.0:
                x -= 1.0
                yield f

class Limit(MetaCatFilter):
    """
    Inputs: single file set
    
    Parameters:
        limit: integer
    
    Output: First <limit> files from the input file set
    """
    
    def filter(self, inputs, limit, **ignore):
        file_set = inputs[0]
        return limited(file_set, limit)

class EveryNth(MetaCatFilter):
    """
    Inputs: single file set
    
    Parameters:
        modulo: integer
        remainder: integer, from 0 to <modulo>-1
    
    Output: Every <modulo>'th file from the input file set, starting from <remainder>. 
        The output depends on the order of the files in the input set.
    """
    
    def filter(self, inputs, modulo, remainder, **ignore):
        file_set = inputs[0]
        i = 0
        for f in file_set:
            if i % modulo == remainder:
                yield f
            i += 1
            
class Hash(MetaCatFilter):
    """
    Inputs: single file set
    
    Parameters:
        modulo: integer
        remainder: integer, from 0 to <modulo>-1
    
    Output: Approximately every <modulo>'th file from the input file set. The filter calculates Adler32 on each file id and outputs the file
        if the <adler32 hash> % <modulo> == <remainder>. The output does not depend on the order of files in the input set.
    """
    
    def filter(self, inputs, modulo, remainder, **ignore):
        from zlib import adler32
        file_set = inputs[0]
        for f in file_set:
            r = adler32(f.FID.encode("utf-8")) % modulo
            if r == remainder:
                yield f
                
class Randomize(MetaCatFilter):
    
    """
    Inputs: single file set
    
    Keyword arguments:
        seed: integer, random number generator seed. If missing, seed will be random.
        window: integer, randomization window - the wider the window, the more random the output will be. The distance from original index of the
            file to its randomaized index will be around the window. Default=1000.
    
    Output: Returns the same files as in the input set, but in randomized order.
    """
    
    def filter(self, inputs, seed=None, window=1000):
        rng = random.Random(seed)
        saved = [None] * window
        for f in inputs[0]:
            i = rng.randint(0, window-1)
            s = saved[i]
            if s is not None:
                yield s
            saved[i] = f
        for f in saved:
            if f is not None:
                yield f
            
class Mix(MetaCatFilter):
    """
    Inputs: multiple file sets
    
    Parameters:
        *ratios - floating point numbers, do not have to be normalized
    
    Output: Mixes files from the input file sets proportionally to the ratios. Stops when one of the input file sets is exhausted.
    """
    
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
    "hash":         Hash(),
    "randomize":    Randomize()
}
            
