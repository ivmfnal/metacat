def chunked(iterable, n):
    if isinstance(iterable, (list, tuple)):
        for i in range(0, len(iterable), n):
            yield iterable[i:i+n]
    else:
        chunk = []
        for item in iterable:
            chunk.append(item)
            if len(chunk) >= n:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
            
def unique(iterable, key=None):
    seen = set()
    for item in iterable:
        k = item if key is None else key(item)
        if not k in seen:
            seen.add(k)
            yield item

def first_not_empty(lst):
    val = None
    for v in lst:
        val = v
        if v is not None and not (isinstance(v, list) and len(v) == 0):
            return v
    else:
        return val
        
def limited(iterable, n):
    for f in iterable:
        if n is None:
            yield f
        else:
            if n or n > 0:
                yield f
            else:
                break
            n -= 1
            
def strided(iterable, n, i=0):
    for j, f in enumerate(iterable):
        if j%n == i:
            yield f
            
def skipped(iterable, n):
    for f in iterable:
        if n > 0:
            n -= 1
        else:
            yield f
            
