def fetch_generator(c):
    while True:
        tup = c.fetchone()
        if tup is None: break
        yield tup

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

def limited(iterable, n):
    if n is None:
        yield from iterable
    if isinstance(iterable, (list, tuple)):
        yield from iterable[:n]
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
    if n is None:
        yield from iterable
    for j, f in enumerate(iterable):
        if j%n == i:
            yield f
            
def skipped(iterable, n):
    if n is None:
        yield from iterable
    if isinstance(iterable, (list, tuple)):
        yield from iterable[n:]
    for f in iterable:
        if n > 0:
            n -= 1
        else:
            yield f
