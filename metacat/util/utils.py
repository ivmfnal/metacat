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

