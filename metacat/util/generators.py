def fetch_generator(c):
    while True:
        tup = c.fetchone()
        if tup is None: break
        yield tup
