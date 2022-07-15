import time

class TracePoint(object):
    def __init__(self, name):
        self.Name = name
        self.T0 = None
        self.reset()
        
    def reset(self):
        self.Count = 0
        self.Time = 0.0

    def begin(self):
        self.T0 = time.time()
        return self
        
    def end(self):
        self.Count += 1
        self.Time += time.time() - self.T0
        return self
        
    def stats(self, t0 = 0.0):
        avg = None
        if self.Count > 0:  avg = self.Time/self.Count - t0
        return self.Count, self.Time - t0*self.Count, avg

    def __enter__(self):
        self.begin()

    def __exit__(self, et, ev, tb):
        self.end()

class Tracer(object):

    def __init__(self, calibrate = False):
        self.Points = {}
        self.TZero = 0.0
        if calibrate:   self.calibrate()
        
    def __getitem__(self, name):
        point = self.Points.get(name)
        if point is None:
            point = self.Points[name] = TracePoint(name)
        return point
        
    def begin(self, name):
        return self[name].begin()
        
    def end(self, name):
        return self[name].end()
    
    def stats(self):
        return [(n, p.stats(self.TZero)) for n, p in self.Points.items()]
        
    def formatStats(self):
        lst = self.stats()
        lst.sort()
        out = []
        for name, (count, total, average) in lst:
            out.append("%-40s: %-6d %f %f" % (name, count, total, average))
        return "\n".join(out)
        
    def printStats(self):
        print(self.formatStats())
        
    def reset(self):
        self.Points = {}     
        
    def calibrate(self):
        t = Tracer()
        for _ in range(10000):   
            with t["x"]:
                pass
        tx = t["x"]
        self.TZero = tx.Time/tx.Count
        
if __name__ == '__main__':
    T = Tracer()
    for i in range(100):
        T.op1.begin()
        time.sleep(0.1)
        T.op1.end()
        
        if i % 3:
            T.op2.begin()
            time.sleep(0.2)
            T.op2.end()
        if (i%10) == 0:
            for n, st in T.stats():
                print('%s: %s %s %s' % ((n,)+st))
