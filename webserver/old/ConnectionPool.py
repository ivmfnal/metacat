import time
from threading import RLock, Thread


class _WrappedConnection(object):
    #
    # The pool can be used in 2 ways:
    #
    # 1. explicit connection
    #   
    #   conn = pool.connect()
    #   ....
    #   conn.close()
    #
    #   conn = pool.connect()
    #   ...
    #   # connection goes out of scope and closed during garbage collection
    #
    # 2. via context manager
    #
    #   with pool.connect() as conn:
    #       ...
    #

    def __init__(self, pool, connection):
        self.Connection = connection
        self.Pool = pool
        
    def __str__(self):
        return "WrappedConnection(%s)" % (self.Connection,)

    def _done(self):
        if self.Pool is not None:
            self.Pool.returnConnection(self.Connection)
            self.Pool = None
        if self.Connection is not None:
            self.Connection = None
    
    #
    # If used via the context manager, unwrap the connection
    #
    def __enter__(self):
        return self.Connection
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._done()
    
    #
    # If used as is, instead of deleting the connection, give it back to the pool
    #
    def __del__(self):
        self._done()
    
    def close(self):
        if self.Connection is not None:
            self.Connection.close()
            self.Connection = None
            self.Pool = None
    
    #
    # act as a database connection object
    #
    def __getattr__(self, name):
        return getattr(self.Connection, name)

class _IdleConnection(object):
    def __init__(self, conn):
        self.Connection = conn          # db connection
        self.IdleSince = time.time()

class ConnectorBase(object):

    def connect(self):
        raise NotImplementedError
        
    def probe(self, connection):
        return True
        
    def connectionIsClosed(self, c):
        raise NotImplementedError
        
class DummyConnection(object):

    Lock = RLock()
    Count = 0
    NextID = 1

    def __init__(self):
        with DummyConnection.Lock:
            DummyConnection.Count += 1
            self.ID = DummyConnection.NextID
            DummyConnection.NextID += 1

        print ("Connection created: >> %s" % (self,))

    def __str__(self):
        return "<connection %d>" % (self.ID,)
        
    def close(self):
        print ("Connection closed:  << %s" % (self,))
        with self.Lock:
            DummyConnection.Count -= 1
        

class DummyConnector(ConnectorBase):

    def connect(self):
        return DummyConnection()
        
    def probe(self, connection):
        return True
        
    def connectionIsClosed(self, connection):
        return False



class PsycopgConnector(ConnectorBase):

    def __init__(self, connstr):
        ConnectorBase.__init__(self)
        self.Connstr = connstr
        
    def connect(self):
        import psycopg2
        return psycopg2.connect(self.Connstr)
        
    def connectionIsClosed(self, conn):
        return conn.closed
        
    def probe(self, conn):
        try:
            c = conn.cursor()
            c.execute("rollback; select 1")
            alive = c.fetchone()[0] == 1
            c.execute("rollback")
            return alive
        except:
            return False
            
class MySQLConnector(ConnectorBase):
    def __init__(self, connstr):
        raise NotImplementedError
        
              
class ConnectionPool(object):      

    class CleanUpThread(Thread):    

        def __init__(self, idle_list, lock, idle_timeout):
            Thread.__init__(self)
            self.IdleTimeout = idle_timeout
            self.Interval = max(1.0, float(self.IdleTimeout)/2.0)
            self.IdleList = idle_list
            self.Lock = lock

        def run(self):
            while True:
                time.sleep(self.Interval)
                with self.Lock:
                    now = time.time()
                    #print("cleanUp: idle connections: %d %x %s" % (len(self.IdleConnections), id(self.IdleConnections), self.IdleConnections))

                    for ic in self.IdleList[:]:
                        t = ic.IdleSince
                        c = ic.Connection
                        if t < now - self.IdleTimeout:
                            print ("closing idle connection %x" % (id(c),))
                            c.close()
                            self.IdleList.remove(ic)



    def __init__(self, postgres=None, mysql=None, connector=None, 
                idle_timeout = 30, max_idle_connections = 1):
        self.IdleTimeout = idle_timeout
        if connector is not None:
            self.Connector = connector
        elif postgres is not None:
            self.Connector = PsycopgConnector(postgres)
        elif mysql is not None:
            self.Connector = MySQLConnector(mysql)
        else:
            raise ValueError("Connector must be provided")
        self.IdleConnections = []           # available, [_IdleConnection(c), ...]
        self.Lock = RLock()
        self.CleanerThread = None
        self.MaxIdleConnections = max_idle_connections
        
    def idleCount(self):
        with self.Lock:
            return len(self.IdleConnections)
        
    def connect(self):
        print("ConnectionPool.connect: waiting for lock...")
        with self.Lock:
            print("ConnectionPool.connect: lock acquired")
            use_connection = None
            print ("ConnectionPool.connect: IdleConnections:", self.IdleConnections)
            while self.IdleConnections:
                c = self.IdleConnections.pop().Connection
                print("ConnectionPool.connect: probing:", c)
                if self.Connector.probe(c):
                    use_connection = c
                    print ("connect: reuse idle connection %s" % (c,))
                    break
                else:
                    print ("connect: probe failed for %s" % (c,))
                    c.close()       # connection is bad
            else:
                # no connection found
                print("ConnectionPool.connect: calling the connector...")
                use_connection = self.Connector.connect()
                #print ("connect: new connection %s" % (use_connection,))
            print("ConnectionPool.connect: returning wrapped connection (%s)" % (use_connection,))
            return _WrappedConnection(self, use_connection)
        
    def returnConnection(self, c):
        print("ConnectionPool.returnConnection(%x)" % (id(c),))
        if self.Connector.connectionIsClosed(c):  
            print("ConnectionPool.returnConnection: connection %x is closed, not returning" % (id(c),))  
            return
        with self.Lock:
            if len(self.IdleConnections) >= self.MaxIdleConnections:
                print("ConnectionPool.returnConnection: too many idle connections:", len(self.IdleConnections))
                c.close()
            elif not c in (x.Connection for x in self.IdleConnections):                    
                self.IdleConnections.append(_IdleConnection(c))
                self.startCleaner()
                #print ( "return: park idle connection %s" % (c,))
            
    def startCleaner(self):
        with self.Lock:
            if self.CleanerThread is None or not self.CleanerThread.is_alive():
                self.CleanerThread = self.CleanUpThread(
                        self.IdleConnections, self.Lock, self.IdleTimeout)
                self.CleanerThread.start()

    def __del__(self):
        # make sure to stop the clean up thread
        if self.CleanerThread is not None and self.CleanerThread.is_alive():
            self.CleanerThread.stop()
            self.CleanerThread = None
        
if __name__ == "__main__":
    #
    # test
    #
    import time, random
    
    class DummyConnection(object):
    
        Lock = RLock()
        Count = 0
    
        def __init__(self):
            print ("Connection created: >> %x" % (id(self),))
            with self.Lock:
                DummyConnection.Count += 1
            
        def close(self):
            print ("Connection closed:  << %x" % (id(self),))
            with self.Lock:
                DummyConnection.Count -= 1
            
    
    class DummyConnector(ConnectorBase):
    
        def connect(self):
            return DummyConnection()
            
        def probe(self, connection):
            return True
            
        def connectionIsClosed(self, connection):
            return False
            
    class Client(Thread):
    
        def __init__(self, pool):
            Thread.__init__(self)
            self.Pool = pool
    
        def run(self):
            for _ in range(10):
                time.sleep(random.random()*1.0)
                c = pool.connect()
                time.sleep(random.random()*1.0)
                #c.close()
            print ("thread is done")
    
    pool = ConnectionPool(connector=DummyConnector(), idle_timeout = 5)
    
    clients = [Client(pool) for _ in range(5)]
    for c in clients:
        c.start()
        
    t0 = time.time()
    
    while True:
        print ("idle count:", pool.idleCount(), "     open count:", DummyConnection.Count)
        time.sleep(3)
    
    
    
    
    
    
