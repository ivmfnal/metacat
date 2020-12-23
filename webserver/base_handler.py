from webpie import WPApp, WPHandler, Response, WPStaticHandler
from metacat.db import DBUser
from urllib.parse import quote_plus, unquote_plus

class BaseHandler(WPHandler):
    
    def connect(self):
        return self.App.connect()

    def text_chunks(self, gen, chunk=100000):
        buf = []
        size = 0
        for x in gen:
            n = len(x)
            buf.append(x)
            size += n
            if size >= chunk:
                yield "".join(buf)
                size = 0
                buf = []
        if buf:
            yield "".join(buf)
            
    def authenticated_user(self):
        username = self.App.user_from_request(self.Request)
        if not username:    return None
        db = self.App.connect()
        return DBUser.get(db, username)

    def messages(self, args):
        unquoted = {}
        for k in ("error", "message"):
            m = args.get(k)
            if m:
                unquoted[k] = unquote_plus(m)
        return unquoted
        
