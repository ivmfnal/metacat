from webpie import WPApp, WPHandler, Response, WPStaticHandler
from metacat.db import DBUser, DBNamespace
from urllib.parse import quote_plus, unquote_plus

class BaseHandler(WPHandler):
    
    def __init__(self, *params):
        WPHandler.__init__(self, *params)
        self.NamespaceAuthorizations = {}                # namespace -> True/False
        
    def _namespace_authorized(self, db, namespace, user):
        authorized = self.NamespaceAuthorizations.get(namespace)
        if authorized is None:
            ns = DBNamespace.get(db, namespace)
            if ns is None:
                raise KeyError("Namespace %s does not exist")
            authorized = ns.owned_by_user(user)
            self.NamespaceAuthorizations[namespace] = authorized
        return authorized
    
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
        username, error = self.App.user_from_request(self.Request)
        if not username:    return None
        db = self.App.connect()
        return DBUser.get(db, username)

    def messages(self, args):
        return {k: unquote_plus(args.get(k,"")) for k in ("error", "message")}
        
