from metacat.auth.server import BaseHandler
from metacat.db import DBUser, DBNamespace, DBRole
from webpie import http_exceptions
import re

class MetaCatHandler(BaseHandler):
    
    def __init__(self, *params, **args):
        BaseHandler.__init__(self, *params, **args)
        self.NamespaceAuthorizations = {}
        
    SafePatterns = {
        "any": None,        # no-op
        "safe": re.compile(r"[^']+", re.I),
        "aname": re.compile(r"[a-z][a-z0-9_.-]*", re.I),
        "fname": re.compile(r"[a-z0-9_./-]+", re.I)
    }

    def sanitize(self, *words, allow="fname", **kw):
        pattern = self.SafePatterns[allow]
        if pattern is not None:
            for w in words:
                if w and not pattern.fullmatch(w):
                    raise http_exceptions.HTTPBadRequest("Invalid value: %s" % (w,))
            for name, value in kw.items():
                if value and not pattern.fullmatch(value):
                    raise http_exceptions.HTTPBadRequest("Invalid value for %s: %s" % (name, value))

    def authenticated_user(self):
        username, error = self.authenticated_username()
        if username is None:
            return None, error
        user = DBUser.get(self.App.connect(), username)
        if user is not None:
            return user, None
        else:
            return None, "user not found"

    def _namespace_authorized(self, db, namespace, user):
        authorized = self.NamespaceAuthorizations.get(namespace)
        if authorized is None:
            ns = DBNamespace.get(db, namespace)
            if ns is None:
                raise KeyError("Namespace %s does not exist")
            authorized = ns.owned_by_user(user)
            self.NamespaceAuthorizations[namespace] = authorized
        return authorized
    
