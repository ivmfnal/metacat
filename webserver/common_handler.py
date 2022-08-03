from metacat.auth.server import BaseHandler
from metacat.db import DBUser, DBNamespace, DBRole

class MetaCatHandler(BaseHandler):
    
    def __init__(self, *params, **args):
        BaseHandler.__init__(self, *params, **args)
        self.NamespaceAuthorizations = {}

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
    
