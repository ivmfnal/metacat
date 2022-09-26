def _undid(did, namespace=None):
    if ':' in did:
        return did.split(':', 1)
    else:
        return namespace, did

class ObjectSpec(object):
    
    def __init__(self, namespace=None, name=None, did=None, fid=None):
        self.Namespace = self.Name = self.FID = None
        if namespace and name:
            self.Namespace, self.Name = namespace, name
        elif did:
            self.Namespace, self.Name = _undid(did, namespace)
        self.FID = fid

    def did(self):
        if not self.Namespace:
            raise ValueError("Unspecified namespace")
        if not self.Name:
            raise ValueError("Unspecified name")
        return f"{self.Namespace}:{self.Name}"
