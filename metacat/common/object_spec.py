def undid(did, default_namespace=None):
    namespace = default_namespace
    if ':' not in did:
        name = did
    else:
        namespace, name = did.split(':', 1)
    return namespace, name

class ObjectSpec(object):

    def __init__(self, p1=None, p2=None, namespace=None, fid=None):
        self.FID = fid
        name = None
        if p1 and p2:
            namespace, name = p1, p2
        elif p1:
            namespace, name = undid(p1, namespace)
        self.Namespace, self.Name = namespace, name
        
    def __str__(self):
        return f"ObjectSpec({self.Namespace}:{self.Name}, fid={self.FID})"

    def did(self):
        if not (self.Name and self.Namespace):
            raise ValueError("The specification does not have name or namespace")
        return f"{self.Namespace}:{self.Name}"

    @staticmethod
    def from_dict(self, data):
        name = data.get("name")
        if name:
            spec = Spec(data.get("namespace"), name, fid=data.get("fid"))
        else:
            spec = Spec(namespace=data.get("namespace"), did=data["did"], fid=data.get("fid"))
        return spec

    def valid(self):
        return self.Namespace and self.Name or self.FID

    def validate(self):
        if not self.valid():
            raise ValueError("Invalid specification")
        return self

    def as_dict(self):
        self.validate()
        out = {}
        if self.FID:    out["fid"] = self.FID
        if self.Name:   out["name"] = self.Name
        if self.Namespace:  out["namespace"] = self.Namespace
        return out

if __name__ == "__main__":
    for params, args in [
                (("scope", "name"), {}),
                (("scope:name",), {}),
                (("name",), {"namespace":"default"}),
                (("scope:name",), {"namespace":"default"}),
                (("name",), {})
            ]:
        kw = args.copy()
        print(params, kw, "->", ObjectSpec(*params, **kw))
        kw["fid"] = "abcd"
        print(params, kw, "->", ObjectSpec(*params, **kw))
        
    
    
