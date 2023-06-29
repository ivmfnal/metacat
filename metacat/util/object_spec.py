def undid(did, default_namespace=None):
    namespace = default_namespace
    if ':' not in did:
        name = did
    else:
        namespace, name = did.split(':', 1)
    return namespace, name

class ObjectSpec(object):

    def __init__(self, p1=None, p2=None, name=None, namespace=None, fid=None, validate=True):
        self.Params = (p1, p2, namespace, fid)
        self.DID = None
        if isinstance(p1, dict):
            if "did" in p1:
                did = self.DID = p1["did"]
                namespace, name = undid(did, namespace)
            fid = p1.get("fid")
            name = p1.get("name", name)
            namespace = p1.get("namespace", namespace)
            p1 = None
        self.FID = fid
        if p1 and p2:
            namespace, name = p1, p2
        elif p1:
            if ':' in p1:
                self.DID = p1
                namespace, name = undid(p1, namespace)
            else:
                self.FID = self.FID or p1
        self.Namespace, self.Name = namespace, name
        if validate:
            self.validate()
        
    def __str__(self):
        return f"Spec(namespace={self.Namespace}, name={self.Name}, fid={self.FID})"

    def did(self):
        if not (self.Name and self.Namespace):
            raise ValueError("The specification does not have name or namespace")
        return f"{self.Namespace}:{self.Name}"

    @staticmethod
    def from_dict(data, namespace=None, validate=True):
        name = data.get("name")
        fid = data.get("fid")
        if name:
            spec = ObjectSpec(data.get("namespace", namespace), name, fid=fid, validate=validate)
        else:
            spec = ObjectSpec(data.get("did"), namespace=data.get("namespace", namespace), fid=data.get("fid"), validate=validate)
        return spec

    def valid(self):
        return self.Namespace and self.Name or self.FID

    def validate(self):
        if not self.valid():
            raise ValueError(f"Invalid object specification: %s, %s, namespace=%s, fid=%s" % self.Params)
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
                (("scope:name",), {"namespace":"default"})
            ]:
        kw = args.copy()
        print(params, kw, "->", ObjectSpec(*params, **kw))
        kw["fid"] = "abcd"
        print(params, kw, "->", ObjectSpec(*params, **kw))
    for d in [
                {   "fid": "abcd"   },
                {   "did":  "scope:name"    },
                {   "namespace":"scope", "name":"name" }
            ]:
        print(d, "->", ObjectSpec.from_dict(d))
        d["fid"] = "abcd"
        print(d, "->", ObjectSpec.from_dict(d))
        
        
        
    
    
