import hashlib, json, base64, time, uuid, sys
from struct import pack, unpack
from py3 import to_bytes, to_str

import jwt

class SignedToken(object):
    
    def __init__(self, inp={}, expiration=None, subject=None):
        self.Expiration = None
        self.NotBefore = None
        self.Encoded = None
        self.Payload = {}
        self.Subject = None
        if isinstance(inp, dict):
            self.from_dict(inp)
            self.Id = uuid.uuid1().hex
            self.Subject = subject
            self.Expiration = expiration
        elif isinstance(inp, (bytes, str)):
            self.from_bytes(to_bytes(inp))
            
    def __str__(self):
        return "[SignedToken %s]" % (to_str(self.Id),)
            
    def from_dict(self, data):
        self.Payload.update(data)
        
    def from_bytes(self, encoded):
        self.Encoded = encoded
        self.Payload = payload = jwt.decode(encoded, options={"verify_signature":False})
        self.Id = payload.get("kid")
        self.Expiration = payload.get("exp")
        self.Subject = payload.get("sub")
        
    def encode(self, key=None, private_key=None):
        if key is None and private_key is None:
            if self.Encoded:    return self.Encoded
            else:
                raise ValueError("Key is required")
        k = key or private_key
        payload = {}
        payload.update(self.Payload)
        if self.Expiration is not None: payload["exp"] = self.Expiration
        if self.Subject is not None: payload["sub"] = self.Subject
        if self.Id: payload["kid"] = self.Id
        encoded = jwt.encode(payload, 
            key if private_key is None else private_key,
            algorithm="HS256" if private_key is None else "RS256"
        )
        self.Encoded = encoded
        return encoded
            
    def verify(self, key=None, public_key=None, ignore_time=False, leeway=0):
        if self.Encoded is None:
            raise ValueError("The ticket was not decoded")
        if key is None and public_key is None:
            raise ValueError("Key is required")
        options = {}
        if ignore_time:
            options["verify_exp"] = False
            options["verify_nbf"] = False
        self.Payload = jwt.decode(self.Encoded, key if public_key is None else public_key,
                algorithms=["HS256" if public_key is None else "RS256"],
                options = options,
                leeway = float(leeway)
        )
    
    
    #
    # dict interface
    #
    
    def __getitem__(self, key):
        return self.Payload[key]
        
    def __setitem__(self, key, value):
        self.Payload[key] = value
        
    def get(self, key, default=None):
        return self.Payload.get(key, default)
        
    def keys(self):
        return self.Payload.keys()
    
    def items(self):
        return self.Payload.items()
    

        
if __name__ == "__main__":
    import os, pprint
    key = "abcd"

    payload = {"text":"hello world", "temp":32.0}
    print("Payload:")
    pprint.pprint(payload)
    
    t0 = SignedToken(payload, expiration=1000)
    print("Initial token:", t0)
    
    encoded = t0.encode(key=key)

    print("----------------")
    
    print("Encoded token:", encoded)
    
    print("----------------")
    
    
    t1 = SignedToken(encoded)
    t1.verify(key=key)
    print("Decoded token:",t1.Payload, t1.Id)
        
    
        
