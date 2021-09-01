import time, uuid, sys
from .py3 import to_bytes, to_str
from datetime import timedelta

import jwt

SignedTokenExpiredError = jwt.ExpiredSignatureError
SignedTokenImmatureError = jwt.ImmatureSignatureError
SignedTokenUnacceptedAlgorithmError = jwt.InvalidAlgorithmError
SignedTokenSignatureVerificationError = jwt.InvalidSignatureError

class SignedToken(object):
    
    def __init__(self, payload, issued_at=None, subject=None, expiration=None, not_before=None, tid=None):
        self.TID = tid or uuid.uuid4().hex[-8:]
        self.Encoded = None
        self.Payload = payload
        self.Subject = subject
        self.IssuedAt = time.time() if issued_at is None else issued_at
        self.NotBefore = not_before if (not_before is None or not_before > 365*24*3600) else self.IssuedAt + not_before
        self.Expiration = expiration if (expiration is None or expiration > 365*24*3600) else self.IssuedAt + expiration
            
    def __str__(self):
        return "[SignedToken %s]" % (to_str(self.TID),)
    
    @staticmethod   
    def from_bytes(encoded):
        payload = jwt.decode(encoded, options={"verify_signature":False})
        tid = payload.get("jti") or payload.get("tid")
        exp = payload.get("exp")
        sub = payload.get("sub")
        iat = payload.get("iat")
        nbf = payload.get("nbf")
        token = SignedToken(payload, issued_at=iat, subject=sub, expiration=exp, not_before=nbf, tid=tid)
        token.Encoded = encoded
        return token

    decode = from_bytes
        
    def encode(self, key=None, private_key=None):
        if key is None and private_key is None:
            if self.Encoded:    return self.Encoded
            else:
                raise ValueError("Key is required")
        k = key or private_key
        payload = {}
        payload.update(self.Payload)
        payload["iat"] = self.IssuedAt
        payload["iss"] = "metacat"
        if self.Expiration is not None: payload["exp"] = self.Expiration
        if self.NotBefore is not None: payload["nbf"] = self.NotBefore
        if self.Subject is not None: payload["sub"] = self.Subject
        if self.TID: payload["jti"] = self.TID
        encoded = jwt.encode(payload, 
            key if private_key is None else private_key,
            algorithm="HS256" if private_key is None else "RS256"
        )
        self.Encoded = encoded
        return encoded
            
    def verify(self, key=None, public_key=None, ignore_times=False, leeway=0):

        if self.Encoded is None:
            raise ValueError("The ticket was not decoded")

        options = {}
        if ignore_times:
            options["verify_exp"] = False
            options["verify_nbf"] = False
        use_key = None
        algorithms = []
        if public_key:
            use_key = public_key
            algorithms=["RS256"]
        elif key:
            use_key = key
            algorithms=["HS256"]
        else:
            options["verify_signature"] = False
        
        self.Payload = jwt.decode(self.Encoded, use_key,
                leeway = timedelta(seconds=leeway),
                algorithms=algorithms,
                options = options
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
    print("Decoded token:",t1.Payload, t1.TID)
        
    
        
