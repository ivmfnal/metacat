import time, uuid, sys
from .py3 import to_bytes, to_str
from datetime import timedelta
import jwt

SignedTokenExpiredError = jwt.ExpiredSignatureError
SignedTokenImmatureError = jwt.ImmatureSignatureError
SignedTokenUnacceptedAlgorithmError = jwt.InvalidAlgorithmError
SignedTokenSignatureVerificationError = jwt.InvalidSignatureError

class SignedToken(object):
    
    def __init__(self, payload={}, subject=None, expiration=None, issuer=None, **claims):
        self.Encoded = None
        
        now = time.time()
        
        self.Payload = {
            "iat":  now,
            "nbf":  now,
            "sub":  subject,
            "exp":  expiration if (expiration is None or expiration > 10*365*24*3600) else now + expiration,
            "jti":  uuid.uuid4().hex[-8:]
        }
        if issuer:
            self.Payload["iss"] = issuer
        self.Payload.update(claims)
        self.Payload.update(payload)
        
    def __str__(self):
        data = {"ctime":time.ctime(self.Payload["exp"])}
        data.update(self.Payload)
        return "[SignedToken %(jti)s sub=%(sub)s iss=%(iss)s exp=%(ctime)s]" % data

    @property
    def expiration(self):
        return self.Payload.get("exp")

    @property
    def issuer(self):
        return self.Payload.get("iss")

    @property
    def subject(self):
        return self.Payload.get("sub")

    @property
    def tid(self):
        return self.Payload.get("jti")

    @staticmethod   
    def from_bytes(encoded):
        encoded = to_bytes(encoded).strip()   # convert to bytes and remove all white space
        payload = jwt.decode(encoded, options={"verify_signature":False})
        token = SignedToken(payload=payload)
        token.Encoded = encoded
        return token

    decode = from_bytes
        
    def encode(self, key=None, private_key=None):
        if key is None and private_key is None:
            if self.Encoded:    return self.Encoded
            else:
                raise ValueError("Key is required")
        k = key or private_key
        encoded = jwt.encode(self.Payload, 
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
    
    claims = items

        
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
        
    
        
