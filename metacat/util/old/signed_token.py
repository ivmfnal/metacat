import hashlib, json, base64, time, uuid, sys
#from struct import pack, unpack
from .py3 import to_bytes

class crypt(object):
    
    HEAD_PAD = 64
    
    def __init__(self, key=None):
        import secrets, Crypto              # pycrypto is requred
        if key is None:
            key = secrets.token_bytes(16)   # AES uses 16 bytes key
        else:
            assert isinstance(key, bytes) and len(key) == 16
        self.Key = key
    
    def encrypt(self, data):
        import secrets, Crypto              # pycrypto is requred
        from Crypto.Cipher import AES
        data = to_bytes(data)
        l = len(data)
        padded_l = ((l+15)//16)*16
        pad_l = padded_l - l
        padded_data = data + secrets.token_bytes(pad_l) if pad_l > 0 else data
        padded_data = secrets.token_bytes(self.HEAD_PAD) + padded_data
        iv = secrets.token_bytes(16)
        aes = AES.new(self.Key, AES.MODE_CBC, iv)
        return aes.encrypt(padded_data), iv, l
        
    def decrypt(self, encrypted, iv, length):
        import secrets, Crypto              # pycrypto is requred
        from Crypto.Cipher import AES
        if isinstance(encrypted, str):   encrypted = encrypted.encode("utf-8")
        aes = AES.new(self.Key, AES.MODE_CBC, iv)
        data = aes.decrypt(encrypted)[self.HEAD_PAD:]
        return data[:length]        

def generate_secret(length):
    import secrets
    return secrets.token_bytes(length)

class SignedTokenSignatureVerificationError(Exception):
    pass
    
class SignedTokenHeaderError(Exception):
    pass
    
class SignedTokenExpiredError(Exception):
    pass
    
class SignedTokenImmatureError(Exception):
    pass
    
class SignedTokenUnacceptedAlgorithmError(Exception):
    def __init__(self, alg):
        self.Alg = alg
        
    def __str__(self):
        return "Unaccepted signature algorithm %s" % (self.Alg,)
        
class SignedTokenAuthoriztionError(Exception):
    def __init__(self, msg):
        if isinstance(msg, bytes):
            msg = msg.decode("utf-8", "ignore")
        self.Message = msg
        
    def __str__(self):
        return "Authorization or authentication failed: %s" % (self.Message,)
        
class SignedToken(object):
    
    AcceptedAlgorithms = ["sha256","sha384","sha512","md5"]

    AvailableAlgorithms = set(hashlib.algorithms if hasattr(hashlib, "algorithms") else hashlib.algorithms_available)
    
    #PreferredAlgorithms = [a for a in AcceptedAlgorithms if a in AvailableAlgorithms]
    
    def __init__(self, payload, expiration=None, not_before=None, tid=None):
        if tid is not None:
            assert len(tid) == 32
            try:    bytes.fromhex(tid)
            except:
                assert False, "token ID must be a 32-character hex string"
        self.TID = tid if tid is not None else uuid.uuid4().hex
        self.Alg = [a for a in self.AcceptedAlgorithms if a in self.AvailableAlgorithms][0]
        self.Payload = payload
        self.IssuedAt = time.time()
        self.NotBefore = not_before if (not_before is None or not_before > 365*24*3600) else self.IssuedAt + not_before
        self.Expiration = expiration if (expiration is None or expiration > 365*24*3600) else self.IssuedAt + expiration
        self.Encoded = None
        
    def __getitem__(self, name):
        return self.Payload[name]
        
    def __setitem__(self, name, value):
        self.Payload[name] = value
        
    def __str__(self):
        return "SignedToken(%s %s iat=%s, nbf=%s, exp=%s, payload=%s)" % (self.TID, self.Alg, self.IssuedAt, self.NotBefore, self.Expiration,
            self.Payload)
            
    @staticmethod
    def serialize(x):
        # if it is already bytes, just base64 encode it
        if not isinstance(x, bytes):
            #print ("serialize: x:", type(x), repr(x))
            x = json.dumps(x)
            if isinstance(x, str):
                x = x.encode("utf-8")
        return x
        
    @staticmethod
    def deserialize(x):
        # x can be either json representation of an object, or bytes
        try:    return json.loads(x)
        except: return x
        
    @staticmethod
    def pack(*words):
        assert len(words) == 3, "Token must consist of 3 words, got %d instead" % (len(words),)
        #print("pack: words:", words)
        return b".".join([base64.b64encode(w) for w in words])
        
    @staticmethod
    def unpack(txt):
        if isinstance(txt, str):	txt = txt.encode("utf-8")
        words = txt.split(b'.')
        assert len(words) == 3, "Token must consist of 3 words, got %d instead: [%s]" % (len(words), txt)
        return [base64.b64decode(w) for w in words]
    
    @staticmethod
    def signature(alg, *words):
        text = SignedToken.pack(*words)
        h = hashlib.new(alg)
        h.update(text)
        return h.digest()

    def encode(self, signature_key=None, encryption_key=None):
        if signature_key is None:
            assert self.Encoded is not None
            return self.Encoded
        payload = self.serialize(self.Payload)      # this will be bytes
        header = {"iat":self.IssuedAt, "exp":self.Expiration, "alg":self.Alg, "nbf":self.NotBefore, "tid":self.TID}
        if encryption_key is not None:
            # encrypt the payload
            key = to_bytes(encryption_key)
            assert isinstance(key, bytes) and len(key) == 16
            c = crypt(key)
            payload, iv, length = c.encrypt(payload)
            header["enc"] = {"ini":base64.b64encode(iv).decode("utf-8", "ignore"), "len":length, "alg":"AES"}
        header = self.serialize(header)
        signature = self.signature(self.Alg, header, payload, signature_key)
        encoded = self.Encoded = self.pack(header, payload, signature)
        return encoded
        
    @staticmethod
    def decode(txt, signature_key=None, verify_times=False, leeway=0, encryption_key=None):
        #print("SignedToken.decode(%s)" % (txt,))
        header, payload, signature = SignedToken.unpack(txt)
        #print("SignedToken.decode(): unpacked:", header, payload, signature)
        header_decoded = SignedToken.deserialize(header)
        #print("SignedToken.decode(): header_decoded:", header_decoded)
        
        try:    alg = header_decoded["alg"]
        except: raise SignedTokenHeaderError
        exp = header_decoded.get("exp")
        nbf = header_decoded.get("nbf")
        iat = header_decoded.get("iat")
        tid = header_decoded.get("tid", uuid.uuid1().hex)		# in case there is none ??
        if signature_key is not None:
            if not alg in SignedToken.AcceptedAlgorithms:
                raise SignedTokenUnacceptedAlgorithmError(alg)
            calculated_signature = SignedToken.signature(alg, header, payload, signature_key)
            if calculated_signature != signature:
                raise SignedTokenSignatureVerificationError
        if verify_times:
            if exp is not None and time.time() > exp + leeway:
                raise SignedTokenExpiredError
            if nbf is not None and time.time() < nbf - leeway:
                raise SignedTokenImmatureError
                
        enc = header_decoded.get("enc")
        if enc is not None:
            key = to_bytes(encryption_key)
            assert key is not None and len(key) == 16
            c = crypt(key)
            l = enc["len"]
            iv = base64.b64decode(enc["ini"])
            payload = c.decrypt(payload, iv, l)
            
        payload = SignedToken.deserialize(payload)
        
        token = SignedToken(payload, exp, nbf, tid=tid)
        token.IssuedAt = iat
        token.Alg = alg
        token.Header = header_decoded
        token.Encoded = to_bytes(txt)
        return token
        
    from_bytes = decode
        
if __name__ == "__main__":
    import os, pprint
    secret = generate_secret(128)
    key = generate_secret(16)

    payload = {"text":"hello world", "temp":32.0}
    print("Payload:")
    pprint.pprint(payload)
    
    t0 = SignedToken(payload, expiration=1000)
    print("Initial token:", t0)
    
    encoded = t0.encode(secret, key=key)

    print("----------------")
    
    print("Encoded token:", encoded)
    
    print("----------------")
    
    
    t1 = SignedToken.decode(encoded, secret, leeway=10, key=key)
    print("Decoded token:",t1)
    print("Header:")
    pprint.pprint(t1.Header)
    print("Payload:")
    pprint.pprint(t1.Payload)
        
    
        
