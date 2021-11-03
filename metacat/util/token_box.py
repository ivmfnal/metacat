class TokenBox(object):
    def __init__(self, url, username, password, margin = 10, request_now = False):
        self.URL = url
        self.Username = username
        self.Password = password
        self.Token = None
        self.Expiration = 0
        self.Encoded = None
        self.Margin = margin
        if request_now:
            self.renewIfNeeded()
        
    def renewIfNeeded(self):
        need_to_renew = self.Token is None or time.time() > self.Expiration - self.Margin
        if need_to_renew:
            from .rfc2617 import digest_client
            status, body = digest_client(self.URL, self.Username, self.Password)
            if status/100 == 2:
                encoded = body.strip()
                t = SignedToken.decode(encoded)
                self.Token = t
                self.Encoded = encoded
                self.Expiration = t.expiration
            else:
                raise SignedTokenAuthoriztionError(body)
    
    @property
    def token(self):
        self.renewIfNeeded()
        return self.Encoded
            
        
