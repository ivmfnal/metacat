import os
from .signed_token_jwt import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError
from .py3 import to_bytes, to_str

class TokenLib(object):

        DefaultFile = "%s/.token_library" % (os.environ["HOME"],)

        def __init__(self, path = None):
            self.Path = path or self.DefaultFile
            #print("Token lib path:", self.Path)
            self.Tokens = self.load_tokens()

        def load_tokens(self):
            # returns dict: { url: token }
            # removes expired tokens
            token_file = self.Path
            try:        lines = open(token_file, "r").readlines()
            except:     return {}
            out = {}
            for line in lines:
                line = line.strip()
                url, encoded = line.split(None, 1)
                try:
                    token = SignedToken.decode(encoded)
                    #print("TokenLib.load: token:", token)
                    token.verify()      # this will verify expiration/maturity times only
                    #print("  token verified. Exp:", token.Expiration)
                except SignedTokenExpiredError:
                    #print("TokenLib.load: token expired")
                    token = None
                except SignedTokenImmatureError:
                    #print("TokenLib.load: token immature")
                    pass
                if token is not None:
                    out[url] = token
            #print("TokenLib.load: out:", out)
            return out

        def save_tokens(self):
            token_file = self.Path
            f = open(token_file, "w")
            for url, token  in self.Tokens.items():
                f.write("%s %s\n" % (url, to_str(token.encode())))
            f.close()

        def __setitem__(self, url, token):
                if isinstance(token, (str, bytes)):
                    token = SignedToken.decode(token)
                self.Tokens[url] = token
                self.save_tokens()

        def __getitem__(self, url):
                return self.Tokens[url]

        def get(self, url):
                return self.Tokens.get(url)

        def items(self):
                return self.Tokens.items()

