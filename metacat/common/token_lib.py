import os, os.path, stat
from .signed_token_jwt import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError
from metacat.util import to_bytes, to_str

class TokenLib(object):

        def __init__(self, path = None, create = True):
            if not path:
                locations = [
                    f"{location}/.token_library" for location in 
                    [ os.environ.get("HOME"), os.getcwd(), "/tmp" ]
                    if location
                ]
            else:
                locations = [path]
            self.Tokens, self.Location = self.load_library(locations)
            if not self.Location and create:
                # not found
                self.Location = self.create_library(locations)
                
        def create_library(self, paths):
            for path in paths:
                try:    
                    open(path, "w").close()         # create empty library
                    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR )
                    return path
                except: pass
            return None

        def load_library(self, paths):
            #print("load_library: paths:", paths)
            for path in paths:
                if os.path.isfile(path):
                    try:    
                        #print("loading tokens from:", path)
                        tokens = self.load_from_file(path)
                        #print("tokens loaded from:", path)
                        return tokens, path
                    except:
                        pass
            return {}, None     # not found
            
        def load_from_file(self, path):
            lines = open(path, "r").readlines()
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
            if self.Location:
                f = open(self.Location, "w")
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
                
        def exists(self):
            return not not self.Location