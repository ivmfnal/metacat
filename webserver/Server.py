import yaml, os, getopt, sys

from webpie import WPApp, WPHandler, Response, WPStaticHandler
from metacat.db import DBUser, DBRole

#import webpie
#print("webpie imported from:", webpie.__file__)

import json, time, secrets, traceback, hashlib, pprint
from urllib.parse import quote_plus, unquote_plus

from metacat.util import to_str, to_bytes, SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError

from metacat import Version
from wsdbtools import ConnectionPool

from gui_handler import GUIHandler
from data_handler import DataHandler
from auth_handler import AuthHandler
            
class RootHandler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.data = DataHandler(*params, **args)
        self.gui = GUIHandler(*params, **args)
        self.static = WPStaticHandler(*params, root=self.App.StaticLocation)
        self.auth = AuthHandler(*params, **args)

    def index(self, req, relpath, **args):
        return self.redirect("./gui/index")
        
class App(WPApp):

    Version = Version

    def __init__(self, cfg, root, static_location="./static", **args):
        WPApp.__init__(self, root, **args)
        self.StaticLocation = static_location
        self.Cfg = cfg
        self.DefaultNamespace = cfg.get("default_namespace")
        
        self.DBCfg = cfg["database"]
        
        connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % self.DBCfg
        
        self.DB = ConnectionPool(postgres=connstr, max_idle_connections=3)
        from metacat.filters import standard_filters
        self.Filters = standard_filters
                
        self.AuthConfig = cfg.get("authentication")
        secret = cfg.get("secret") 
        if secret is None:    self.TokenSecret = secrets.token_bytes(128)     # used to sign tokens
        else:         
            h = hashlib.sha256()
            h.update(to_bytes(secret))      
            self.TokenSecret = h.digest()
        self.Tokens = {}                # { token id -> token object }

    def auth_config(self, method):
        return self.AuthConfig.get(method)
        
    def connect(self):
        conn = self.DB.connect()
        #print("conn: %x" % (id(conn),), "   idle connections:", ",".join("%x" % (id(c),) for c in self.DB.IdleConnections))
        return conn
        
    def get_digest_password(self, realm, username):
        db = self.connect()
        u = DBUser.get(db, username)
        if u is None:
            return None
        hashed = u.authenticator("password").password_for_digest()
        return hashed

    TokenExpiration = 24*3600*7

    def user_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: 
            return None, "Token not found"
        try:    
            token = SignedToken.from_bytes(encoded)
            token.verify(self.TokenSecret)
        except SignedTokenExpiredError:
            return None, "Token expired"           
        except SignedTokenImmatureError:
            return None, "Token immature"           
        except SignedTokenUnacceptedAlgorithmError:
            return None, "Invalid token algorithm"           
        except SignedTokenSignatureVerificationError:
            return None, "Invalid token"           
        except Exception as e:
            return None, str(e)
        else:
            return token.Payload.get("user"), None


    def encoded_token_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: return None
        try:    token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except: return None             # invalid token
        return encoded

    def response_with_auth_cookie(self, user, redirect):
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
        token = SignedToken({"user": user}, expiration=self.TokenExpiration)
        encoded = token.encode(self.TokenSecret)
        #print("Server.App.response_with_auth_cookie: new token created:", token.TID)
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        #print ("response:", resp, "  reditrect=", redirect)
        resp.headers["X-Authentication-Token"] = to_str(encoded)
        resp.set_cookie("auth_token", encoded, max_age = int(self.TokenExpiration))
        return resp

    def response_with_unset_auth_cookie(self, redirect):
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        try:    resp.set_cookie("auth_token", "-", max_age=100)
        except: pass
        return resp

    def verify_token(self, encoded):
        try:
            token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except Exception as e:
            return False, e
        return True, None
        

    def filters(self):
        return self.Filters
        
def create_application(config_file=None):
    config_file = config_file or os.environ.get("METACAT_SERVER_CFG")
    if not config_file:
        print("Configuration file must be provided using METACAT_SERVER_CFG environment variable")
        return None
    config = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)  
    cookie_path = config.get("cookie_path", "/metacat")
    static_location = config.get("static_location", os.environ.get("METACAT_SERVER_STATIC_DIR", "static"))
    application=App(config, RootHandler, static_location=static_location)

    templdir = config.get("templates", "")
    if templdir.startswith("$"):
        templdir = os.environ[templdir[1:]]

    application.initJinjaEnvironment(
        tempdirs=[templdir, "."],
        globals={
            "GLOBAL_Version": Version, 
            "GLOBAL_SiteTitle": config.get("site_title", "DEMO Metadata Catalog")
        }
    )
    
    return application

if __name__ == "__main__":
    from webpie import HTTPServer
    import sys
    import yaml, os
    import sys, getopt

    opts, args = getopt.getopt(sys.argv[1:], "c:p:")
    opts = dict(opts)
    port = int(opts.get("-p", 8080))
    config_file = opts.get("-c")
    
    server = HTTPServer(port, create_application(config_file), debug=sys.stdout)
    server.run()
    #application.run_server(port)
else:
    # running under uwsgi
    application = create_application()
    
    
        
