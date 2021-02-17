from auth_handler import AuthHandler
from metacat.util import to_str, to_bytes, SignedToken


from webpie import WPApp, WPHandler, Response, WPStaticHandler
import psycopg2, json, time, secrets, traceback, hashlib, pprint
from metacat.db import DBUser
from wsdbtools import ConnectionPool
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_str, to_bytes, SignedToken
from metacat.mql import MQLQuery
from metacat import Version

from auth_handler import AuthHandler, AuthAppMixin


class AuthApp(WPApp, AuthAppMixin):

    Version = Version

    def __init__(self, cfg, root, static_location="./static", **args):
        AuthAppMixin.__init__(self, cfg)
        WPApp.__init__(self, root, **args)
        self.StaticLocation = static_location
        self.Cfg = cfg
        
        self.DBCfg = cfg["database"]
        
        connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % self.DBCfg
        
        self.DB = ConnectionPool(postgres=connstr, max_idle_connections=3)
                
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
            #print("App: no token:", list(request.headers.items()) )
            
            return None
        try:    token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except:
            #print("App: token error:", traceback.format_exc()) 
            return None             # invalid token
        return token.Payload.get("user")

    def encoded_token_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: return None
        try:    token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except: return None             # invalid token
        return encoded

    def response_with_auth_cookie(self, user, redirect):
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
        token = SignedToken({"user": user}, expiration=self.TokenExpiration).encode(self.TokenSecret)
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        #print ("response:", resp, "  reditrect=", redirect)
        resp.headers["X-Authentication-Token"] = to_str(token)
        resp.set_cookie("auth_token", token, max_age = int(self.TokenExpiration))
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
        

import yaml, os
import sys, getopt

opts, args = getopt.getopt(sys.argv[1:], "c:")
opts = dict(opts)
config = opts.get("-c", os.environ.get("METACAT_SERVER_CFG"))
if not config:
    print("Configuration file must be provided either using -c command line option or via METADATA_SERVER_CFG environment variable")
    sys.exit(1)
    
config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)  
cookie_path = config.get("cookie_path", "/metadata")
#static_location = os.environ.get("METACAT_SERVER_STATIC_DIR", "./static")
#static_location = config.get("static_location", static_location)
application=AuthApp(config, AuthHandler)

port = int(config.get("auth_port", 8080))

if __name__ == "__main__":
    from webpie import HTTPServer
    import sys
    server = HTTPServer(port, application, debug=sys.stdout)
    server.run()
    #application.run_server(port)
else:
    # running under uwsgi
    pass
    
    
