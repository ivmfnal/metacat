# common functionality for Auth, GUI and Data servers

from webpie import WPApp, Response, WPHandler
from wsdbtools import ConnectionPool
from metacat.util import to_str, to_bytes
from metacat.auth import BaseDBUser, \
    SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError
import psycopg2, json, time, secrets, traceback, hashlib, pprint, os, yaml
from urllib.parse import quote_plus, unquote_plus

class BaseApp(WPApp):

    def __init__(self, cfg, root_handler, **args):
        WPApp.__init__(self, root_handler, **args)
        self.Cfg = cfg
        
        db_config = cfg["database"]
        connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % db_config
        self.DB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=20)
        self.DBSchema = db_config.get("schema")

        if "user_database" in cfg:
            connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % cfg["user_database"]
            self.UserDB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=20)
            self.UserDBSchema = cfg["user_database"].get("schema")
        else:
            self.UserDB = self.DB
            self.UserDBSchema = self.DBSchema

        self.AuthConfig = cfg.get("authentication")
        self.Realm = self.AuthConfig.get("realm", "metacat")           # realm used by the rfc2617 authentication
        self.Issuer = self.AuthConfig.get("issuer")
        secret = self.AuthConfig.get("secret")
        if secret is None:    
            raise ValueError("Authentication secret not found in the configuration")
            self.TokenSecret = secrets.token_bytes(128)     # used to sign tokens
        else:         
            h = hashlib.sha256()
            h.update(to_bytes(secret))      
            self.TokenSecret = h.digest()
        self.SciTokenIssuers = self.AuthConfig.get("sci_token_issuers", [])
            
    def init(self):
        #print("ScriptHome:", self.ScriptHome)
        self.initJinjaEnvironment(tempdirs=[self.ScriptHome, self.ScriptHome + "/templates"])

    def connect(self):
        conn = self.DB.connect()
        #print("conn: %x" % (id(conn),), "   idle connections:", ",".join("%x" % (id(c),) for c in self.DB.IdleConnections))
        if self.DBSchema:
            conn.cursor().execute(f"set search_path to {self.DBSchema}")
        return conn
        
    db = connect        # for compatibility
    
    def user_db(self, group=None):
        # group is ignored, can be used by a subclass
        conn = self.UserDB.connect()
        if self.UserDBSchema:
            conn.cursor().execute(f"set search_path to {self.UserDBSchema}")
        return conn
        
    def get_digest_password(self, realm, username):
        db = self.connect()
        u = BaseDBUser.get(db, username)
        if u is None:
            return None
        return u.get_password(self.Realm)

    TokenExpiration = 24*3600*7

    def user_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        #print("server.user_from_request: encoded:", encoded)
        if not encoded: 
            return None, "Token not found"
        token, error = self.verify_token(encoded)
        #print("user_from_request: out:", out)
        return token and token.subject, error
            
    def response_with_auth_cookie(self, user, redirect, token=None, expiration=None):
        # expiration here is absolute time
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
        if expiration is None:
            expiration = self.TokenExpiration + time.time()
        if token is not None:
            encoded = token.encode()
        else:
            token, encoded = self.generate_token(user, {"user": user}, expiration=expiration)
        #print("Server.App.response_with_auth_cookie: new token created:", token.Payload)
        #print("   ", encoded)
        #print("   time:", time.time())
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        #print ("response:", resp, "  reditrect=", redirect)
        resp.headers["X-Authentication-Token"] = to_str(encoded)
        resp.set_cookie("auth_token", encoded, max_age = max(0, int(expiration - time.time())))
        return resp

    def response_with_unset_auth_cookie(self, redirect):
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        try:    resp.set_cookie("auth_token", "-", max_age=100)
        except: pass
        return resp

class BaseHandler(WPHandler):
    
    def connect(self):
        return self.App.connect()

    def text_chunks(self, gen, chunk=10000):
        buf = []
        size = 0
        for x in gen:
            n = len(x)
            buf.append(x)
            size += n
            if size >= chunk:
                #print("yielding:", "".join(buf))
                yield "".join(buf)
                size = 0
                buf = []
        if buf:
            #print("final yielding:", "".join(buf))
            yield "".join(buf)
            
    def authenticated_username(self):
        username, error = self.App.user_from_request(self.Request)
        return username, error

    def authenticated_user(self):
        username, error = self.authenticated_username()
        if username:
            user = BaseDBUser.get(self.App.user_db(), username)
            if user is not None:
                return user, None
            error = f"user {username} not found"
        return None, error

    def messages(self, args):
        return {k: unquote_plus(args.get(k,"")) for k in ("error", "message")}
        
    def jinja_globals(self):
        return {"G_User":self.authenticated_user()[0]}

