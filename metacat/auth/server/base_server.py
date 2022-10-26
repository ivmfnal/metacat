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
        self.DB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=5)
        self.DBSchema = db_config.get("schema")

        if "user_database" in cfg:
            connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % cfg["user_database"]
            self.UserDB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=5)
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
            
    def init(self):
        #print("ScriptHome:", self.ScriptHome)
        self.initJinjaEnvironment(tempdirs=[self.ScriptHome, self.ScriptHome + "/templates"])
        

    def auth_config(self, method):
        return self.AuthConfig.get(method)
                
    def connect(self):
        conn = self.DB.connect()
        #print("conn: %x" % (id(conn),), "   idle connections:", ",".join("%x" % (id(c),) for c in self.DB.IdleConnections))
        if self.DBSchema:
            conn.cursor().execute(f"set search_path to {self.DBSchema}")
        return conn
        
    db = connect        # for compatibility
    
    def user_db(self):
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
        try:    
            token = SignedToken.from_bytes(encoded)
            #print("server.user_from_request: token:", token)
            #print("                          secret:", self.TokenSecret)
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
            return token.get("sub"), None

    def encoded_token_from_request(self, request):
        token = self.token_from_request(request)
        if token is not None:
            return token.encode()
        else:
            return None

    def token_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: return None
        try:
            #print("token_from_request: encoded:", encoded)
            token = SignedToken.decode(encoded)
            token.verify(key=self.TokenSecret)
        except Exception as e:
            #print("token_from_request: Exception in verify:", e, traceback.format_exc())
            return None             # invalid token
        return token

    def generate_token(self, user, payload={}, expiration=None):
        expiration = expiration or self.TokenExpiration
        token = SignedToken(payload, subject=user, expiration=expiration, issuer=self.Issuer)
        return token, token.encode(self.TokenSecret)

    def response_with_auth_cookie(self, user, redirect, token=None):
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
        if token is not None:
            encoded = token.encode()
        else:
            _, encoded = self.generate_token(user, {"user": user})
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
            token = SignedToken.decode(encoded)
            token.verify(self.TokenSecret)
        except Exception as e:
            return None, e
        return token, None
        
class BaseHandler(WPHandler):
    
    def connect(self):
        return self.App.connect()

    def text_chunks(self, gen, chunk=10000):
        print("text_chunks...")
        buf = []
        size = 0
        for x in gen:
            n = len(x)
            buf.append(x)
            size += n
            if size >= chunk:
                print("yielding:", "".join(buf))
                yield "".join(buf)
                size = 0
                buf = []
        if buf:
            print("final yielding:", "".join(buf))
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

