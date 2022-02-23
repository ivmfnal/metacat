# common functionality for Auth, GUI and Data servers

from webpie import WPApp, Response
from wsdbtools import ConnectionPool
from metacat.util import to_str, to_bytes, SignedToken
import psycopg2, json, time, secrets, traceback, hashlib, pprint, os, yaml
from metacat.db import DBUser
from urllib.parse import quote_plus, unquote_plus
from metacat import Version

class BaseApp(WPApp):

    Version = Version

    def __init__(self, cfg, root_handler, **args):
        WPApp.__init__(self, root_handler, **args)
        self.Cfg = cfg

        db_config = cfg["database"]
        connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % db_config
        self.DB = ConnectionPool(postgres=connstr, max_idle_connections=3)
        self.DBSchema = db_config.get("schema")

        if "user_database" in cfg:
            connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % cfg["user_database"]
            self.UserDB = ConnectionPool(postgres=connstr, max_idle_connections=3)
            self.UserDBSchema = cfg["user_database"].get("schema")
        else:
            self.UserDB = self.DB
            self.UserDBSchema = self.DBSchema

        self.AuthConfig = cfg.get("authentication")
        secret = cfg.get("secret") 
        if secret is None:    self.TokenSecret = secrets.token_bytes(128)     # used to sign tokens
        else:         
            h = hashlib.sha256()
            h.update(to_bytes(secret))      
            self.TokenSecret = h.digest()

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
            conn.cursor.execute(f"set search_path to {self.UserDBSchema}")
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
            return token.get("sub"), None

    def encoded_token_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: return None
        try:    token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except: return None             # invalid token
        return encoded

    def generate_token(self, user, payload={}, expiration=None):
        expiration = expiration or self.TokenExpiration
        token = SignedToken(payload, subject=user, expiration=expiration)
        return token, token.encode(self.TokenSecret)

    def response_with_auth_cookie(self, user, redirect):
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
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
            token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except Exception as e:
            return False, e
        return True, None
        
