# common functionality for Auth, GUI and Data servers

from wsdbtools import ConnectionPool
from metacat.util import to_str, to_bytes
from metacat.common import (digest_server, 
    SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, 
    SignedTokenSignatureVerificationError
)

from metacat.auth import BaseDBUser as DBUser
import psycopg2, json, time, secrets, traceback, hashlib, pprint, os, yaml

class AuthenticationCore(object):

    """Class encapsulating the client authentication funtionality for a group
    """

    DefaultTokenExpiration = 24*3600*7


    def __init__(self, cfg, group=None):
        self.Cfg = cfg
        db_config = cfg.get("user_database") or cfg["database"]
        connstr = self.connstr(db_config)
        self.UserDB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=20)
        self.UserDBSchema = db_config.get("schema")

        self.AuthConfig = cfg.get("authentication")
        self.Group = group or self.AuthConfig.get("group", "metacat")          
        self.Realm = self.AuthConfig.get("realm", self.Group) # will be used by the rfc2617 authentication
        self.TokenIssuer = self.AuthConfig.get("issuer")
        self.SciTokenIssuers = self.AuthConfig.get("sci_token_issuers", [])
        secret = self.AuthConfig.get("secret")
        if secret is None:    
            raise ValueError("Authentication secret not found in the configuration")
            self.TokenSecret = secrets.token_bytes(128)     # used to sign tokens
        else:         
            h = hashlib.sha256()
            h.update(to_bytes(secret))      
            self.TokenSecret = h.digest()
        self.TokenExpiration = self.DefaultTokenExpiration

    def connstr(self, cfg):
        cs = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s" % cfg
        if cfg.get("password"):
            cs += " password=%(password)s" % cfg
        return cs

    def auth_config(self, method):
        # configuration for particular authentication mechanism
        return self.AuthConfig.get(method)

    def user_db(self):
        conn = self.UserDB.connect()
        if self.UserDBSchema:
            conn.cursor().execute(f"set search_path to {self.UserDBSchema}")
        return conn
        
    def get_user(self, username):
        db = self.user_db()
        return DBUser.get(db, username)

    def verify_token(self, encoded_token):
        try:    
            token = SignedToken.from_bytes(encoded_token)
            #print("verify_token: token:", token.Payload)
            t = time.time()
            #print("   time:", t, "  t-iat:", t-token["iat"], "  t-nbf:", t-token["nbf"])
            token.verify(self.TokenSecret)
            #print("verify_token: verified. subject:", token.subject)
        except SignedTokenExpiredError:
            return None, "Token expired"           
        except SignedTokenImmatureError:
            return None, "Token immature"           
        except SignedTokenUnacceptedAlgorithmError:
            return None, "Invalid token algorithm"           
        except SignedTokenSignatureVerificationError:
            return None, "Token verification failed"           
        except Exception as e:
            return None, str(e)
        else:
            #print("verify_token: token:", token, "  subject:", token.subject)
            return token, None

    def generate_token(self, user, payload={}, expiration=None):
        if expiration is None:
            expiration = self.TokenExpiration
        token = SignedToken(payload, subject=user, expiration=expiration, issuer=self.TokenIssuer)
        #print("generate_token: payload:", token.Payload)
        return token, token.encode(self.TokenSecret)

    def get_digest_password(self, realm, username):
        u = self.get_user(username)
        if u is None:
            return None
        return u.get_password(self.Realm)

    def _auth_digest(self, request_env, redirect):
        # give them cookie with the signed token
        
        ok, data = digest_server(self.Realm, request_env, self.get_digest_password)   # use the Group as realm, if present
        if ok:
            username = data
            return "ok", dict(username=username)
        elif data:
            return "continue", (401, "Authorization required", {
                'WWW-Authenticate': data
            })
        else:
            return "reject", "Authentication failed"

    def _auth_ldap(self, request, redirect, username):
        ldap_config = self.AuthConfig.get("ldap")
        if not ldap_config:
            return "reject", "LDAP is not configured"
        if username:
            password = to_str(request.body.strip())
        else:
            username, password = request.body.split(b":",1)
            username = to_str(username)
            password = to_str(password)
        u = self.get_user(username)
        config = self.AuthConfig["ldap"]
        result, reason, expiration = u.authenticate("ldap", config, password)
        #print("AuthCore._auth_ldap:", result, reason, expiration)
        if result:
            return "ok", dict(username=username)
        else:
            return "reject", "Authentication failed"

    def accepted_sci_token_issuer(self, issuer):
        return issuer in self.SciTokenIssuers

    def _auth_token(self, request, redirect, username):
        u = self.get_user(username)
        if u is None:
            return "reject", "Authentication failed"

        encoded = None
        headers = request.headers
        authorization = headers.get("Authorization")
        if authorization:
            try:
                encoded = authorization.split(None, 1)[-1]      # ignore "type", e.g. bearer
            except:
                pass

        if not encoded:
            encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded:
            return "reject", "Authentication failed. Token not found"

        result, reason, expiration = u.authenticate("scitoken", self.SciTokenIssuers, encoded)
        if not result:
            result, reason, expiration = u.authenticate("jwttoken", 
                    {
                        "issuer": self.TokenIssuer,
                        "secret": self.TokenSecret
                    }, encoded)
        if result:
            return "ok", dict(username=username, expiration=expiration)
        else:
            return "reject", "Authentication failed"

    def _auth_x509(self, request, redirect, username):
        #log = open("/tmp/_auth_x509.log", "w")
        #print("_auth_x509: request.environ:")
        #for k, v in sorted(request.environ.items()):
        #    print(f"   {k}={v}")
        #print("_auth_x509: scheme:", request.environ.get("REQUEST_SCHEME"))
        if request.environ.get("REQUEST_SCHEME") != "https":
            return "reject", "HTTPS scheme required"
            
        u = self.get_user(username)
        #print("_auth_x509: u:", username, u)
        result, reason, expiration = u.authenticate("x509", None, request.environ)
        
        if result:
            return "ok", dict(username=username)
        else:
            return "reject", None
        
    def authenticate(self, method, username, request, redirect):
        #print("AuthCore.authenticate: method:", method)
        try:
            if method == "x509":
                status, extra = self._auth_x509(request, redirect, username)
            elif method == "digest":
                status, extra = self._auth_digest(request.environ, redirect)
            elif method == "ldap":
                status, extra = self._auth_ldap(request, redirect, username)
            elif method == "token":
                status, extra = self._auth_token(request, redirect, username)
            else:
                status, extra = "reject", "Unknown authentication method"
        except:
            traceback.print_exc()
            raise

        return status, extra

    def user_from_request(self, request):
        token, error = self.token_from_request(request)
        #print("AuthenticationCore.user_from_request:", token, token.subject)
        if not token:
            return None, error
        return token.subject, None
            
    def token_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded:
            authorization = request.headers.get("Authorization")
            if authorization:
                try:
                    schema, tail = authorization.split(None, 1)
                    if schema.lower() == "bearer":  encoded = tail.strip()
                except:
                    pass
        #print("token_from_request: encoded:", encoded)
        if not encoded: return None, "Token not found"
        return self.verify_token(encoded)
