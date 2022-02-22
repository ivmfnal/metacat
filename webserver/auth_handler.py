from webpie import Response, WPApp
from metacat.util import digest_server, to_str, to_bytes, SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError
from metacat import Version
from metacat.db import DBUser
from urllib.parse import quote_plus, unquote_plus
import hashlib, json

from base_handler import BaseHandler

class AuthAppMixin(object):

    def __init__(self, cfg):
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

class AuthHandler(BaseHandler):

    def whoami(self, request, relpath, **args):
        user, error = self.App.user_from_request(request)
        return user or "", "text/plain"
        
    def token(self, request, relpath, **args):
        return self.App.encoded_token_from_request(request)+"\n"
        
    def _auth_digest(self, request_env, redirect):
        # give them cookie with the signed token

        a = authenticator("digest", {"domain":"metadata", "get_password":self.App.get_digest_password})
        ok, data = a.authenticate(None, None, request_env)
        if ok:
            #print("AuthHandler.auth: digest_server ok")
            resp = self.App.response_with_auth_cookie(data, redirect)
            return resp
        elif data:
            return Response("Authorization required", status=401, headers={
                'WWW-Authenticate': data
            })

        else:
            return "Authentication failed\n", 403

    def _auth_ldap(self, request, redirect, username):
        
        # check HTTPS here
        
        if username:
            password = to_str(request.body.strip())
        else:
            username, password = request.body.split(b":",1)
            username = to_str(username)
            password = to_str(password)
        db = self.App.connect()
        u = DBUser.get(db, username)
        config = self.App.auth_config("ldap")
        if u.authenticate("ldap", config, password):
            return self.App.response_with_auth_cookie(username, redirect)
        else:
            return "Authentication failed\n", 403
            
    def _auth_x509(self, request, redirect, username):
        ssl = request.environ.get("HTTPS") == "on" or request.environ.get("REQUEST_SCHEME") == "https"
        if not ssl:
            return "Authentication failed\n", 403
            
        db = self.App.connect()
        u = DBUser.get(db, username)
        if u.authenticate("x509", None, request.environ):
            return self.App.response_with_auth_cookie(username, redirect)
        else:
            return "Authentication failed\n", 403
        
    def auth(self, request, relpath, redirect=None, method="password", username=None, **args):
        if method == "x509":
            return self._auth_x509(request, redirect, username)
        elif method == "digest":
            return self._auth_digest(request.environ, redirect)
        elif method == "ldap":
            return self._auth_ldap(request, redirect, username)
        else:
            return "Unknown authentication method\n", 400
            
    def mydn(self, request, relpath):
        ssl = request.environ.get("HTTPS") == "on" or request.environ.get("REQUEST_SCHEME") == "https"
        if not ssl:
            return "Use HTTPS connection\n", 400
        return json.dumps({
            "subject":  request.environ.get("SSL_CLIENT_S_DN",""),
            "issuer":  request.environ.get("SSL_CLIENT_I_DN","")
        }), "text/json"
        
    def logout(self, request, relpath, redirect=None, **args):
        return self.App.response_with_unset_auth_cookie(redirect)

    def login(self, request, relpath, redirect=None, **args):
        return self.render_to_response("login.html", redirect=redirect, **self.messages(args))
        
    def do_login(self, request, relpath, **args):
        username = request.POST["username"]
        password = request.POST["password"]
        redirect = request.POST.get("redirect", self.scriptUri() + "/gui/index")
        #print("redirect:", redirect)
        db = self.App.connect()
        u = DBUser.get(db, username)
        if not u:
            #print("authentication error")
            self.redirect("./login?message=User+%s+not+found" % (username,))
        
        ok = u.authenticate("password", None, password)
        if not ok:
            ok = u.authenticate("ldap", self.App.auth_config("ldap"), password)

        if not ok:
            self.redirect("./login?error=%s" % (quote_plus("Authentication error"),))
            
        #print("authenticated")
        return self.App.response_with_auth_cookie(username, redirect)

    def verify(self, request, relpath, **args):
        username, error = self.App.user_from_request(request)
        return ("OK","text/plain") if username else (error, 403)

