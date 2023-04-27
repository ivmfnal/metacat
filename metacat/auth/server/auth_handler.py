from metacat.auth import SignedToken
from .base_server import BaseHandler
from metacat.auth import BaseDBUser as DBUser, digest_server, SignedToken
from metacat.util import to_str, to_bytes

import time, os, yaml, json, traceback
from datetime import datetime
from urllib.parse import quote_plus, unquote_plus

#print("auth_handler importing")

class AuthHandler(BaseHandler):

    def whoami(self, request, relpath, **args):
        user, error = self.App.user_from_request(request)
        return user or "", "text/plain"
        
    def token(self, request, relpath, **args):
        return self.App.encoded_token_from_request(request)+"\n"
        
    def _auth_digest(self, request_env, redirect):
        # give them cookie with the signed token
        
        ok, data = digest_server(self.App.Realm, request_env, self.App.get_digest_password)
        if ok:
            #print("AuthHandler.auth: digest_server ok")
            resp = self.App.response_with_auth_cookie(data, redirect)
            return resp
        elif data:
            return 401, "Authorization required", {
                'WWW-Authenticate': data
            }

        else:
            return "Authentication failed\n", 401

    def _auth_ldap(self, request, redirect, username):
        if username:
            password = to_str(request.body.strip())
        else:
            username, password = request.body.split(b":",1)
            username = to_str(username)
            password = to_str(password)
        db = self.App.user_db()
        u = DBUser.get(db, username)
        config = self.App.auth_config("ldap")
        result, reason, expiration = u.authenticate("ldap", config, password)
        if result:
            #print("ldap authentication succeeded")
            return self.App.response_with_auth_cookie(username, redirect)
        else:
            return "Authentication failed\n", 401

    def _auth_token(self, request, redirect, username):
        db = self.App.user_db()
        u = DBUser.get(db, username)
        if u is None:
            return 401, "Authentication failed\n"

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
            return "Authentication failed. Token not found\n", 401

        result, reason, expiration = u.authenticate("scitoken", self.App.SciTokenIssuers, encoded)
        if not result:
            result, reason, expiration = u.authenticate("jwttoken", 
                    {
                        "issuer": self.App.Issuer,
                        "secret": self.App.TokenSecret
                    }, encoded)
        if result:
            return self.App.response_with_auth_cookie(username, redirect, expiration=expiration)
        else:
            return "Authentication failed\n", 401

    def _auth_x509(self, request, redirect, username):
        #log = open("/tmp/_auth_x509.log", "w")
        #print("request.environ:", file=log)
        #for k, v in sorted(request.environ.items()):
        #    print(f"{k}={v}", file=log)
        if request.environ.get("REQUEST_SCHEME") != "https":
            return "HTTPS scheme required\n", 401
            
        db = self.App.user_db()
        u = DBUser.get(db, username)
        #print("_auth_x509: u:", username, u, file=log)
        result, reason, expiration = u.authenticate("x509", None, request.environ)
        if result:
            return self.App.response_with_auth_cookie(username, redirect)
        else:
            return "Authentication failed\n", 401
        
    def auth(self, request, relpath, redirect=None, method="password", username=None, **args):
        #print("method:", method)
        try:
            if method == "x509":
                return self._auth_x509(request, redirect, username)
            elif method == "digest":
                return self._auth_digest(request.environ, redirect)
            elif method == "ldap":
                return self._auth_ldap(request, redirect, username)
            elif method == "token":
                return self._auth_token(request, redirect, username)
            else:
                return "Unknown authentication method\n", 400
        except:
            traceback.print_exc()
            raise
            
    def mydn(self, request, relpath):
        ssl = request.environ.get("HTTPS") == "on" or request.environ.get("REQUEST_SCHEME") == "https"
        if not ssl:
            return "Use HTTPS connection\n", 400
        if relpath == "issuer":
            return request.environ.get("SSL_CLIENT_I_DN","") + "\n", "text/plain"
        elif relpath == "subject":
            return request.environ.get("SSL_CLIENT_S_DN","") + "\n", "text/plain"
        else:
            return json.dumps({
                "subject":  request.environ.get("SSL_CLIENT_S_DN",""),
                "issuer":  request.environ.get("SSL_CLIENT_I_DN","")
            }) + "\n", "text/json"
        
    def logout(self, request, relpath, redirect=None, **args):
        return self.App.response_with_unset_auth_cookie(redirect)

    def login(self, request, relpath, redirect=None, **args):
        if redirect: redirect = unquote_plus(redirect)
        return self.render_to_response("login.html", redirect=redirect, **self.messages(args))
        
    def logged_in(self, request, relpath, **args):
        token = self.App.token_from_request(request)
        encoded = to_str(token.encode())
        exp = datetime.utcfromtimestamp(token.expiration)
        return self.render_to_response("show_token.html", token=token, expiration=exp, encoded=encoded)

    def do_login(self, request, relpath, **args):
        # handle SciTokens here !!!
        username = request.POST["username"]
        hashed_password = request.POST.get("hashed_password")
        password = request.POST.get("password")
        token_text = request.POST.get("token")
        redirect = request.POST.get("redirect")
        relogin_url = "./login"
        if redirect:
            relogin_url += "?redirect=%s&" % (quote_plus(redirect),)
        else:
            relogin_url += "?"
        token = None
        
        db = self.App.user_db()
        if token_text:
            token, error = self.App.verify_token(token_text)
            subject = token and token.subject
            if not subject:
                self.redirect("%serror=%s" % (relogin_url, quote_plus(error)))
            username = subject
            
        if not username:
            self.redirect("%serror=%s" % (relogin_url, quote_plus("Need username or token")))
            
        u = DBUser.get(db, username)
        if not u:
            #print("authentication error")
            self.redirect("%serror=User+%s+not+found" % (relogin_url, username))
        
        if not token:
            if (password or hashed_password) and username:
                ok, _, expiration = u.authenticate("password", self.App.Realm, hashed_password or password)
                if not ok and password:
                    ok, _, expiration = u.authenticate("ldap", self.App.auth_config("ldap"), password)
                if not ok:
                    self.redirect("%serror=%s" % (relogin_url, quote_plus("Authentication error")))
            else:
                self.redirect(relogin_url)

        #print("authenticated as", username)
        return self.App.response_with_auth_cookie(username, redirect, token=token)
        
    def token(self, request, relpath, download=False, **args):
        encoded = self.App.encoded_token_from_request(request)
        #print("token from request:", encoded)
        token = None
        if encoded:
            token, error = self.App.verify_token(encoded)
        if not token:
            #print("redirecting. error:", error)
            self.redirect("./login")
        headers = {"Content-Type":"text/plan"}
        if download == "yes":
            headers["Content-Disposition"] = "attachment"
        return 200, encoded, headers

    def verify(self, request, relpath, **args):
        username, error = self.App.user_from_request(request)
        return ("OK","text/plain") if username else (error, 401)

