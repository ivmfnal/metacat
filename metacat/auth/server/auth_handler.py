from metacat.auth import SignedToken
from .base_server import BaseHandler
from metacat.auth import BaseDBUser as DBUser, digest_server, SignedToken
from metacat.util import to_str, to_bytes

import time, os, yaml, json, traceback
from datetime import datetime
from urllib.parse import quote_plus, unquote_plus

#print("auth_handler importing")

class AuthHandler(BaseHandler):
    
    def __init__(self, request, app, group=None):
        #
        # group will be used by an app, which can do authentication for multiple groups
        # standard BaseApp ignores it
        BaseHandler.__init__(self, request, app)
        self.Group = group
        self.AuthCore = self.App.auth_core(self.Group)
    
    def whoami(self, request, relpath, **args):
        user, error = self.App.user_from_request(request, group)
        return user or "", "text/plain"
        
    def user_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        #print("server.user_from_request: encoded:", encoded)
        if not encoded: 
            return None, "Token not found"
        token, error = self.AuthCore.verify_token(encoded)
        #print("user_from_request: out:", out)
        return token and token.subject, error

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

    def token(self, request, relpath, **args):
        return self.App.encoded_token_from_request(request, group)+"\n"
      
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
        
        db = self.App.user_db(self.Group)
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

