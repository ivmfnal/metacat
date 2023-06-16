from metacat.auth import SignedToken
from .base_server import BaseHandler
from metacat.auth import BaseDBUser as DBUser, SignedToken
from metacat.util import to_str, to_bytes

import time, os, yaml, json, traceback
from datetime import datetime
from urllib.parse import quote_plus, unquote_plus
from webpie import Response

print("auth_handler importing")

class AuthHandler(BaseHandler):
    
    def __init__(self, request, app, group=None):
        #
        # group will be used by an app, which can do authentication for multiple groups
        # standard BaseApp ignores it
        BaseHandler.__init__(self, request, app, group)
        #print("AuthHandler(): created with group:", group, "   core:", self.AuthCore)
    
    def whoami(self, request, relpath, **args):
        user, error = self.AuthCore.user_from_request(request)
        return user or "", "text/plain"
        
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
        
    def ________token(self, request, relpath, download=False, **args):
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
        username, error = self.AuthCore.user_from_request(request)
        return ("OK","text/plain") if username else (error, 401)

    def auth(self, request, relpath, redirect=None, method="password", username=None, **args):
        status, extra = self.AuthCore.authenticate(method, username, request, redirect)
        #print("AuthHandler.auth:", method, status, extra)
        if status == "continue":
            return extra
        elif status == "reject":
            return 401, (extra or "Authentication failed") + "\n"
        elif status != "ok":
            return 401, "Unknown authentication status\n"
            
        # status == "ok":
        username = extra.get("username", username)
        if not username:
            return 401, "Authentication failed: unknown username\n"

        token, encoded = self.AuthCore.generate_token(username, expiration=extra.get("expiration"))
        headers = {"X-Authentication-Token": to_str(encoded)}
        http_status = 200
        if redirect:
            headers["Location"] = redirect
            http_status = 302
        return http_status, "", headers


class GUIAuthHandler(AuthHandler):
    
    def logout(self, request, relpath, redirect=None, **args):
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        try:    resp.set_cookie("auth_token", "-", max_age=1)
        except: pass
        return resp

    def login(self, request, relpath, redirect=None, **args):
        if redirect: redirect = unquote_plus(redirect)
        return self.render_to_response("login.html", redirect=redirect, **self.messages(args))
        
    def logged_in(self, request, relpath, **args):
        token = self.AuthCore.token_from_request(request)
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
            token, error = self.AuthCore.verify_token(token_text)
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
                #print("GUIAuthHandler: password:", password,"  hashed_password:", hashed_password)
                ok, reason, expiration = u.authenticate("password", self.App.Realm, hashed_password or password)
                #print("GUIAuthHandler: ok, reason:", ok, reason)
                if not ok and password:
                    ok, _, expiration = u.authenticate("ldap", self.AuthCore.auth_config("ldap"), password)
                if not ok:
                    self.redirect("%serror=%s" % (relogin_url, quote_plus("Authentication error")))
            else:
                self.redirect(relogin_url)

        if token is not None:
            expiration = token.expiration
            
        if expiration is None:
            expiration = self.AuthCore.TokenExpiration + time.time()

        if token is not None:
            encoded = token.encode()
        else:
            token, encoded = self.AuthCore.generate_token(username, expiration=expiration)

        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        #print ("response:", resp, "  reditrect=", redirect)
        resp.headers["X-Authentication-Token"] = to_str(encoded)
        resp.set_cookie("auth_token", encoded, max_age = max(0, int(expiration - time.time())))
        #print("GUIAuthHandler.do_login: returning", resp)
        return resp