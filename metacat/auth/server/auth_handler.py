from metacat.auth import SignedToken
from .base_server import BaseHandler
from metacat.auth import BaseDBUser as DBUser
from metacat.util import to_str, to_bytes

import time, os, yaml, json
from datetime import datetime
from urllib.parse import quote_plus, unquote_plus

class AuthHandler(BaseHandler):

    def whoami(self, request, relpath, **args):
        user, error = self.App.user_from_request(request)
        return user or "", "text/plain"
        
    def token(self, request, relpath, **args):
        return self.App.encoded_token_from_request(request)+"\n"
        
    def _auth_digest(self, request_env, redirect):
        from .rfc2617 import digest_server
        # give them cookie with the signed token
        
        ok, data = digest_server("metadata", request_env, self.App.get_digest_password)
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
        db = self.App.user_db()
        u = DBUser.get(db, username)
        config = self.App.auth_config("ldap")
        #print("ldap config:", config)
        if u.authenticate("ldap", config, password):
            return self.App.response_with_auth_cookie(username, redirect)
        else:
            return "Authentication failed\n", 403
            
    def _auth_x509(self, request, redirect, username):
        ssl = request.environ.get("HTTPS") == "on" or request.environ.get("REQUEST_SCHEME") == "https"
        if not ssl:
            return "Authentication failed\n", 403
            
        db = self.App.user_db()
        u = DBUser.get(db, username)
        if u.authenticate("x509", None, {
            "subject_dn":   request.environ.get("SSL_CLIENT_S_DN"),
            "issuer_dn":    request.environ.get("SSL_CLIENT_I_DN")
        }):
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
        return self.render_to_response("login.html", redirect=redirect, **self.messages(args))
        
    def logged_in(self, request, relpath, **args):
        token = self.App.token_from_request(request)
        encoded = to_str(token.encode())
        exp = datetime.utcfromtimestamp(token.expiration)
        return self.render_to_response("show_token.html", token=token, expiration=exp, encoded=encoded)

    def do_login(self, request, relpath, **args):
        username = request.POST["username"]
        password = request.POST.get("password")
        token_text = request.POST.get("token_text")
        redirect = request.POST.get("redirect", self.scriptUri() + "./logged_in")
        #print("redirect:", redirect)
        db = self.App.user_db()
        u = DBUser.get(db, username)
        if not u:
            #print("authentication error")
            self.redirect("./login?message=User+%s+not+found" % (username,))

        token = None
        if token_text:
            token, error = self.App.verify_token(token_text)
            if not token:
                self.redirect("./login?error=%s" % (quote_plus("Authentication error"),))
            if token.subject != username:
                self.redirect("./login?error=%s" % (quote_plus("Authentication error"),))
        elif password:
            ok = u.authenticate("password", None, password)
            if not ok:
                ok = u.authenticate("ldap", self.App.auth_config("ldap"), password)
            if not ok:
                self.redirect("./login?error=%s" % (quote_plus("Authentication error"),))
        else:
            self.redirect("./login?error=%s" % (quote_plus("Authentication error"),))

        #print("authenticated")
        return self.App.response_with_auth_cookie(username, redirect, token)
        
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
        return ("OK","text/plain") if username else (error, 403)

