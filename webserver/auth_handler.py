from webpie import Response
from metacat.util import to_str, to_bytes, SignedToken
from metacat import Version
from metacat.db import DBUser
from urllib.parse import quote_plus, unquote_plus

from base_handler import BaseHandler

class AuthHandler(BaseHandler):

    def whoami(self, request, relpath, **args):
        return str(self.App.user_from_request(request)), "text/plain"
        
    def token(self, request, relpath, **args):
        return self.App.encoded_token_from_request(request)+"\n"
        
    def _auth_digest(self, request_env, redirect):
        from metacat.util import digest_server
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

    def _auth_ldap(self, request, redirect):
        
        # check HTTPS here
        
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
        if request.environ.get("HTTPS") != "on" \
                    or request.environ.get("HTTP_X_CLIENT_VERIFY") != "SUCCESS" \
                    or not request.environ.get("HTTP_X_DN"):
            return "Authentication failed\n", 403
        dn = request.environ["HTTP_X_DN"]
            
        db = self.App.connect()
        u = DBUser.get(db, username)
        if u.authenticate("x509", None, dn):
            return self.App.response_with_auth_cookie(username, redirect)
        else:
            return "Authentication failed\n", 403
        
    def auth(self, request, relpath, redirect=None, method="digest", username=None, **args):
        if method == "digest":
            return self._auth_digest(request.environ, redirect)
        elif method == "ldap":
            return self._auth_ldap(request, redirect)
        elif method == "x509":
            return self._auth_x509(request, redirect, username)
        else:
            return 400, "Unknown authentication method\n"
        
            
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
        username = self.App.user_from_request(request)
        return "OK" if username else ("Token verification error", 403)

