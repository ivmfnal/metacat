#
# Multi-group authentication server
#

from metacat.auth.server import BaseApp, AuthHandler
from metacat.auth.auth_core import AuthenticationCore
from webpie import Response, WPApp, WPHandler
from metacat.util import to_str

import time, os, yaml, os.path
from urllib.parse import quote_plus, unquote_plus

class MultiGroupAuthServerApp(BaseApp):

    def __init__(self, config, root_handler, **args):
        BaseApp.__init__(self, config, root_handler, **args)
        self.Cfg = config

        self.Cores = {
            group: AuthenticationCore(group_cfg, group)
            for group, group_cfg in config["groups"].items()
        }

    def groups(self):
        return self.Cores.keys()

    def auth_core(self, group):
        return self.Cores.get(group)
        
    def response_with_auth_cookie(self, user, redirect, token=None, expiration=None):
        # expiration here is absolute time
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
        if expiration is None:
            expiration = self.TokenExpiration + time.time()
        if token is not None:
            encoded = token.encode()
        else:
            token, encoded = self.AuthCore.generate_token(user, {"user": user}, expiration=expiration)
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
        #print("BaseApp.response_with_auth_cookie: returning", resp)
        return resp

        
class MultiGroupTopHandler(WPHandler):

    def __init__(self, request, app):
        WPHandler.__init__(self, request, app)
        for group in app.groups():
            self.addHandler(group, AuthHandler(request, app, group))

def create_application(config=None):
    if config is None:  config = os.environ.get("AUTH_SERVER_CFG")
    if config is None:  
        print("Config file is not defined. Use AUTH_SERVER_CFG environment variable", file=sys.stderr)
        sys.exit(1)
    if isinstance(config, str):
        config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)
    app = MultiGroupAuthServerApp(config, MultiGroupTopHandler)
    #templdir = config.get("templates", "")
    #if templdir.startswith("$"):
    #    templdir = os.environ[templdir[1:]]
    #app.initJinjaEnvironment(tempdirs=[templdir, "."])
    return app

if __name__ == "__main__":
    from webpie import HTTPServer
    import sys, getopt
    
    Usage = """
    python mg_auth_server.py [-p <port>] [-c <config.yaml>]
    """

    opts, args = getopt.getopt(sys.argv[1:], "c:p:")
    opts = dict(opts)
    config_file = opts.get("-c", os.environ.get("AUTH_SERVER_CFG"))
    if not config_file:
        print("Configuration file must be provided either using -c command line option or via AUTH_SERVER_CFG environment variable")
        sys.exit(1)
    config = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)
    app = create_application(config)
    port = int(opts.get("-p", config.get("port", -1)))
    if port == -1:
        print("Port number is not configured")
        sys.exit(1)

    key = cert = ca_file = None
    if "ssl" in config:
        key = config["ssl"]["key"]
        cert = config["ssl"]["cert"]
        ca_file = config["ssl"]["ca_bundle"]
        
    server = HTTPServer(port, app, certfile=cert, keyfile=key, verify="optional", ca_file=ca_file)
    print("staring on port", port)
    server.run()
else:
    application = create_application()
