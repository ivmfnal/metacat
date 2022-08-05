import yaml, os, getopt, sys

from webpie import WPApp, WPHandler, Response, WPStaticHandler
from metacat.db import DBUser, DBRole
from datetime import datetime, timezone
#import webpie
#print("webpie imported from:", webpie.__file__)

import json, time, secrets, traceback, hashlib, pprint
from urllib.parse import quote_plus, unquote_plus

from metacat.util import to_str, to_bytes
from metacat.auth import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError

from metacat import Version
from wsdbtools import ConnectionPool

from gui_handler import GUIHandler
from data_handler import DataHandler
from metacat.auth.server import AuthHandler, BaseApp
            
class RootHandler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.data = DataHandler(*params, **args)
        self.gui = GUIHandler(*params, **args)
        self.static = WPStaticHandler(*params, root=self.App.StaticLocation)
        self.auth = AuthHandler(*params, **args)

    def index(self, req, relpath, **args):
        return self.redirect("./gui/index")
        
    def version(self, req, relpath, **args):
        return Version

def as_dt_utc(t):
    # datetim in UTC
    if t is None:
        return ""
    if isinstance(t, (int, float)):
        t = datetime.utcfromtimestamp(t)
    return t.strftime("%Y-%m-%d %H:%M:%S")

def as_dt_local(t):
    # datetim in UTC
    if t is None:
        return ""
    if isinstance(t, (int, float)):
        t = datetime.utcfromtimestamp(t)
    return t.strftime("%Y-%m-%d %H:%M:%S")
    
def as_json(x):
    return json.dumps(x)



class App(BaseApp):

    Version = Version

    def __init__(self, cfg, root, static_location="./static", **args):
        BaseApp.__init__(self, cfg, root, **args)
        self.Title = cfg.get("site_title", "DEMO Metadata Catalog")
        
        self.StaticLocation = static_location
        self.DefaultNamespace = "dune"

        from metacat.filters import standard_filters

        self.Filters = {}
        self.Filters.update(standard_filters)

        try:
            from custom_filters import create_filters
            custom_filters = create_filters(self.Cfg.get("custom_filters", {}))
            #print("Custom filters imported:", ",".join(custom_filters.keys()))
            self.Filters.update(custom_filters)
        except:
            pass

    def filters(self):
        return self.Filters

    def init(self):
        #print("ScriptHome:", self.ScriptHome)
        self.initJinjaEnvironment(
            filters={"as_dt_utc":as_dt_utc,
                "as_dt_local":as_dt_local,
                "json": as_json
            },
            tempdirs=[self.ScriptHome, self.ScriptHome + "/templates"],
            globals={
                "GLOBAL_Version": Version, 
                "GLOBAL_SiteTitle": self.Title
            }
        )

def create_application(config=None, prefix=None):
    config = config or os.environ.get("METACAT_SERVER_CFG")
    if config is None:
        print("Configuration file must be provided using METACAT_SERVER_CFG environment variable")
        return None
    if isinstance(config, str):
        config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)  
    cookie_path = config.get("cookie_path", "/metacat")
    static_location = config.get("static_location", os.environ.get("METACAT_SERVER_STATIC_DIR", "static"))
    print("metacat server prefix:", prefix)
    application=App(config, RootHandler, static_location=static_location, prefix=prefix)

    if False:
        templdir = config.get("templates", "")
        if templdir.startswith("$"):
            templdir = os.environ[templdir[1:]]

        application.initJinjaEnvironment(
            filters={"as_dt_utc":as_dt_utc,
                "as_dt_local":as_dt_local,
                "json": as_json
            },
            tempdirs=[templdir, "."],
            globals={
                "GLOBAL_Version": Version, 
                "GLOBAL_SiteTitle": config.get("site_title", "DEMO Metadata Catalog")
            }
        )
    
    return application

if __name__ == "__main__":
    from webpie import HTTPServer
    import sys
    import yaml, os
    import sys, getopt

    opts, args = getopt.getopt(sys.argv[1:], "c:p:")
    opts = dict(opts)

    config_file = opts.get("-c") or os.environ.get("METACAT_SERVER_CFG")
    if config_file is None:
        print("Config file must be specified with -c or METACAT_SERVER_CFG")
        sys.exit(1)
    config_file = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)

    port = int(opts.get("-p", config_file.get("port", 8080)))
    prefix = config_file.get("prefix")
    print(f"Starting the server on port {port} ...")   
    server = HTTPServer(port, create_application(config_file, prefix=prefix), debug=sys.stdout)
    server.run()
    #application.run_server(port)
else:
    # running under uwsgi or sumilar framework - assume the framework handles the prefix
    print("creating application...")
    application = create_application(prefix="")
