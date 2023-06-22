import yaml, os, getopt, sys

from webpie import WPApp, WPHandler, Response, WPStaticHandler
from pythreader import schedule_task, Primitive, synchronized
from metacat.db import DBUser, DBRole, DBDataset
from metacat.filters import load_filters_module, standard_filters

from datetime import datetime, timezone
#import webpie
#print("webpie imported from:", webpie.__file__)

import json, time, secrets, traceback, hashlib, pprint
from urllib.parse import quote_plus, unquote_plus

from metacat.util import to_str, to_bytes
from metacat.common import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError

from metacat import Version
from wsdbtools import ConnectionPool

from gui_handler import GUIHandler
from data_handler import DataHandler
from metacat.auth.server import GUIAuthHandler, BaseApp
            
class RootHandler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.data = DataHandler(*params, **args)
        self.gui = GUIHandler(*params, **args)
        self.static = WPStaticHandler(*params, root=self.App.StaticLocation, cache_ttl=3600)
        self.auth = GUIAuthHandler(*params, **args)

    def index(self, req, relpath, **args):
        return self.redirect("./gui/index")
        
    def version(self, req, relpath, **args):
        return Version

    def probe(self, req, relpath, **args):
        try:    db = self.App.connect()
        except Exception as e:
            return 500, str(e)
        c = db.cursor()
        c.execute("select 1")
        return "OK" if c.fetchone()[0] == 1 else 500


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

def none_as_blank(x):
    if x is None:
        return ""
    else:
        return str(x)

def angle_brackets(text):
    if text is None:    return None
    return text.replace("<", "&lt;").replace(">", "&gt;")


class App(BaseApp, Primitive):

    Version = Version

    def __init__(self, cfg, root, static_location="./static", **args):
        BaseApp.__init__(self, cfg, root, **args)
        Primitive.__init__(self, name="MetaCat app")
        self.Title = cfg.get("site_title", "DEMO Metadata Catalog")
        
        self.StaticLocation = static_location

        self.StandardFilters = {}
        self.CustomFilters = {}

        filters_config = self.Cfg.get("filters", {})
        if filters_config.get("standard_filters", True):
            self.StandardFilters.update(standard_filters)

        for mod_spec in filters_config.get("modules", []):
            name = mod_spec["name"]
            env = mod_spec.get("env")
            filter_cfg = mod_spec.get("config")
            filters_from_module = load_filters_module(name, env, filter_cfg)
            self.CustomFilters.update(filters_from_module)

        self.Filters = {}
        self.Filters.update(self.StandardFilters)
        self.Filters.update(self.CustomFilters)
        self.init_auth_core(cfg)
        self.Realm = self.AuthCore.Realm

    def update_file_counts(self):
        db = self.connect()
        for dataset in DBDataset.list(db):
            self.DatasetCounts[(dataset.Namespace, dataset.Name)] = self.get_dataset_counts(dataset)

    @synchronized
    def ______dataset_file_counts(self, namespace, name):
        counts = self.DatasetCounts.get((namespace, name))
        if counts is None:
            db = self.connect()
            dataset = DBDataset.get(db, namespace, name)
            counts = self.DatasetCounts[(namespace, name)] = self.get_dataset_counts(dataset)
        return counts

    def filters(self):
        return self.Filters
        
    def init(self):
        #print("ScriptHome:", self.ScriptHome)
        self.initJinjaEnvironment(
            filters={"as_dt_utc":as_dt_utc,
                "as_dt_local":as_dt_local,
                "none_as_blank":none_as_blank,
                "angle_brackets":angle_brackets,
                "json": as_json
            },
            tempdirs=[self.ScriptHome, self.ScriptHome + "/templates"],
            globals={
                "G_Version": Version, 
                "G_SiteTitle": self.Title,
                "G_StaticRoot": self.externalPath("/" + self.StaticLocation)
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
    #print("static_location:", static_location)
    application=App(config, RootHandler, static_location=static_location, prefix=prefix)
    return application

if __name__ == "__main__":
    # running from shell
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
    #print("main: config:", config_file)
    port = int(opts.get("-p", config_file.get("port", 8080)))
    prefix = config_file.get("prefix")
    print(f"Starting the server on port {port} ...")   
    server = HTTPServer(port, create_application(config_file, prefix), debug=False, logging=True)
    server.run()
