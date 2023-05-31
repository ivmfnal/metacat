# common functionality for Auth, GUI and Data servers

from webpie import WPApp, Response, WPHandler
from wsdbtools import ConnectionPool
from metacat.util import to_str, to_bytes
from metacat.auth import BaseDBUser, \
    SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError
from metacat.auth.auth_core import AuthenticationCore
import psycopg2, json, time, secrets, traceback, hashlib, pprint, os, yaml
from urllib.parse import quote_plus, unquote_plus

class BaseApp(WPApp):

    def __init__(self, cfg, root_handler, **args):
        WPApp.__init__(self, root_handler, **args)
        self.Cfg = cfg

        self.DB = self.DBSchema = None
        if "database" in cfg:
            db_config = cfg["database"]
            connstr = self.connstr(db_config)
            self.DB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=20)
            self.DBSchema = db_config.get("schema")

        if "user_database" in cfg:
            connstr = self.connstr(cfg["user_database"])
            self.UserDB = ConnectionPool(postgres=connstr, max_idle_connections=1, idle_timeout=20)
            self.UserDBSchema = cfg["user_database"].get("schema")
        elif self.DB is not None:
            self.UserDB = self.DB
            self.UserDBSchema = self.DBSchema

        self.Realm = None
        self.Group = None
        self.AuthCore = None
        
    def connstr(self, cfg):
        cs = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s" % cfg
        if cfg.get("password"):
            cs += " password=%(password)s" % cfg
        return cs
        
            
    def init(self):
        #print("ScriptHome:", self.ScriptHome)
        self.initJinjaEnvironment(tempdirs=[self.ScriptHome, self.ScriptHome + "/templates"])
        
    def init_auth_core(self, config):
        self.AuthCore = AuthenticationCore(config)
        return self.AuthCore

    def connect(self):
        conn = self.DB.connect()
        #print("conn: %x" % (id(conn),), "   idle connections:", ",".join("%x" % (id(c),) for c in self.DB.IdleConnections))
        if self.DBSchema:
            conn.cursor().execute(f"set search_path to {self.DBSchema}")
        return conn
        
    db = connect        # for compatibility
    
    def user_db(self, group=None):
        # group is ignored, can be used by a subclass
        conn = self.UserDB.connect()
        if self.UserDBSchema:
            conn.cursor().execute(f"set search_path to {self.UserDBSchema}")
        return conn
        
    def auth_config(self, method, group=None):
        return self.auth_core(group).auth_config(method)

    # overridable
    def auth_core(self, group = None):
        if group is None:
            group = self.Realm
        if group != self.Realm:
            raise KeyError("Can not find authentication core for group %s" % (group,))
        return self.AuthCore

    def get_digest_password(self, realm, username):
        db = self.connect()
        u = BaseDBUser.get(db, username)
        if u is None:
            return None
        return u.get_password(self.Realm)

    TokenExpiration = 24*3600*7

    def user_from_request(self, request):
        return self.AuthCore.user_from_request(request)
            
class BaseHandler(WPHandler):
    
    def __init__(self, request, app, group=None):
        WPHandler.__init__(self, request, app)
        self.Group = group
        self.AuthCore = app.auth_core(group)
    
    def connect(self):
        return self.App.connect()

    def text_chunks(self, gen, chunk=10000):
        buf = []
        size = 0
        for x in gen:
            n = len(x)
            buf.append(x)
            size += n
            if size >= chunk:
                #print("yielding:", "".join(buf))
                yield "".join(buf)
                size = 0
                buf = []
        if buf:
            #print("final yielding:", "".join(buf))
            yield "".join(buf)
            
    def authenticated_username(self):
        username, error = self.AuthCore.user_from_request(self.Request)
        return username, error

    def authenticated_user(self):
        username, error = self.authenticated_username()
        if username:
            user = self.AuthCore.get_user(username)
            if user is not None:
                return user, None
            error = f"user {username} not found"
        return None, error

    def jinja_globals(self):
        return {"G_User":self.authenticated_user()[0]}

    def messages(self, args):
        return {k: unquote_plus(args.get(k,"")) for k in ("error", "message")}
        

