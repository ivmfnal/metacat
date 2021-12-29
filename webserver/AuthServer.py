from auth_handler import AuthHandler
from metacat.util import to_str, to_bytes, SignedToken


from webpie import WPApp, WPHandler, Response, WPStaticHandler
import psycopg2, json, time, secrets, traceback, hashlib, pprint, os, yaml
from metacat.db import DBUser
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_str, to_bytes, SignedToken
from metacat.mql import MQLQuery
from metacat import Version

from auth_handler import AuthHandler
from base_server import BaseApp

class AuthApp(BaseApp):
    pass

def create_application(config_path=None):
    config_path = config_path or os.environ.get("METACAT_SERVER_CFG")
    if not config_path:
        print("Config file is not defined. Use METACAT_SERVER_CFG environment variable")
    config = yaml.load(open(config_path, "r"), Loader=yaml.SafeLoader)  
    cookie_path = config.get("cookie_path", "/metadata")        # not used ???
    return AuthApp(config, AuthHandler)
    
if __name__ == "__main__":
    from webpie import HTTPServer
    import sys, getopt
    
    Usage = """
    python AuthServer.py [-p <port>] [-c <config.yaml>]
    """

    opts, args = getopt.getopt(sys.argv[1:], "c:p:")
    opts = dict(opts)
    config_file = opts.get("-c", os.environ.get("METACAT_SERVER_CFG"))
    if not config_file:
        print("Configuration file must be provided either using -c command line option or via METADATA_SERVER_CFG environment variable")
        sys.exit(1)
    
    config = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)  
    port = int(opts.get("-p", config.get("auth_port", -1)))
    if port == -1:
        print("AuthServer port is not configured")
        sys.exit(1)

    key = cert = ca_file = None
    if "ssl" in config:
        key = config["ssl"]["key"]
        cert = config["ssl"]["cert"]
        ca_file = config["ssl"]["ca_file"]
        
    application = create_application(config_file)
    
    server = HTTPServer(port, application, cert, key, verify="optional", ca_file=ca_file, 
        debug=sys.stdout)
    server.run()
else:
    application = create_application()

    
