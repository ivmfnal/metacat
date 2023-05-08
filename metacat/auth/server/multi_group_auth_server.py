from metacat.auth.server import BaseApp, AuthHandler
from webpie import Response

import time, os, yaml, os.path
from urllib.parse import quote_plus, unquote_plus

class AuthApp(BaseApp):
    
    def __init__(self, cfg):
        BaseApp.__init__(self, cfg, AuthHandler)

def create_application(config_path=None):
    config_path = config_path or os.environ.get("AUTH_SERVER_CFG")
    if not config_path:
        print("Config file is not defined. Use AUTH_SERVER_CFG environment variable")
    config = yaml.load(open(config_path, "r"), Loader=yaml.SafeLoader)
    app = AuthApp(config, AuthHandler)
    templdir = config.get("templates", "")
    if templdir.startswith("$"):
        templdir = os.environ[templdir[1:]]
    app.initJinjaEnvironment(tempdirs=[templdir, "."])
    return app

class GroupRouter(object):
    
    # WSGI application which routes the request to a specific App based on the first element in the URL path
    
    def __init__(self, config_path):
        config = yaml.load(open(config_path, "r"), Loader=yaml.SafeLoader)
        self.Apps = {       # group -> WPApp
            group: AuthApp(cfg)
            for group, cfg in config.items()
        }          
    
    def __call__(self, environ, start_response):
        path = environ["PATH_INFO"]
        if path and path.startswith("/"):
            path = path[1:]
        group, tail = path.split("/", 1)
        app = self.Apps.get(group)
        if app is None:
            return Response("Group not found", status=404)(environ, start_response)
        return app(environ | {"PATH_INFO": "/" + tail}, start_response)

application = GroupRouter(os.environ.get("AUTH_SERVER_CFG"))


class 