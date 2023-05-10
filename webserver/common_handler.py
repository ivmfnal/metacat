from metacat.auth.server import BaseHandler
from metacat.db import DBUser, DBNamespace, DBRole
import re, json


_StatusReasons = {
    # Status Codes
    # Informational
    100: 'Continue',
    101: 'Switching Protocols',
    102: 'Processing',

    # Successful
    200: 'OK',
    201: 'Created',
    202: 'Accepted',
    203: 'Non-Authoritative Information',
    204: 'No Content',
    205: 'Reset Content',
    206: 'Partial Content',
    207: 'Multi Status',
    226: 'IM Used',

    # Redirection
    300: 'Multiple Choices',
    301: 'Moved Permanently',
    302: 'Found',
    303: 'See Other',
    304: 'Not Modified',
    305: 'Use Proxy',
    307: 'Temporary Redirect',
    308: 'Permanent Redirect',

    # Client Error
    400: 'Bad Request',
    401: 'Unauthorized',
    402: 'Payment Required',
    403: 'Forbidden',
    404: 'Not Found',
    405: 'Method Not Allowed',
    406: 'Not Acceptable',
    407: 'Proxy Authentication Required',
    408: 'Request Timeout',
    409: 'Conflict',
    410: 'Gone',
    411: 'Length Required',
    412: 'Precondition Failed',
    413: 'Request Entity Too Large',
    414: 'Request URI Too Long',
    415: 'Unsupported Media Type',
    416: 'Requested Range Not Satisfiable',
    417: 'Expectation Failed',
    418: "I'm a teapot",
    422: 'Unprocessable Entity',
    423: 'Locked',
    424: 'Failed Dependency',
    426: 'Upgrade Required',
    428: 'Precondition Required',
    429: 'Too Many Requests',
    451: 'Unavailable for Legal Reasons',
    431: 'Request Header Fields Too Large',

    # Server Error
    500: 'Internal Server Error',
    501: 'Not Implemented',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout',
    505: 'HTTP Version Not Supported',
    507: 'Insufficient Storage',
    510: 'Not Extended',
    511: 'Network Authentication Required',
}



class SanitizeException(Exception):
    pass
    
def _error_response(code, message, reason=None, type="json"):
    reason = reason or _StatusReasons.get(code, "unknown")
    if type == "json":
        text = json.dumps({ "message": message, "code":"%s %s" % (code, reason), "title":reason })
        content_type = "application/json"
    else:
        text = message
        content_type = "text/plain"
    return code, text, content_type

    
def sanitized(method):
    def decorated(*params, **agrs):
        try:    out = method(*params, **agrs)
        except SanitizeException as e:
            out = _error_response(400, str(e))
        return out
    return decorated

class MetaCatHandler(BaseHandler):
    
    def __init__(self, *params, **args):
        BaseHandler.__init__(self, *params, **args)
        self.NamespaceAuthorizations = {}
        
    SafePatterns = {
        "any": None,        # no-op
        "safe": re.compile(r"[^']+", re.I),
        "aname": re.compile(r"[a-z][a-z0-9_.-]*", re.I),
        "fname": re.compile(r"[a-z0-9_./-]+", re.I)
    }

    def sanitize(self, *words, allow="fname", **kw):
        pattern = self.SafePatterns[allow]
        if pattern is not None:
            for w in words:
                if w and not pattern.fullmatch(w):
                    raise SanitizeException("Invalid value: %s" % (w,))
            for name, value in kw.items():
                if value and not pattern.fullmatch(value):
                    raise SanitizeException("Invalid value for %s: %s" % (name, value))

    def authenticated_user(self):
        username, error = self.authenticated_username()
        if username is None:
            return None, error
        user = DBUser.get(self.App.connect(), username)
        if user is not None:
            return user, None
        else:
            return None, "user not found"

    def _namespace_authorized(self, db, namespace, user):
        authorized = self.NamespaceAuthorizations.get(namespace)
        if authorized is None:
            ns = DBNamespace.get(db, namespace)
            if ns is None:
                raise KeyError("Namespace %s does not exist")
            authorized = ns.owned_by_user(user)
            self.NamespaceAuthorizations[namespace] = authorized
        return authorized

    def error_response(self, code, message, reason=None, type="json"):
        reason = reason or _StatusReasons.get(code, "unknown")
        if type == "json":
            text = json.dumps({ "message": message, "code":"%s %s" % (code, reason), "title":reason })
            content_type = "application/json"
        else:
            text = message
            content_type = "text/plain"
        return code, text, content_type
