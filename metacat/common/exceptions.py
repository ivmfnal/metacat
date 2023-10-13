import json

def to_bytes(x):
    if not isinstance(x, bytes):
        x = x.encode("utf-8")
    return x
    
def to_str(x):
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x

class MCError(Exception):
    pass

class WebAPIError(MCError):
    
    Headline = "HTTP error"
    
    def __init__(self, url, response):
        self.URL = url
        self.StatusCode = response.status_code
        self.Message = None
        self.Body = to_str(response.text)
        if response.headers.get("content-type") in ("text/json", "application/json"):
            try:    
                self.Data = json.loads(response.text)
                if isinstance(self.Data, dict):
                    self.Message = self.Data.get("message", "")
            except:
                self.Data = None
        else:
            self.Message = to_str(response.text)
        
    def __str__(self):
        lines = []
        if self.Message:
            lines.append(self.Message)
        else:
            lines.append(self.Body)
        return "\n".join(lines)

    def json(self):
        return self.Data

"""old
class _____ServerSideError(WebAPIError):
    Headline = "Server side application error"

    def __init__(self, url, status_code, type, value):
        message = type
        if value:
            message += ": " + value
        WebAPIError.__init__(self, url, status_code, message=message)
"""

class InvalidArgument(WebAPIError):
    Headline = "Invalid argument"
        
class PermissionError(WebAPIError):
    Headline = "Permission denied"
        
class NotFoundError(WebAPIError):
    Headline = "Object not found"

class BadRequestError(WebAPIError):
    Headline = "Invalid request"

class AlreadyExists(WebAPIError):
    Headline = "Already exists"

