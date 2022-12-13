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

class ServerSideError(WebAPIError):
    Headline = "Server side application error"

    def __init__(self, url, status_code, type, value):
        message = type
        if value:
            message += ": " + value
        HTTPError.__init__(self, url, status_code, message=message)

class InvalidArgument(WebAPIError):
    Headline = "Invalid argument"
        
class PermissionError(WebAPIError):
    Headline = "Permission denied"
        
class NotFoundError(WebAPIError):
    Headline = "Object not found"

class BadRequestError(WebAPIError):
    Headline = "Invalid request"

