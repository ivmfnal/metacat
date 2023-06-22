import requests, time, json, random
from urllib.parse import quote_plus, unquote_plus
from .exceptions import MCError, NotFoundError, InvalidArgument, PermissionError, BadRequestError, WebAPIError

INVALID_METADATA_ERROR_CODE = 488

def to_bytes(x):
    if not isinstance(x, bytes):
        x = x.encode("utf-8")
    return x
    
def to_str(x):
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x


class HTTPClient(object):

    InitialRetry = 1.0
    RetryExponent = 1.5
    DefaultTimeout = 300.0

    def __init__(self, server_url, token, timeout):
        self.ServerURL = server_url
        self.Token = token
        self.Timeout = timeout or self.DefaultTimeout
        self.LastResponse = self.LastStatusCode = None
        self.LastURL = ""

    def retry_request(self, method, url, timeout=None, **args):
        """
        Implements the functionality to retry on 503 response with random exponentially growing delay
        Use timemout = 0 to try the request exactly once
        Returns the response with status=503 on timeout
        """
        if timeout is None:
            timeout = self.DefaultTimeout
        tend = time.time() + timeout
        retry_interval = self.InitialRetry
        response = None
        done = False
        while time.time() < tend:
            if method == "get":
                response = requests.get(url, timeout=self.Timeout, **args)
            else:
                response = requests.post(url, timeout=self.Timeout, **args)
            if response.status_code != 503:
                break
            sleep_time = min(random.random() * retry_interval, tend-time.time())
            retry_interval *= self.RetryExponent
            if sleep_time > 0:
                time.sleep(sleep_time)
        return response
        
    def raise_on_error(self, response):
        if response.status_code == 403:
            raise PermissionError(self.LastURL, response)
        elif response.status_code == 404:
            raise NotFoundError(self.LastURL, response)
        elif response.status_code//100 == 4:
            raise BadRequestError(self.LastURL, response)
        elif response.status_code//100 != 2:
            raise WebAPIError(self.LastURL, response)

    def send_request(self, method, uri_suffix, headers=None, timeout=None, **args):
        self.LastURL = url = "%s/%s" % (self.ServerURL, uri_suffix)
        req_headers = {
            "Accept": "text/plain, application/json, text/json, application/json-seq"
        }
        try:
            req_headers.update(self.auth_headers())           # in case we have TokenAuthClientMixin or similar
        except AttributeError:
            pass
        if headers:
            req_headers.update(headers)
        self.LastResponse = response = self.retry_request(method, url, headers=req_headers, **args)
        #print(response, response.text)
        self.LastStatusCode = response.status_code
        self.raise_on_error(response)
        return response

    def get_text(self, uri_suffix):
        return self.send_request("get", uri_suffix).text

    def post_text(self, uri_suffix, data):
        return self.send_request("post", uri_suffix, data=data).text

    def unpack_json(self, json_text):
        results = json.loads(json_text)
        #if isinstance(results, dict):
        #    if "results" in results:
        #        results = results["results"]
        #    elif "error" in results:
        #        raise ServerReportedError(self.LastURL, self.LastStatusCode, results["error"]["type"], results["error"].get("value", ""))
        return results

    RS = b'\x1E'
    def unpack_json_seq(self, response):
        for line in response.iter_lines():
            if line:    line = line.strip()
            while line and line.startswith(self.RS):
                line = line[1:]
            if line:
                #print(f"stream line:[{line}]")
                obj = json.loads(line)
                yield obj

    def unpack_json_data(self, response):
        response_content_type = response.headers.get("content-type")
        if "application/json-seq" in response_content_type:
            return self.unpack_json_seq(response)
        else:
            return response.json()

    def get_json(self, uri_suffix):
        headers = {"Accept": "application/json-seq, application/json, text/json"}
        return self.unpack_json_data(self.send_request("get", uri_suffix, headers=headers, stream=True))

    def post_json(self, uri_suffix, data):
        if not isinstance(data, (str, bytes)):
            data = json.dumps(data)
        headers = {
                "Accept": "application/json-seq, application/json, text/json",
                "Content-Type": "text/json"
        }
        response = self.send_request("post", uri_suffix, data=data, headers=headers, stream=True)
        return self.unpack_json_data(response)

    def interpret_json_stream(self, response):
        for line in response.iter_lines():
            if line:    line = line.strip()
            while line.startswith(b'\x1E'):
                line = line[1:]
            if line:
                #print(f"stream line:[{line}]")
                obj = json.loads(line)
                yield obj
                    
    def interpret_response(self, response, none_if_not_found=True):
        if response.status_code != 200:
            if none_if_not_found and response.status_code == 404:
                return None
            elif response.status_code == 404:
                raise NotFoundError(self.LastURL, response)
            else:
                raise WebAPIError(self.LastURL, response)
        content_type = response.headers.get("Content-Type", "")
        if content_type.startswith("text/json"):
            return json.loads(response.text)
        elif content_type == "application/json-seq":
            return self.interpret_json_stream(response)
        else:
            return response.text

    def get(self, uri_suffix, none_if_not_found=False):
        if not uri_suffix.startswith("/"):  uri_suffix = "/"+uri_suffix
        url = "%s%s" % (self.ServerURL, uri_suffix)
        #print("url:", url)
        try:
            headers = self.auth_headers()           # in case we have TokenAuthClientMixin or similar
        except AttributeError:
            headers = {}
        response = self.retry_request("get", url, headers=headers)
        return self.interpret_response(response, none_if_not_found)

    def post(self, uri_suffix, data):
        #print("post_json: data:", type(data), data)
        
        if not uri_suffix.startswith("/"):  uri_suffix = "/"+uri_suffix
        
        if data is None or isinstance(data, (dict, list)):
            data = json.dumps(data)
        else:
            data = to_bytes(data)
        #print("post_json: data:", type(data), data)
            
        url = "%s%s" % (self.ServerURL, uri_suffix)
        
        try:
            headers = self.auth_headers()           # in case we have TokenAuthClientMixin or similar
        except AttributeError:
            headers = {}

        response = self.retry_request("post", url, data=data, headers=headers)
        return self.interpret_response(response)
