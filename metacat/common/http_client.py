import requests, time, json, random
from urllib.parse import quote_plus, unquote_plus
from .exceptions import MCError, NotFoundError, ServerSideError, InvalidArgument, PermissionError, BadRequestError

class HTTPClient(object):

    InitialRetry = 1.0
    RetryExponent = 1.5
    DefaultTimeout = 300.0

    def __init__(self, server_url, token, timeout):
        self.ServerURL = server_url
        self.Token = token
        self.Timeout = timeout or self.DefaultTimeout
        self.LastResponse = self.LastURL = self.LastStatusCode = None

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
            raise PermissionError(url, response)
        elif response.status_code == 404:
            raise NotFoundError(url, response)
        elif response.status_code//100 == 4:
            raise BadRequestError(url, response)
        elif response.status_code//100 != 2:
            raise WebAPIError(url, response)

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

    def get_json_stream(self, uri_suffix):
        url = "%s/%s" % (self.ServerURL, uri_suffix)
        headers = {"Accept": "application/json-seq"}
        if self.Token is not None:
            headers["X-Authentication-Token"] = self.Token.encode()

        with self.retry_request("get", url, headers=headers, stream=True) as response:
            if response.status_code == INVALID_METADATA_ERROR_CODE:
                raise InvalidMetadataError(url, response.status_code, response.text)
            if response.status_code == 404:
                raise NotFoundError(url, response.status_code, response.text)
            elif response.status_code != 200:
                raise WebAPIError(url, response.status_code, response.text)
            
            if response.headers.get("Content-Type") != "application/json-seq":
                raise WebAPIError(url, 200, "Expected content type application/json-seq. Got %s instead." % (response.headers.get("Content-Type"),))

            for line in response.iter_lines():
                if line:    line = line.strip()
                while line.startswith(b'\x1E'):
                    line = line[1:]
                if line:
                    #print(f"stream line:[{line}]")
                    obj = json.loads(line)
                    yield obj

