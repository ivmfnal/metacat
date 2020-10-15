import requests, json, fnmatch
from metacat.util import to_str, to_bytes, TokenLib

class ServerError(Exception):
    
    def __init__(self, url, status_code, message):
        self.URL = url
        self.StatusCode = status_code
        self.Message = message
        
    def __str__(self):
        return f"MetaCatServer error:\n  URL: {self.URL}\n  HTTP status code: {self.StatusCode}\n  Message: {self.Message}"

class HTTPClient(object):

    def __init__(self, server_url, token):
        self.ServerURL = server_url
        self.Token = token

    def get_json(self, uri_suffix):
        url = "%s/%s" % (self.ServerURL, uri_suffix)
        response = requests.get(url, 
                headers = {"X-Authentication-Token": self.Token.encode()}
        )
        if response.status_code != 200:
            raise ServerError(url, response.status_code, response.text)
        data = json.loads(response.text)
        return data
        
    def post_json(self, uri_suffix, data):
        #print("post_json: data:", type(data), data)
        if isinstance(data, (dict, list)):
            data = json.dumps(data)
        else:
            data = to_bytes(data)
        #print("post_json: data:", type(data), data)
            
        
            
        url = "%s/%s" % (self.ServerURL, uri_suffix)
        
        response = requests.post(url, 
                data = data,
                headers = {"X-Authentication-Token": self.Token.encode()}
        )
        if response.status_code != 200:
            raise ServerError(url, response.status_code, response.text)
        data = json.loads(response.text)
        return data
        

class MetaCatClient(HTTPClient):
    
    def __init__(self, server_url):    
        self.TL = TokenLib()
        HTTPClient.__init__(self, server_url, self.TL.get(server_url))

    def list_datasets(self, namespace_pattern=None, name_pattern=None, with_file_counts=False):
        url = "data/datasets?with_file_counts=%s" % ("yes" if with_file_counts else "no")
        lst = self.get_json(url)
        for item in lst:
            namespace, name = item["namespace"], item["name"]
            if namespace_pattern is not None and not fnmatch.fnmatch(namespace, namespace_pattern):
                continue
            if name_pattern is not None and not fnmatch.fnmatch(name, name_pattern):
                continue
            yield item
    
    def get_dataset(self, spec, namespace=None, name=None):
        if namespace is not None:
            spec = namespace + ':' + name
        item = self.get_json(f"data/dataset?dataset={spec}")
        return item

    def create_dataset(self, spec, parent=None):
        url = f"data/create_dataset?dataset={spec}"
        if parent:
            url += f"&parent={parent}"
        return self.get_json(url)
        
    def add_files(self, dataset, file_list, namespace=None):
        url = f"data/add_files?dataset={dataset}"
        if namespace:
            url += f"&namespace={namespace}"
        out = self.post_json(url, file_list)
        return out

    def declare_files(self, dataset, file_list, namespace=None):
        url = f"data/declare?dataset={dataset}"
        if namespace:
            url += f"&namespace={namespace}"
        out = self.post_json(url, file_list)
        return out

    def update_meta(self, metadata, names=None, fids=None, mode="update", namespace=None):
        if (fids is None) == (names is None):
            raise ValueError("File list must be specified either as list or names or list of ids, but not both")
        url = f"data/update_meta?mode={mode}"
        if namespace:
            url += f"&namespace={namespace}"
        data = {
            "metadata":metadata
        }
        if names:
            data["names"] = names
        else:
            data["fids"] = fids
        out = self.post_json(url, data)
        return out
        
    def get_file(self, fid=None, name=None, with_metadata = True, with_relations=True):
        assert (fid is None) != (name is None), 'Either name="namespace:name" or fid="fid" must be specified, but not both'
        with_meta = "yes" if with_metadata else "no"
        with_rels = "yes" if with_relations else "no"
        url = f"data/file?with_metadata={with_meta}&with_relations={with_rels}"
        if name:
            url += f"&name={name}"
        else:
            url += f"&fid={fid}"        
        return self.get_json(url)

    def run_query(self, query, namespace=None, with_metadata=False, save_as=None):
        url = "data/query?with_meta=%s" % ("yes" if with_metadata else "no",)
        if namespace:
            url += f"&namespace={namespace}"
        if save_as:
            url += f"&save_as={save_as}"
        results = self.post_json(url, query)
        return results
    
    def create_namespace(self, name, owner_role=None):
        url = f"data/create_namespace?name={name}"
        if owner_role:
            url += f"&owner={owner_role}"
        return self.get_json(url)
        
    def get_namespace(self, name):
        return self.get_json(f"data/namespace?name={name}")
        
    def list_namespaces(self, pattern=None):
        lst = self.get_json("data/namespaces")
        for item in lst:
            if pattern is None or fnmatch.fnmatch(item["name"], pattern):
                yield item
    
    def login_password(self, username, password):
        from requests.auth import HTTPDigestAuth
        server_url = self.ServerURL
        url = "%s/%s" % (server_url, "/auth/auth")
        response = requests.get(url, auth=HTTPDigestAuth(username, password))
        if response.status_code != 200:
            raise ServerError(url, response.status_code, "Authentication failed")
        #print(response)
        #print(response.headers)
        token = response.headers["X-Authentication-Token"]
        self.TL[server_url] = token
        token = self.TL[server_url]
        return token["user"], token.Expiration

    def auth_info(self):
        server_url = self.ServerURL
        token = self.TL.get(server_url)
        if not token:
            raise ValueError("No token found for server %s" % (server_url,))
        url = server_url + "/auth/verify"
        response = requests.get(url, headers={
                "X-Authentication-Token":token.encode()
        })
        if response.status_code/100 == 2:
            return token["user"], token.Expiration
        else:
            raise ServerError(url, response.status_code, "Verification failed")
