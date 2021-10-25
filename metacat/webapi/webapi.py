import requests, json, fnmatch, sys, os
from metacat.util import to_str, to_bytes, password_hash, SignedToken, TokenLib
from pythreader import Task, TaskQueue, Promise
from urllib.parse import quote_plus, unquote_plus

INVALID_METADATA_ERROR_CODE = 488

class ServerError(Exception):
    
    def __init__(self, url, status_code, message, body=""):
        self.URL = url
        self.StatusCode = status_code
        self.Message = message
        self.Body = to_str(body)
        
    def __str__(self):
        msg = f"MetaCatServer error:\n  URL: {self.URL}\n  HTTP status code: {self.StatusCode}\n  Message: {self.Message}"
        if self.Body:
            msg += "\nMessage from the server:\n"+self.Body+"\n"
        return msg
        
class WebAPIError(ServerError):
    
    def __init__(self, url, status_code, body):
        ServerError.__init__(self, url, status_code, "", body)
    
    def json(self):
        #print("WebAPIError.json: body:", self.Body)
        return json.loads(self.Body)
        
class InvalidMetadataError(WebAPIError):

    def __str__(self):
        msg = ["Metadata error"]
        for item in self.json():
            msg.append("  %s" % (item["message"]))
            for error in item.get("metadata_errors", []):
                msg.append("    %s: %s" % (error["name"], error["reason"]))
        return "\n".join(msg)

class AuthenticationError(WebAPIError):
    def __init__(self, message):
        self.Message = message
        
    def __str__(self):
        return f"Authentication error: {self.Message}"
    

class HTTPClient(object):

    def __init__(self, server_url, token):
        self.ServerURL = server_url
        self.Token = token

    def get_json(self, uri_suffix):
        url = "%s/%s" % (self.ServerURL, uri_suffix)
        headers = {}
        if self.Token is not None:
            headers["X-Authentication-Token"] = self.Token.encode()
        response = requests.get(url, headers =headers)
        if response.status_code == INVALID_METADATA_ERROR_CODE:
            raise InvalidMetadataError(url, response.status_code, response.text)
        if response.status_code != 200:
            raise WebAPIError(url, response.status_code, response.text)
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
        
        headers = {}
        if self.Token is not None:
            headers["X-Authentication-Token"] = self.Token.encode()
        #print("HTTPClient.post_json: url:", url)
        #print("HTTPClient.post_json: data:", data)
        
        response = requests.post(url, data = data, headers = headers)
        if response.status_code == INVALID_METADATA_ERROR_CODE:
            #print("raising InvalidMetadataError")
            raise InvalidMetadataError(url, response.status_code, response.text)
        if response.status_code != 200:
            raise WebAPIError(url, response.status_code, response.text)
        #print("response.text:", response.text)
        data = json.loads(response.text)
        return data
        

class MetaCatClient(HTTPClient):
    
    def __init__(self, server_url=None, auth_server_url=None, max_concurrent_queries = 5,
                token = None, token_file = None):    

        """Initializes the MetaCatClient object

        Parameters
        ----------
        server_url : str
            The server endpoint URL, defult = from METACAT_SERVER_URL environment variable 
        auth_server_url : str
            The endpoint URL for the Authentication server, default = server_url + "/auth"
        max_concurrent_queries : int, optional
            Controls the concurrency when asynchronous queries are used
        token_file : str
            File path to read the authentication token from
        token : bytes or str or SignedToken
            Use this token for authentication, optional
        """

        self.TokenLib = self.Token = None
        self.TokenFile = token_file

        if token_file and token is None:
            token = self.resfresh_token()

        if token is not None:
            if isinstance(token, (str, bytes)):
                token = SignedToken.decode(token)
            
        if token is None:
            self.TokenLib = TokenLib()
            token = self.TokenLib.get(server_url)
        
        server_url = server_url or os.environ.get("METACAT_SERVER_URL")
        if not server_url:
            raise RuntimeError("MetaCat server URL unspecified")
        
        HTTPClient.__init__(self, server_url, token)
        self.AuthURL = auth_server_url or server_url + "/auth"
        self.QueryQueue = TaskQueue(max_concurrent_queries)       
        
    def resfresh_token(self):
        if self.TokenFile:
             token = open(self.TokenFile, "rb").read()
             self.Token = SignedToken.decode(token)
        return self.Token

    def list_datasets(self, namespace_pattern=None, name_pattern=None, with_file_counts=False):
        """Gets the list of datasets with namespace/name matching the templates. The templates are
        Python ``fnmatch`` module style templates where ``'*'`` matches any substring and ``'?'`` matches a single character.
        
        Parameters
        ----------
        namespace_pattern : str
        name_pattern : str
        with_file_counts : boolean
            controls whether the results should include file counts or dataset names only
        
        Yields
        ------
        generator
            yields dictionaries like {"namespace":..., "name":..., "file_count":...}
        """        
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
        """Gets single dataset
        
        Parameters
        ----------
        namespace : str
        name : str
        
        Returns
        -------
        dict
            dataset attributes
        """        
        
        if namespace is not None:
            spec = namespace + ':' + name
        item = self.get_json(f"data/dataset?dataset={spec}")
        return item

    def create_dataset(self, spec, parent=None):
        """Creates new dataset. Requires client authentication.
        
        Parameters
        ----------
        spec : str
            "namespace:name"
        
        Returns
        -------
        dict
            created dataset attributes
        """        
        url = f"data/create_dataset?dataset={spec}"
        if parent:
            url += f"&parent={parent}"
        return self.get_json(url)
        
    def add_files(self, dataset, file_list, namespace=None):
        """Add existing files to an existing dataset. Requires client authentication.
        
        Parameters
        ----------
        dataset : str
            "namespace:name"
        file_list : list
            List of dictionaries, one dictionary per file. Each dictionary must contain either a file id
        
            .. code-block:: python
        
                    { "fid": "..." }

            or a file namespace/name:
        
            .. code-block:: python

                    { "name": "namespace:name" }
        
        namespace : str, optional
            Default namespace. If a ``file_list`` item is specified with a name without a namespace, the ``default namespace``
            will be used.
        
        Returns
        -------
        list
            list of dictionaries, one dictionary per file with file ids: { "fid": "..." }
        """        
        url = f"data/add_files?dataset={dataset}"
        if namespace:
            url += f"&namespace={namespace}"
        out = self.post_json(url, file_list)
        return out

    def declare_files(self, dataset, file_list, namespace=None):
        """Declare new files and add them to an existing dataset. Requires client authentication.
        
        Parameters
        ----------
        dataset : str
            "namespace:name"
        file_list : list
            List of dictionaries, one dictionary per file. Each dictionary must contain at least filename and
            may contain other items (see Notes below)
        namespace: str, optional
            Default namespace for files to be declared
        
        Returns
        -------
        list
            list of dictionaries, one dictionary per file with file ids: { "fid": "..." }
        
        Notes
        -----
        
            Each file to be added must be represented with a dictionary. The dictionary must contain at least filename.
            It may also explicitly include file namespace, or the value of the ``namespace`` argument will be used.
        
            .. code-block:: python
        
                [
                    { 
                        "name": "namespace:filename",       # namespace can be specified for each file explicitly,
                        "name": "filename",                 # or implicitly using the namespace=... argument
                        "fid":  "...",                      # file id, optional. Will be auto-generated if unspecified.
                        "parents": ["fid","fid",...],       # list of parent file ids, optional
                        "metadata": {...},                  # file metadata, optional
                        "checksums": {                      # checksums dictionary, optional
                            "method": "value",...
                        }
                    },...
                ]
        
        
        
        """        

        url = f"data/declare_files?dataset={dataset}"
        if namespace:
            url += f"&namespace={namespace}"
        out = self.post_json(url, file_list)
        return out

    def update_file_meta(self, metadata, names=None, fids=None, mode="update", namespace=None):
        """Updates metadata for existing files. Requires client authentication.
        
        Parameters
        ----------
        metadata : dict or list
            see Notes
        names : list
            Either list of filenames (if ``namespace`` argument is used), or a list of "namespace:filename" combinations
            for the files.
        fids : list
            List of file ids. The list of files can be specified with ``fids`` or with ``names`` argument, but not
            both.
        mode : str
            Either ``"update"`` (default) or ``"replace"``. If mode is ``"update"``, existing metadata will be updated with
            values in ``metadata``. If ``"replace"``, then new values will replace existing metadata. Also, see notes below.
        
        Returns
        -------
        list
            list of dictionaries, one dictionary per file with file ids: { "fid": "..." }
        
        
        Notes
        -----
        This method can be be used in 2 different ways:
            * to apply the same metadata change to a list of files
            * to update a set of files individually
        
        To apply *common changes* to multiple files, use a dictionary as the value for ``metadata`` argument and
        specify the list of files to be affected either with ``fids`` or with ``names`` argument.
        The ``metadata`` dictionary will be used to either update existing metadata of listed files (if ``mode="update"``) or
        replace it (if ``mode="replace"``).
        
        To make changes on *file-by-file basis*, use a list of dictionaries for ``metadata`` argument. In this
        case, ``names`` and ``fids`` arguments of the method are ignored. The ``metadata`` list should look like this:
        
        .. code-block:: python
    
            [
                {       
                    "name": "namespace:filename",       # namespace can be specified for each file explicitly,
                    "name": "filename",                 # or implicitly using the namespace=... argument
                    "fid":  "...",                      # file id, optional. 
        
                                                        # Each dictionary in the list
                                                        #   must have either ``"name"`` element or ``"fid"``
                    
                    "parents":  ["fid",...],            # list of ids for the file parent files, optional
                    "metadata": { ... },                # new metadata values, optional,
                                                        #   will be used to either update or replace existing file metadata
                    "checksums": { ... }                # optional dictionary with checksums, will update or replace existing
                                                        #   checksums dictionary
                }, ...
            ]
        
        In this case, you can also update file parentage and checksums dictionary.
        
        """        
        if (fids is None) == (names is None):
            raise ValueError("File list must be specified either as list or names or list of ids, but not both")
        url = f"data/update_file_meta?mode={mode}"
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
        
    def update_dataset_meta(self, metadata, dataset, mode="update"):   
        """Update dataset metadata. Requires client authentication.
        
        Parameters
        ----------
        metadata : dict
            New metadata values
        dataset : str
           "namespace:name"
        mode: str
            Either ``"update"`` or ``"replace"``. If ``"update"``, metadata will be updated with new values. If ``"replace"``,
            metadata will be replaced with new values.

        Returns
        -------
        dict
            dictionary with new metadata values
        """        
        url = f"data/update_dataset_meta?mode={mode}&dataset={dataset}"
        out = self.post_json(url, metadata)
        return out
        
    def get_files(self, lookup_list, with_metadata = True, with_provenance=True):
        """Get many file records
        
        Parameters
        ----------
        lookup_list : list
            List of dictionaries, one dictionary per file. Each dictionary must have either
                "name":"namespace:name", or
                "fid":"file id"
        with_metadata : boolean
            whether to include file metadata
        with_provenance:
            whether to include parents and children list

        Returns
        -------
        List of file records, each record is the same as returned by get_file()
        """
        
        with_metadata = "yes" if with_metadata else "no"
        with_provenance = "yes" if with_provenance else "no"
        
        #print("with_metadata:", with_metadata)
        
        return self.post_json("data/files?with_metadata=%s&with_provenance=%s" % (with_metadata, with_provenance), 
            lookup_list) 
        
    def get_file(self, fid=None, name=None, with_metadata = True, with_provenance=True):
        """Get one file record
        
        Parameters
        ----------
        fid : str, optional
            File id
        name : str, optional
            "nemaspace:name" either ``fid`` or ``name`` must be specified
        with_metadata : boolean
            whether to include file metadata
        with_provenance:
            whether to include parents and children list

        Returns
        -------
        dict
            dictionary with file information

            .. code-block:: python

                {       
                    "name": "namespace:filename",       # file name, namespace
                    "fid":  "...",                      # files id
                    "creator":  "...",                  # username of the file creator
                    "created_timestamp":   ...,         # numeric UNIX timestamp
                    "size": ...,                        # file size in bytes
                    "parents":  ["fid",...],            # list of ids for the file parent files
                    "children": ["fid",...],            # list of ids for the file child files
                    "metadata": { ... },                # file metadata
                    "checksums": { ... }                # file checksums
                }
        
        Notes
        -----
        Retrieving file provenance and metadata takes slightly longer time
        """        
        assert (fid is None) != (name is None), 'Either name="namespace:name" or fid="fid" must be specified, but not both'
        with_meta = "yes" if with_metadata else "no"
        with_rels = "yes" if with_provenance else "no"
        url = f"data/file?with_metadata={with_meta}&with_provenance={with_rels}"
        if name:
            url += f"&name={name}"
        else:
            url += f"&fid={fid}"        
        return self.get_json(url)

    def query(self, query, namespace=None, with_metadata=False, with_provenance=False, save_as=None, add_to=None):
        """Run file query. Requires client authentication if save_as or add_to are used.
        
        Parameters
        ----------
        query : str
            Query in MQL
        namespace : str
            default namespace for the query
        with_metadata : boolean
            whether to return file metadata
        with_provenance:
            whether to return parents and children list
        save_as:
            namespace:name for a new dataset to create and add found files to
        add_to:
            namespace:name for an existing dataset to add found files to

        Returns
        -------
        list of dicts
            dictionary with file information. Each file will be represented with a dictionary in this list.
        
        Notes
        -----
        Retrieving file provenance and metadata takes slightly longer time
        """
        
        url = "data/query?with_meta=%s&with_provenance=%s" % ("yes" if with_metadata else "no","yes" if with_provenance else "no")
        if namespace:
            url += f"&namespace={namespace}"
        if save_as:
            url += f"&save_as={save_as}"
        if add_to:
            url += f"&add_to={add_to}"
        results = self.post_json(url, query)
        return results
        
    def async_query(self, query, data, **args):
        """Run file query asynchronously. Requires client authentication if save_as or add_to are used.
        
        Parameters
        ----------
        query : str
            Query in MQL
        data : anything
            Arbitrary data associated with this query
        args : 
            Same keyword arguments as for the run_query() method
        
        Returns
        -------
        Promise
            ``pythreader`` Promise object associated with this query. The promise object will have Data attribute containig the object passed as the ``data``
            argument to the ``async_query`` call. 
        
            See notes below for more on how to use this method.
            
        

        """
        
        class QueryTask(Task):
            def __init__(self, client, query, promise, args):
                Task.__init__(self)
                self.Client = client
                self.Query = query
                self.Args = args
                self.Promise = promise
                
            def run(self):
                #print("QueryTask: started:", self.Query)
                try:    
                    results = self.Client.query(self.Query, **self.Args)
                except Exception:
                    self.Promise.exception(*sys.exc_info())
                else:
                    self.Promise.complete(results)
                    
        p = Promise(data)
        t = QueryTask(self, query, p, args)
        self.QueryQueue << t
        #print ("Task added")
        return p
        
    def wait_queries(self):
        self.QueryQueue.waitUntilEmpty()
    
    def create_namespace(self, name, owner_role=None, description=None):
        """Creates new namespace. Requires client authentication.
        
        Parameters
        ----------
        name : str
            Namespace name
        owner_role : str
            Owner role for the new namespace. The user must be a member of the role.
            Optional. If unspecified, the new namespace will be owned by the user.
        description : str
            New namespace description

        Returns
        -------
        dict 
            New namespace information
        """
        
        url = f"data/create_namespace?name={name}"
        if owner_role:
            url += f"&owner_role={owner_role}"
        if description:
            desc = quote_plus(description)
            url += f"&description={desc}"
        return self.get_json(url)
        
    def get_namespace(self, name):
        """Creates new namespace
        
        Parameters
        ----------
        name : str
            Namespace name

        Returns
        -------
        dict 
            Namespace information
        """
        
        return self.get_json(f"data/namespace?name={name}")
        
    def get_namespaces(self, names):
        """Creates new namespace
        
        Parameters
        ----------
        names : list of str
            Namespace names

        Returns
        -------
        list 
            Namespace information
        """
        
        return self.post_json(f"data/namespaces", names)
        
    def list_namespaces(self, pattern=None):
        """Creates new namespace
        
        Parameters
        ----------
        pattern : str
            Optional fnmatch style pattern to filter namespaces by name

        Returns
        -------
        list 
            List of dictionaries with namespace information
        """
        lst = self.get_json("data/namespaces")
        for item in lst:
            if pattern is None or fnmatch.fnmatch(item["name"], pattern):
                yield item
    
    def login_digest(self, username, password, save_token=False):
        """Performs password-based authentication and stores the authentication token locally.
        
        Parameters
        ----------
        username : str
        password : str
            Password is not sent over the network. It is hashed and then used for digest authentication (:rfc:`2617`).

        Returns
        -------
        str
            username of the authenticated user (same as ``usernme`` argument)
        numeric
            token expiration timestamp
            
        """
        from requests.auth import HTTPDigestAuth
        from metacat.util.authenticators import PasswordAuthenticator
        password_for_digest = PasswordAuthenticator.make_password_for_digest(username, password)
        auth_url = self.AuthURL
        url = "%s/%s?method=digest" % (auth_url, "auth")
        response = requests.get(url, verify=False, auth=HTTPDigestAuth(username, password_for_digest))
        if response.status_code != 200:
            raise ServerError(url, response.status_code, "Authentication failed", response.text)
        #print(response)
        #print(response.headers)
        self.Token = token = SignedToken.decode(response.headers["X-Authentication-Token"])
        if self.TokenLib is not None:
            self.TokenLib[self.ServerURL] = token
        return token["user"], token.Expiration

    def login_ldap(self, username, password):
        """Performs password-based authentication and stores the authentication token locally using LDAP.
        
        Parameters
        ----------
        username : str
        password : str
            Password 

        Returns
        -------
        str
            username of the authenticated user (same as ``usernme`` argument)
        numeric
            token expiration timestamp
            
        """
        auth_url = self.AuthURL
        url = "%s/%s?method=ldap" % (auth_url, "auth")        
        data = b"%s:%s" % (to_bytes(username), to_bytes(password))
        #print("HTTPClient.post_json: url:", url)
        #print("HTTPClient.post_json: headers:", headers)
        response = requests.post(url, verify=False, data = data)
        if response.status_code != 200:
            raise ServerError(url, response.status_code, response.text)
        self.Token = token = SignedToken.decode(response.headers["X-Authentication-Token"])
        if self.TokenLib is not None:
            self.TokenLib[self.ServerURL] = token
        return token["user"], token.Expiration
        
    def login_password(self, username, password):
        user = None
        try:
            user, exp = self.login_ldap(username, password)
        except:
            pass
        if not user:
            user, exp = self.login_digest(username, password)
        return user, exp
            
    def my_x509_dn(self, cert, key=None):
        auth_url = self.AuthURL
        url = f"{auth_url}/mydn"    
        cert_arg = (cert, cert) if key is None else (cert, key)
        response = requests.get(url, verify=False, cert=cert_arg)
        if response.status_code != 200:
            raise ServerError(url, response.status_code, response.text)
        data = json.loads(response.text)
        return data

    def login_x509(self, username, cert, key=None):
        """Performs X.509 authentication and stores the authentication token locally.
        
        Parameters
        ----------
        username : str
        cert : str
            Path to the file with the X.509 certificate or the certificate and private key
        key : str
            Path to the file with the X.509 private key
         

        Returns
        -------
        str
            username of the authenticated user (same as ``usernme`` argument)
        numeric
            token expiration timestamp
            
        """
        auth_url = self.AuthURL
        url = f"{auth_url}/auth?method=x509&username={username}"    
        cert_arg = (cert, cert) if key is None else (cert, key)
        response = requests.get(url, verify=False, cert=cert_arg)
        if response.status_code != 200:
            raise ServerError(url, response.status_code, response.text)
        self.Token = token = SignedToken.decode(response.headers["X-Authentication-Token"])
        if self.TokenLib is not None:
            self.TokenLib[self.ServerURL] = token
        return token["user"], token.Expiration

    def auth_info(self):
        """Returns information about current authentication token.
        
        Returns
        -------
        str
            username of the authenticated user
        numeric
            token expiration timestamp
            
        """
        server_url = self.ServerURL
        token = self.Token
        if not token:
            if self.TokenLib:
                token = self.TokenLib.get(server_url)
        if not token:
            raise AuthenticationError("No token found")
        url = self.AuthURL + "/verify"
        response = requests.get(url, headers={
                "X-Authentication-Token":token.encode()
        })
        #print("web_api.auth_info:", response.status_code, response.text)
        if response.status_code/100 == 2:
            return token["user"], token.Expiration
        else:
            raise ServerError(url, response.status_code, "Verification failed")
