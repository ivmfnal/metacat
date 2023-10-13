import requests, json, fnmatch, sys, os, random, time
from metacat.util import to_str, to_bytes, ObjectSpec, chunked
from metacat.common import SignedToken, TokenLib, TokenAuthClientMixin, AuthenticationError
from urllib.parse import quote_plus, unquote_plus

INVALID_METADATA_ERROR_CODE = 488

def parse_name(name, default_namespace=None):
    words = name.split(":", 1)
    if len(words) < 2:
        assert not not default_namespace, "Null default namespace"
        ns = default_namespace
        name = words[-1]
    else:
        ns, name = words
    return ns, name

undid = parse_name

class MCError(Exception):
    pass

class WebAPIError(MCError):
    
    Headline = "MetaCat API error"
    
    def __init__(self, url, response=None, message=None):
        self.URL = url
        self.StatusCode = response.status_code
        self.Data = None
        self.Body = None
        if message:
            self.Message = self.Body = message
        else:
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
        
class ServerReportedError(WebAPIError):
    Headline = "Server side application error"

    def __init__(self, url, status_code, type, value):
        message = "Type:" + type + f"   Status code:{status_code}"
        if value:
            message += ": " + value
        WebAPIError.__init__(self, url, message=message)
        
class InvalidArgument(WebAPIError):
    Headline = "Invalid argument"
        
class NotFoundError(WebAPIError):
    Headline = "Object not found"

class BadRequestError(WebAPIError):
    Headline = "Invalid request"
            
class AlreadyExistsError(WebAPIError):
    Headline = "Object already exists"
            
class PermissionDeniedError(WebAPIError):
    Headline = "Permission denied"
            
class InvalidMetadataError(WebAPIError):
    Headline = "Invalid metadata"

    def __str__(self):
        msg = ["Invalid metadata"]
        for item in self.json().get("metadata_errors", []):
            item_headline = item["message"]
            index = item.get("index")
            fid = item.get("fid")
            item_id = ""
            if fid is not None:
                item_id = f"file fid={fid}" + item_id
            if index is not None:
                item_id = f"file #{index} " + item_id
            item_id = item_id.strip()
            item_id = f"{item_id}: " if item_id else ""
            msg.append("  " + item_id + item_headline)
            for error in item.get("metadata_errors", []):
                msg.append("    %s: %s" % (error["name"], error["reason"]))
        return "\n".join(msg)

class HTTPClient(object):

    InitialRetry = 1.0
    RetryExponent = 1.5
    DefaultTimeout = 1800.0

    def __init__(self, server_url, token, timeout=None):
        self.ServerURL = server_url
        self.Token = token
        if timeout is not None and timeout <= 0:
            self.Timeout = None         # no timeout
        else:
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
        while not done:
            if method == "get":
                response = requests.get(url, timeout=self.Timeout, **args)
            else:
                response = requests.post(url, timeout=self.Timeout, **args)
            if response.status_code != 503:
                break
            sleep_time = min(random.random() * retry_interval, tend-time.time())
            retry_interval *= self.RetryExponent
            if sleep_time >= 0:
                time.sleep(sleep_time)
            else:
                break       # time out
        return response

    def send_request(self, method, uri_suffix, headers=None, timeout=None, **args):
        self.LastURL = url = "%s/%s" % (self.ServerURL, uri_suffix)
        default_headers = {
            "Accept": "text/plain, application/json, text/json, application/json-seq"
        }
        if self.Token is not None:
            default_headers["X-Authentication-Token"] = self.Token.encode()
        if headers:
            default_headers.update(headers)
        headers = default_headers
        self.LastResponse = response = self.retry_request(method, url, headers=headers, **args)
        self.LastStatusCode = response.status_code
        #print("webapi.send_request: status:", response.status_code)
        if response.status_code == INVALID_METADATA_ERROR_CODE:
            raise InvalidMetadataError(url, response)
        elif response.status_code == 404:
            raise NotFoundError(url, response)
        elif response.status_code == 403:
            raise PermissionDeniedError(url, response)
        elif response.status_code == 409:
            raise AlreadyExistsError(url, response)
        elif response.status_code == 400:
            raise BadRequestError(url, response)
        elif response.status_code/100 != 2:
            raise WebAPIError(url, response)
        return response

    def get_text(self, uri_suffix):
        return self.send_request("get", uri_suffix).text

    def post_text(self, uri_suffix, data):
        return self.send_request("post", uri_suffix, data=data).text

    def unpack_json(self, json_text):
        results = json.loads(json_text)
        if isinstance(results, dict):
            if "results" in results:
                results = results["results"]
            elif "error" in results:
                raise ServerReportedError(self.LastURL, self.LastStatusCode, results["error"]["type"], results["error"].get("value", ""))
        return results

    RS = b'\x1E'
    def unpack_json_seq(self, response):
        for line in response.iter_lines():
            if line:    line = line.strip()
            while line and line.startswith(self.RS):
                line = line[1:]
            if line:
                #print(f"stream line:[{line}]")
                obj = self.unpack_json(line)
                yield obj

    def unpack_json_data(self, response):
        response_content_type = response.headers.get("content-type")
        if "application/json-seq" in response_content_type:
            return self.unpack_json_seq(response)
        else:
            return self.unpack_json(response.text)

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

        response = self.retry_request("get", url, headers=headers, stream=True)
        if response.status_code == INVALID_METADATA_ERROR_CODE:
            raise InvalidMetadataError(url, response)
        if response.status_code == 404:
            raise NotFoundError(url, response)
        elif response.status_code != 200:
            raise WebAPIError(url, response)
        
        if response.headers.get("Content-Type") != "application/json-seq":
            raise WebAPIError(url, response)

        for line in response.iter_lines():
            if line:    line = line.strip()
            while line.startswith(b'\x1E'):
                line = line[1:]
            if line:
                #print(f"stream line:[{line}]")
                obj = json.loads(line)
                yield obj

class MetaCatClient(HTTPClient, TokenAuthClientMixin):
    
    Version = "1.0"
    
    def __init__(self, server_url=None, auth_server_url=None, max_concurrent_queries = 5,
                token = None, token_file = None, token_library = None, timeout = None):    

        """Initializes the MetaCatClient object

        Arguments
        ---------
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
        timeout : int or float
            Request timeout in seconds. Default: None - use default timeout, which is 300 seconds
        """

        server_url = server_url or os.environ.get("METACAT_SERVER_URL")
        if not server_url:
            raise RuntimeError("MetaCat server URL unspecified")

        auth_server_url = auth_server_url or os.environ.get("METACAT_AUTH_SERVER_URL")

        TokenAuthClientMixin.__init__(self, server_url, auth_server_url, token=token, token_file=token_file, token_library=token_library)
        HTTPClient.__init__(self, server_url, token=self.token(), timeout=timeout)
        self.MaxConcurrent = max_concurrent_queries
        self.AsyncQueue = None
        
    def sanitize(self, *words, **kw):
        for w in words:
            if "'" in words:
                raise InvalidArgument("", "Invalid value: %s" % (w,))
        for name, value in kw.items():
            if "'" in value:
                raise InvalidArgument("", "Invalid value for %s: %s" % (name, value))
            if "'" in name:
                raise InvalidArgument("", "Invalid name for: %s" % (name,))

    @property
    def async_queue(self):
        if self.AsyncQueue is None:
            try:    
                from pythreader import TaskQueue
            except ModuleNotFoundError:
                raise ModuleNotFoundError("pythreader module required for asynchronous queries. Use: pip install 'pythreader>=2.7.0'")
            self.AsyncQueue = TaskQueue(self.MaxConcurrent)
        return self.AsyncQueue

    def resfresh_token(self):
        if self.TokenFile:
             token = open(self.TokenFile, "rb").read()
             self.Token = SignedToken.decode(token)
        return self.Token
        
    def simulate_503(self):
        return self.get_text("data/simulate_503")

    def get_version(self):
        """Returns server version as text
        """
        return self.get_text("data/version")

    def list_datasets(self, namespace_pattern=None, name_pattern=None, with_counts=False):
        """Gets the list of datasets with namespace/name matching the templates. The templates are
        Python ``fnmatch`` module style templates where ``'*'`` matches any substring and ``'?'`` matches a single character.

        Arguments
        ---------
        namespace_pattern : str
        name_pattern : str
        with_file_counts : boolean
            controls whether the results should include file counts or dataset names only

        Yields
        ------
        generator
            yields dictionaries like {"namespace":..., "name":..., "file_count":...}
        """        
        #url = "data/datasets?with_file_counts=%s" % ("yes" if with_file_counts else "no")
        url = "data/datasets?with_counts=no"
        lst = self.get_json(url)
        promises = []        # [(dataset_dict, promise)]
        for item in lst:
            namespace, name = item["namespace"], item["name"]
            if namespace_pattern is not None and not fnmatch.fnmatch(namespace, namespace_pattern):
                continue
            if name_pattern is not None and not fnmatch.fnmatch(name, name_pattern):
                continue
            if not with_counts:
                yield item
            else:
                # fetch counts asynchronously
                did = namespace + ":" + name
                promises.append(self.async_queue.add(self.get_dataset_counts, did, promise_data = item).promise)

        for promise in promises:
            counts = promise.wait()
            #print("promise: counts:", counts)
            item = promise.Data
            item.update(counts)
            yield item

    def get_dataset_counts(self, did=None, namespace=None, name=None):
        """Gets single dataset files, subsets, supersets, etc. counts
        
        Arguments
        ---------
        did : str - "namespace:name"
        namespace : str
        name : str
        
        Returns
        -------
        dict
            dataset counts or None if the dataset was not found
        """        

        did = ObjectSpec(did, namespace=namespace, name=name).did()
        try:
            out = self.get_json(f"data/dataset_counts?dataset={did}")
            #print("get_dataset_counts", did, " out=", out)
            return out
        except NotFoundError:
            #print("get_dataset_counts: None")
            return None

    def get_dataset(self, did=None, namespace=None, name=None, exact_file_count=False):
        """Gets single dataset
        
        Arguments
        ---------
        did : str - "namespace:name"
        namespace : str
        name : str
        
        Returns
        -------
        dict
            dataset attributes or None if the dataset was not found
        """        

        spec = ObjectSpec(did, namespace=namespace, name=name).did()

        try:
            url = f"data/dataset?dataset={spec}"
            if exact_file_count:
                url += "&exact_file_count=yes"
            return self.get_json(url)
        except NotFoundError:
            return None

    def get_dataset_files(self, did, namespace=None, name=None, with_metadata=False, include_retired_files=False):
        """Gets single dataset
        
        Arguments
        ---------
        did : str - "namespace:name"
        namespace : str
        name : str
        
        Returns
        -------
        generator
            generates sequence of dictionaries, one dictionary per file
        """        
        
        if namespace is not None:
            did = namespace + ':' + name
        try:
            with_metadata = "yes" if with_metadata else "no"
            include_retired_files = "yes" if include_retired_files else "no"
            url = f"data/dataset_files?dataset={did}&with_metadata={with_metadata}&include_retired_files={include_retired_files}"
            return self.get_json_stream(url)
        except NotFoundError:
            return None
        
        
    def create_dataset(self, did, frozen=False, monotonic=False, metadata=None, metadata_requirements=None, 
            files_query=None, subsets_query=None,
            description=""):

        """Creates new dataset. Requires client authentication.

        Arguments
        ---------
        did : str
            "namespace:name"
        frozen : bool
        monotonic : bool
        metadata : dict
            Dataset metadata
        metadata_requirements : dict
            Metadata requirements for files in the dataset
        file_query : str
            Run MQL file query and add resulting files to the new dataset
        dataset_query : str
            Run MQL dataset query and add resulting datasets to the new dataset as subsets
        description : str

        Returns
        -------
        dict
            created dataset attributes
        """

        namespace, name = did.split(":",1)
        #self.sanitize(namespace=namespace, name=name)
        params = {
            "namespace":    namespace,
            "name":         name,
            "frozen":       frozen,
            "monotonic":    monotonic,
            "metadata":     metadata or {},
            "metadata_requirements":    metadata_requirements or None,
            "description":  description or "",
            "files_query":  files_query or None,
            "subsets_query":  subsets_query or None
        }
        url = f"data/create_dataset"
        return self.post_json(url, params)
        
    def add_child_dataset(self, parent_spec, child_spec):
        """Adds a child dataset to a dataset.
        
        Arguments
        ---------
        parent_spec : str
            Parent namespace, name ("namespace:name")
        child_spec : str
            Child namespace, name ("namespace:name")
        """
        url = f"data/add_child_dataset?parent={parent_spec}&child={child_spec}"
        return self.get_text(url)
        
    def add_files(self, dataset, file_list=None, namespace=None, query=None):
        """Add existing files to an existing dataset. Requires client authentication.
        
        Arguments
        ---------
        dataset : str
            "namespace:name" or "name", if namespace argument is given
        query : str
            MQL query to run and add files matching the query
        file_list : list
            List of dictionaries, one dictionary per file. Each dictionary must contain either a file id
        
            .. code-block:: python
        
                    { "fid": "abcd12345" }

            or namespace/name:
        
            .. code-block:: python

                    { "name": "filename.data", "namespace": "my_namespace" }

            or DID:
        
            .. code-block:: python

                    { "did": "my_namespace:filename.data" }
        
        namespace : str, optional
            Default namespace. If a ``file_list`` item is specified with a name without a namespace, the ``default namespace``
            will be used.

        Returns
        -------
        int
            number of files added to the dataset
        
        Notes
        -----
        Either ``file_list`` or ``query`` must be specified, but not both
        """
            
        default_namespace = namespace
        if ':' not in dataset:
            if default_namespace is None:
                raise ValueError("Namespace not specified for the target dataset")
            dataset = f"{default_namespace}:{dataset}"

        url = f"data/add_files?dataset={dataset}"
        
        if (file_list is None) == (query is None):
            raise ValueError("Either file_list or query must be specified, but not both")
        
        params = {
            "namespace": namespace,
        }
        if file_list is not None:
            lst = []
            for f in file_list:
                spec = ObjectSpec.from_dict(f, default_namespace)
                spec.validate()
                lst.append(spec.as_dict())
            params["file_list"] = lst
        elif query:
            params["query"] = query
        else:
            raise ValueError("Either file_list or query must be specified, but not both")
        
        out = self.post_json(url, params)
        return out["files_added"]

    def remove_files(self, dataset, file_list=None, namespace=None, query=None):
        """Remove files from a dataset. Requires client authentication.
        
        Arguments
        ---------
        dataset : str
            "namespace:name" or "name", if namespace argument is given
        query : str
            MQL query to run and add files matching the query
        file_list : list
            List of dictionaries, one dictionary per file. Each dictionary must contain either a file id
        
            .. code-block:: python
        
                    { "fid": "abcd12345" }

            or namespace/name:
        
            .. code-block:: python

                    { "name": "filename.data", "namespace": "my_namespace" }

            or DID:
        
            .. code-block:: python

                    { "did": "my_namespace:filename.data" }
        
        namespace : str, optional
            Default namespace. If a ``file_list`` item is specified with a name without a namespace, the ``default namespace``
            will be used.

        Returns
        -------
        int
            actual number of files removed from the dataset
        
        Notes
        -----
        Either ``file_list`` or ``query`` must be specified, but not both
        """
            
        default_namespace = namespace
        dataset_spec = ObjectSpec(dataset, namespace=default_namespace)
        
        if (file_list is None) == (query is None):
            raise ValueError("Either file_list or query must be specified, but not both")
        
        params = {
            "dataset_namespace": dataset_spec.Namespace,
            "dataset_name": dataset_spec.Name,
            "namespace": namespace
        }
        if file_list is not None:
            lst = []
            for f in file_list:
                spec = ObjectSpec.from_dict(f, default_namespace)
                spec.validate()
                lst.append(spec.as_dict())
            params["file_list"] = lst
        elif query:
            params["query"] = query
        else:
            raise ValueError("Either file_list or query must be specified, but not both")
        
        out = self.post_json("data/remove_files", params)
        return out["files_removed"]

    def remove_dataset(self, dataset):
        """Remove a dataset. Requires client authentication.
        
        Arguments
        ---------
        dataset : str
            "namespace:name"
        """
        return self.get_text(f"data/remove_dataset/{dataset}")


    def declare_file(self, did=None, namespace=None, name=None, auto_name=None,
                     dataset_did=None, dataset_namespace=None,
                     dataset_name=None, size=0, metadata={}, fid=None, parents=[], checksums={},
                     dry_run=False):
        """Declare new file and add it to the dataset. Requires client authentication.
        
        Arguments
        ---------
        did : str
            file "namespace:name"
        namespace : str
            file namespace
        name : str
            file name
        auto_name : str
            pattern to use for file name auto generation, default None - do not auto-generate file name
        dataset_did : str
            dataset "namespace:name"
        dataset_namespace : str
            dataset namespace
        dataset_name : str
            dataset name
        size : int
            file size in bytes, default 0
        metadata : dict
            file metadata, default empty dictionary
        fid : str
            file id, default None - to be auto-generated
        checksums : dict
            dictionary with checksum values by the checksum type: {"type":"value", ...}
        parents : list of dicts
            each dict represents one parent file. The dict must contain one of the the following
                - "fid" - parent file id
                - "namespace" and "name" - parent file namespace and name
                - "did" - parent file DID ("<namespace>:<name>")
        dry_run : boolean
            If true, run all the necessary checks but stop short of actual file declaraion or adding to a dataset. 
            If not all checks are successful, generate eirher InvalidMetadataError or WebApiError.
            Default: False = do declare

        Returns
        -------
        dict
            dictionary with file name, namespace and file id. Names and file ids will be auto-generated as necessary.

        Notes
        -----
        At least one of the following must be specified for the file:
            - did
            - namespace and either name or auto_name

        At least one of the following must be specified for the dataset:
            - dataset_did
            - dataset_namespace and dataset_name

        Auto-name pattern can be any string with the following substrings, which will be replaced with appropriate values to generate the file name:

            - $clock - current interger timestamp in milliseconds
            - $clock3 - last 3 digits of $clock - milliseconds only
            - $clock6 - last 6 digits of $clock
            - $clock9 - last 9 digits of $clock
            - $uuid - random UUID in hexadecimal representation, 32 hex digits
            - $uuid16 - 16 hex digits from random UUID hexadecimal representation
            - $uuid8 - 8 hex digits from random UUID hexadecimal representation
            - $fid - file id
        """

        if not did:
            if not namespace:
                raise ValueError("Unspecified file namespace")
            if not name and not auto_name:
                raise ValueError("Unspecified file name")
        else:
            namespace, name = undid(did)
        if not (dataset_namespace and dataset_name) and not dataset_did:
            raise ValueError("Either dataset_did or dataset_namespace and dataset_name must be provided")
        if dataset_did is None:
            dataset_did = f"{dataset_namespace}:{dataset_name}" 
        info = dict(
            namespace = namespace, 
            name = name,
            size = size,
            checksums = checksums,
            fid = fid,
            parents = parents,
            metadata = metadata
        )
        if not name and auto_name:
            info["auto_name"] = auto_name
        return self.declare_files(dataset_did, [info])[0]

    def declare_files(self, dataset, files, namespace=None, dry_run=False):
        """Declare new files and add them to an existing dataset. Requires client authentication.
        
        Arguments
        ---------
        dataset : str
            "namespace:name"
        files : list or dict
            List of dictionaries, one dictionary per a file to be declared. See Notes below for the expected contents of each
            dictionary.
            For convenience, if declaring single file, the argument can be the single file dictionary instead of a list.
        namespace: str, optional
            Default namespace for files to be declared
        dry_run : boolean
            If true, run all the necessary checks but stop short of actual file declaraion or adding to a dataset. 
            If not all checks are successful, generate eirher InvalidMetadataError or WebApiError.
            Default: False = do declare

        Returns
        -------
        list
            list of dictionaries, one dictionary per file with file ids: { "fid": "..." }
        
        Notes
        -----
        Each file to be declared must be represented with a dictionary. The dictionary must contain one of:
        
        "did" - string in the format "<namespace>:<name>"

        "name" - file name and optionaly "namespace". If namespace is not present, the ``namespace`` argument will be used
                 as the default namespace

        "auto_name" - pattern to auto-generate file name
    
        .. code-block:: python
    
            { 
                "namespace": "namespace",           # optional, namespace can be specified for each file explicitly or implicitly using the namespace=... argument
                "name": "filename",                 # optional,
                "did": "namespace:filename",        # optional, convenience for Rucio users
                                                    # either "did" or "name", "namespace" must be present
                "size": ...,                        # required, integer number of bytes
                "metadata": {...},                  # optional, file metadata, a dictionary with arbitrary JSON'able contents
                "fid":  "...",                      # optional, file id. Will be auto-generated if unspecified.
                                                    # if specified, must be unique
                "parents": [...],                   # optional, list of dicts, one dict per parent. See below.
                "checksums": {                      # optional, checksums dictionary
                    "method": "value",...
                },
                "auto_name": "..."                  # optional, pattern to auto-generate file name if name is not specified or null
            },...
    
        Parents are specified with dictionaries, one dictionary per file. Each dictionary specifies the parent file in one of three ways:

            - "did": "<namespace>:<name>"
            - "namespace":"...", "name":"..."
            - "fid": "<file id>"
    
        DEPRECATED: if the parent is specified with a string instead of a dictionary, it is interpreferd as the parent file id.
        """        
        
        default_namespace = namespace
        if isinstance(files, dict):
            files = [files]                     # convenience

        lst = []

        for i, item in enumerate(files):
            f = item.copy()
            namespace = f.get("namespace", default_namespace)
            if "did" in f:
                if "name" in f or "namespace" in f:
                    raise ValueError(f"Both DID and namespace/name specified for {did}")
                did = f.pop("did")
                namespace, name = parse_name(did, default_namespace)
                f["name"] = name
            f["namespace"] = namespace
            size = f.get("size")
            if not isinstance(size, int) or size < 0:
                raise ValueError(f"File size is unspecified or invalid for file #{i} in the list")

            meta = item.get("metadata", {})
            for k in meta.keys():
                if '.' not in k:
                    raise ValueError(f'Invalid metadata key "{k}" for file #{i} in the list: metadata key must contain dot (.)')

            f["metadata"] = meta
            lst.append(f)

        url = f"data/declare_files?dataset={dataset}"
        if dry_run: url += "&dry_run=yes"
        #print("webapi: declare_files: post...")
        out = self.post_json(url, lst)
        #print("webapi: declare_files: out:", out)
        return out
        
    def move_files(self, namespace, file_list=None, query=None):
        """
        Arguments
        ---------
        namespace : str
            namespace to move files to
        query : str
            MQL query to run and add files matching the query
        file_list : list
            List of dictionaries, one dictionary per file. Each dictionary must contain either a file id
        
            .. code-block:: python
        
                    { "fid": "abcd12345" }

            or namespace/name:
        
            .. code-block:: python

                    { "name": "filename.data", "namespace": "my_namespace" }

            or DID:
        
            .. code-block:: python

                    { "did": "my_namespace:filename.data" }
        Returns
        -------
        tuple
            number of files moved, list of errors, if any
        """    
        params = {
            "namespace": namespace,
        }
        if file_list is not None:
            lst = []
            for f in file_list:
                spec = ObjectSpec.from_dict(f)
                spec.validate()
                lst.append(spec.as_dict())
            params["files"] = lst
        elif query:
            params["query"] = query
        else:
            raise ValueError("Either file_list or query must be specified, but not both")

        url = "data/move_files"
        out = self.post_json(url, params)
        errors = out.get("errors", [])
        return out["files_moved"], errors, out.get("nerrors", len(errors))

    def update_file(self, did=None, namespace=None, name=None, fid=None, replace=False,
                size=None, checksums=None, parents=None, children=None, metadata=None
        ):
        """
        Arguments
        ---------
        did : str
            file "namespace:name"
        fid : str
            file id
        namespace : str
            file namespace
        name : str
            file name
        replace : bool
            If True, the specified attribute values will be replaced with new values. 
            Otherwise added (for parents and children) and updated (for checksums and metadata)
        size : int >= 0
            file size, optional
        checksums : dict
            checksum values, optional
        parents : list
            list of parent file ids, optional
        children : list
            list of child file ids, optional
        metadata : dict
            dictionary with metadata to update or replace, optional

        Returns
        -------
        dict
            Dictionary with updated file information
        """

        data = {"mode":"replace" if replace else "add-update"}

        if fid:
            data["fid"] = fid
        else:
            if did:
                namespace, name = did.split(':', 1)
            assert namespace and name
            data["namespace"] = namespace
            data["name"] = name
            
        if size is not None:
            assert isinstance(size, int) and size >= 0
            data["size"] = size
            
        if checksums is not None:
            assert isinstance(checksums, dict)
            data["checksums"] = checksums
            
        if parents is not None:
            assert isinstance(parents, list)
            data["parents"] = [ObjectSpec(p).as_dict() for p in parents]
            
        if children is not None:
            assert isinstance(children, list)
            data["children"] = [ObjectSpec(c).as_dict() for c in children]
            
        if metadata is not None:
            assert isinstance(metadata, dict)
            data["metadata"] = metadata

        return self.post_json("data/update_file", data)

    def update_file_meta(self, metadata, files=None, names=None, fids=None, namespace=None, dids=None, mode="update"):
        """Updates metadata for existing files. Requires client authentication.
        
        **DEPRECATED** *update_file() should be used instead*
        
        Arguments
        ---------
        metadata : dict
            see Notes
        files : list of dicts
            Each dict specifies a file. See Notes
        names : list of strings
            List of file names. Requires namespace to be specified
        dids : list of strings
            List of DIDs ("namespace:name") strings
        fids : list of strings
            List of file ids. The list of files can be specified with ``fids`` or with ``names`` argument, but not
            both.
        namespace : string
            Default namespace
        mode : str
            Either ``"update"`` (default) or ``"replace"``. If mode is ``"update"``, existing metadata will be updated with
            values in ``metadata``. If ``"replace"``, then new values will replace existing metadata. Also, see notes below.
        
        Returns
        -------
        list
            list of dictionaries, one dictionary per file with file ids: { "fid": "..." }
        
        
        Notes
        -----
        This method can be be used to apply common metadata changes to a list of files. This method **can not** be used to update
        file provenance information.
        
        The``metadata`` argument is used to specify the common changes to the metadata to apply to multiple files.
        The ``metadata`` dictionary will be used to either update existing metadata of listed files (if ``mode="update"``) or
        replace it (if ``mode="replace"``).
        
        Files to update have to be specified in one of the following ways:
        
            - files = [list of dicts] - each dict must be in one of the following formats:
        
                - {"fid":"<file id>"} 
                - {"namespace":"<file namespace>", "name":"<file name>"} - namespace is optional. Default: the value of the "namespace" method argument
                - {"did":"<file namespace>:<file name>"} 
        
            - dids = [list of file DIDs]
            - names = [list of file names] - "namespace" argument method must be used to specify the common namespace
            - fids = [list of file ids]
        """
        
        if names and not namespace:
            raise ValueError("Namespace must be specified with names argument")

        def combined():
            for name in (names or []):
                yield ObjectSpec(namespace, name).as_dict()
            for did in (dids or []):
                spec = ObjectSpec(did)
                spec.validate()             # will raise ValueError
                yield spec.as_dict()
            for fid in (fids or []):
                yield ObjectSpec(fid=fid).as_dict()
            for item in (files or []):
                spec = ObjectSpec.from_dict(item)
                spec.validate()             # will raise ValueError
                yield spec.as_dict()

        url = f"data/update_file_meta"
        out = []
        for chunk in chunked(combined(), 1000):
            data = {
                "metadata":metadata,
                "files":chunk,
                "mode":mode
            }
            out.extend(self.post_json(url, data))

        return out

    def delete_file(self, did=None, namespace=None, name=None, fid=None):
        """Delete an existing file. The file will be removed from all datasets and the database and its name and file id can be reused.
        
        Arguments
        ---------
        did : str
            file "namespace:name"
        fid : str
            file id
        namespace : str
            file namespace
        name : str
            file name
        retire : bool
            whether the file should be retired
        """
        data = {}
        if fid:
            data["fid"] = fid
        else:
            if did:
                namespace, name = did.split(':', 1)
            assert namespace and name
            data["namespace"] = namespace
            data["name"] = name
        #print("API.delete: sending:", data)
        return self.post_json("data/delete_file", data)

    def retire_file(self, did=None, namespace=None, name=None, fid=None, retire=True):
        """Modify retired status of the file. Retured file remains in the database, "occupies" the name in the namespace, but
        id not visible to normal queries. Retired file can be brought back to normal using this method too.
        
        If you need to completely remove the file, use `delete_file` method.
        
        Arguments
        ---------
        did : str
            file "namespace:name"
        fid : str
            file id
        namespace : str
            file namespace
        name : str
            file name
        retire : bool
            whether the file should be retired
        
        Returns
        -------
        dict
            Dictionary with updated file information
        """
        data = {
            "retire":   retire
        }
        if fid:
            data["fid"] = fid
        else:
            if did:
                namespace, name = did.split(':', 1)
            assert namespace and name
            data["namespace"] = namespace
            data["name"] = name
        #print("API.retire: sending:", data)
        return self.post_json("data/retire_file", data)

    def update_dataset(self, dataset, metadata=None, mode="update", frozen=None, monotonic=None, description=None):   
        """Update dataset. Requires client authentication.
        
        Arguments
        ---------
        dataset : str
           "namespace:name"
        metadata : dict or None
            New metadata values, or, if None, leave the metadata unchanged
        mode: str
            Either ``"update"`` or ``"replace"``. If ``"update"``, metadata will be updated with new values. If ``"replace"``,
            metadata will be replaced with new values.
            If ``metadata`` is None, ``mode`` is ignored
        frozen: boolean or None
            if boolean, new value for the flag. If None, leave it unchanged
        monotonic: boolean or None
            if boolean, new value for the flag. If None, leave it unchanged
        description: str or None
            if str, new dataset description. If None, leave the description unchanged

        Returns
        -------
        dict
            dictionary with new dataset information
        """
        request_data = {}
        if metadata is not None:
            request_data["mode"] = mode
            request_data["metadata"] = metadata
        if frozen is not None:  request_data["frozen"] = frozen
        if monotonic is not None:  request_data["monotonic"] = monotonic
        if description is not None: request_data["description"] = description
        url = f"data/update_dataset?dataset={dataset}"
        out = self.post_json(url, request_data)
        return out
        
    def get_files(self, lookup_list, with_metadata = True, with_provenance=True):
        """Get many file records
        
        Arguments
        ---------
        lookup_list : list
            List of dictionaries, one dictionary per file. Each dictionary must have either
                "did":"namespace:name", or
                "namespace":"..." and "name":"..." or
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
        
        new_list = []
        for item in lookup_list:
            if "fid" in item or "namespace" in item and "name" in item:
                pass
            elif "did" in item:
                did = item["did"]
                try:
                    namespace, name = did.split(':', 1)
                except ValueError:
                    raise ValueError("Invalid DID format: " + did)
                item = {"namespace":namespace, "name":name}
            else:
                raise ValueError("Invalid file specifification: " + str(item))
            new_list.append(item)

        url = "data/files?with_metadata=%s&with_provenance=%s" % (with_metadata, with_provenance)

        return self.post_json(url, new_list) 
        
    def get_file(self, name=None, namespace=None, fid=None, did=None, with_metadata = True, with_provenance=True, with_datasets=False):
        """Get one file record
        
        Arguments
        ---------
        fid : str, optional
            File id
        name : str, optional
        namespace : str, optional
            name and namespace must be specified together
        did : str, optional
            "nemaspace:name"
        with_metadata : boolean
            whether to include file metadata
        with_provenance : boolean
            whether to include parents and children list
        with_datasets : boolean
            whether to include the list of datasets the file is in

        Returns
        -------
        dict
            dictionary with file information or None if the file was not found

            .. code-block:: python

                {       
                    "name": "namespace:filename",       # file name, namespace
                    "fid":  "...",                      # files id
                    "creator":  "...",                  # username of the file creator
                    "created_timestamp":   ...,         # numeric UNIX timestamp
                    "size": ...,                        # file size in bytes
                    "checksums": { ... },               # file checksums

                    # included if with_provenance=True
                    "parents":  ["fid",...],            # list of ids for the file parent files
                    "children": ["fid",...],            # list of ids for the file child files

                    # included if with_metadata=True
                    "metadata": { ... },                # file metadata

                    # included if with_datasets=True
                    "datasets": [
                        {"namespace":"...", "name":"..."}, ...
                    ]
                }

        Notes
        -----
        Retrieving file provenance and metadata takes slightly longer time
        """        
        assert (fid is not None) or (did is not None) or (name is not None and namespace is not None), \
            "Either DID or file id or namespace and name must be specified"
        with_meta = "yes" if with_metadata else "no"
        with_rels = "yes" if with_provenance else "no"
        with_datasets = "yes" if with_datasets else "no"
        url = f"data/file?with_metadata={with_meta}&with_provenance={with_rels}&with_datasets={with_datasets}"
        if did:
            namespace, name = parse_name(did, None)
            assert namespace is not None, f"Invalid DID format: {did}"
        if name:
            url += f"&name={name}&namespace={namespace}"
        else:
            url += f"&fid={fid}"        
            
        try: 
            return self.get_json(url)
        except NotFoundError:
            return None

    def query(self, query, namespace=None, with_metadata=False, with_provenance=False, save_as=None, add_to=None,
                        include_retired_files=False, summary=None):
        """Run file query. Requires client authentication if save_as or add_to are used.
        
        Arguments
        ---------
        query : str
            Query in MQL
        namespace : str
            default namespace for the query
        include_retired_files:
            boolean, whether to include retired files into the query results, default=False
        with_metadata : boolean
            whether to return file metadata
        with_provenance : boolean
            whether to return parents and children list
        save_as : str
            namespace:name for a new dataset to create and add found files to
        add_to : str
            namespace:name for an existing dataset to add found files to
        summary : str or None
            "count" - return file count only as int
            "keys" - return list of all top level metadata keys for the selected files
            ``summary`` can not be used together with ``save_as`` or ``add_to``

        Returns
        -------
        list of dicts
            dictionary with file information. Each file will be represented with a dictionary in this list.
        
        Notes
        -----
        Retrieving file provenance and metadata takes slightly longer time
        """
        
        assert not (summary is not None and (add_to or save_as)), "Summary can not be used together with add_to or save_as"
        assert summary in ("count", "keys", None)
        
        if summary:
            url = f"data/query?summary={summary}"
            if namespace:
                url += f"&namespace={namespace}"
            if include_retired_files:
                url += "&include_retired_files=yes"
        else:
            url = "data/query?with_meta=%s&with_provenance=%s" % ("yes" if with_metadata else "no","yes" if with_provenance else "no")
            if namespace:
                url += f"&namespace={namespace}"
            if save_as:
                url += f"&save_as={save_as}"
            if add_to:
                url += f"&add_to={add_to}"
            if include_retired_files:
                url += "&include_retired_files=yes"
        #print("url:", url)
        results = self.post_json(url, query)
        return results

    def async_query(self, query, data=None, **args):
        """Run the query asynchronously. Requires client authentication if save_as or add_to are used.
        
        Arguments
        ---------
        query : str
            Query in MQL
        data : anything
            Arbitrary data associated with this query
        args : 
            Same keyword arguments as for the query() method
        
        Returns
        -------
        Promise
            ``pythreader`` Promise object associated with this query. The promise object will have Data attribute containig the object passed as the ``data``
            argument to the ``async_query`` call. 
        
            See notes below for more on how to use this method.
        """
        
        return self.async_queue.add(self.query, query, promise_data=data, **args).promise

    def wait_queries(self):
        """
        Wait for all issued asynchronous queries to complete
        """
        self.async_queue.waitUntilEmpty()


    def search_named_queries(self, query):
        """
        Run MQL query for named queries
        
        Arguments
        ---------
        query : str
            Query in MQL

        Returns
        -------
        list of dicts
            The list contains one dictionary per matching named query with the query information.
        """
        url = "data/search_queries"
        results = self.post_json(url, query)
        return results
    
    def create_namespace(self, name, owner_role=None, description=None):
        """Creates new namespace. Requires client authentication.
        
        Arguments
        ---------
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
        """Get information about a snamespace
        
        Arguments
        ---------
        name : str
            Namespace name

        Returns
        -------
        dict 
            Namespace information or None if the namespace was not found
        """
        
        try: 
            return self.get_json(f"data/namespace?name={name}")
        except NotFoundError:
            return None
        
    def get_namespaces(self, names):
        """Get information for multiple namespaces
        
        Arguments
        ---------
        names : list of str
            Namespace names

        Returns
        -------
        list 
            Namespace information
        """
        
        return self.post_json(f"data/namespaces", names)
        
    def list_namespaces(self, pattern=None, owner_user=None, owner_role=None, directly=False):
        """List namespaces
        
        Arguments
        ---------
        pattern : str
            Optional fnmatch style pattern to filter namespaces by name
        owner_user : str
            Optional, return only namespaces owned by the specified user
        directly : boolean
            If False and owner_user is specified, return also namespaces owned by all roles the user is in
            Ignored if owner_user is not specified
        owner_role : str
            Optional, return only namespaces owned by the specified role.
            Ignored if owner_user is also specified

        Returns
        -------
        list 
            List of dictionaries with namespace information sorted by the namespace name
        """
        url = "data/namespaces"
        args = ""
        if owner_user:
            args += f"owner_user={owner_user}"
            if directly:
                args += "&directly=yes"
        if owner_role:
            if args: args += "&"            # low level API on the server side will ignore owner_role if owner_user is present, but pass both anyway
            args += f"owner_role={owner_role}"
        if args:
            args = '?' + args
        lst = self.get_json("data/namespaces" + args)
        for item in lst:
            if pattern is None or fnmatch.fnmatch(item["name"], pattern):
                yield item
                
    #
    # Categiries
    #
    def list_categories(self, root=None):
        """List namespaces

        Arguments
        ---------
        root : str
            Optional, if present, list only categories under the root

        Returns
        -------
        list 
            List of dictionaries with category information sorted by category path
        """
        lst = self.get_json("data/categories")
        if root:
            if not root.endswith('.'):
                root += '.'
            lst = [cat for cat in lst if cat["path"].startswith(root)]
        return sorted(lst, key=lambda c: c["path"])

    def get_category(self, path):
        """Get category information
        
        Returns
        -------
        dict 
            A dictionary with category information or None if not found
        """
        out = self.get_json(f"data/category/{path}")
        return out
        
    #
    # Named queries
    #
    def get_named_query(self, namespace, name):
        """Get named query
        
        Arguments
        ---------
        namespace : str
        name : str
        
        Returns
        -------
        dict or None
            A dictionary with information about the named query or None if the named query does not exist.
        """
        try:    data = self.get_json(f"data/named_query?namespace={namespace}&name={name}")
        except NotFoundError:
            return None
        return data

    def list_named_queries(self, namespace=None):
        """Get multiple named queries
        
        Arguments
        ---------
        namespace : str
            optional, if specified the list will include all named queries in the namespace. Orherwise all named queries will be returned
        
        Returns
        -------
        list
            List of dictionaries with information about the named queries.
        """
        url = "data/named_queries"
        if namespace is not None:
            url += f"?namespace={namespace}"
        return self.get_json(url)

    def create_named_query(self, namespace, name, source, parameters=[], update=False):
        data = dict(namespace=namespace, name=name, source=source, parameters=parameters)
        url = "data/create_named_query"
        if update: url += "?update=yes"
        return self.post_json(url, data)
