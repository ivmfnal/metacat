import requests, json, fnmatch, sys, os, random, time
from metacat.util import to_str, to_bytes, ObjectSpec
from metacat.auth import SignedToken, TokenLib, AuthenticationError
from urllib.parse import quote_plus, unquote_plus
from metacat.auth import TokenAuthClientMixin

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

class ServerError(Exception):
    
    def __init__(self, url, status_code, message, body=""):
        self.URL = url
        self.StatusCode = status_code
        self.Message = message
        self.Body = to_str(body)
        
    def __str__(self):
        #msg = f"MetaCatServer error:\n  URL: {self.URL}\n  HTTP status code: {self.StatusCode}\n  {self.Message}"
        msg = self.Message
        if self.Body:
            msg += "\nMessage from the server:\n"+self.Body+"\n"
        return msg
        
class WebAPIError(ServerError):
    
    def __init__(self, url, status_code, body):
        ServerError.__init__(self, url, status_code, "", body)
    
    def json(self):
        #print("WebAPIError.json: body:", self.Body)
        return json.loads(self.Body)
        
class NotFoundError(WebAPIError):
    pass
            
class InvalidMetadataError(WebAPIError):

    def __str__(self):
        msg = ["Invalid metadata error"]
        for item in self.json():
            item_headline = item["message"]
            index = item.get("index")
            fid = item.get("fid")
            item_id = ""
            if fid is not None:
                item_id = f"fid={fid}" + item_id
            if index is not None:
                item_id = f"[{index}] " + item_id
            item_id = item_id.strip()
            item_id = f"{item_id}: " if item_id else ""
            msg.append("  " + item_id + item_headline)
            for error in item.get("metadata_errors", []):
                msg.append("    %s: %s" % (error["name"], error["reason"]))
        return "\n".join(msg)

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
        if headers:
            headers = headers.copy()
        if self.Token is not None:
            headers = headers or {}
            headers["X-Authentication-Token"] = self.Token.encode()
        self.LastResponse = response = self.retry_request(method, url, headers=headers, **args)
        #print(response, response.text)
        self.LastStatusCode = response.status_code
        if response.status_code == INVALID_METADATA_ERROR_CODE:
            raise InvalidMetadataError(url, response.status_code, response.text)
        if response.status_code == 404:
            raise NotFoundError(url, response.status_code, response.text)
        elif response.status_code != 200:
            raise WebAPIError(url, response.status_code, response.text)
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
                message =  "Server side error: " + results["error"]["type"] + ": "
                message += results["error"]["value"]
                raise ServerError(self.LastURL, self.LastStatusCode, message)
        return results
                
    def get_json(self, uri_suffix):
        return self.unpack_json(self.send_request("get", uri_suffix).text)

    def post_json(self, uri_suffix, data):
        if not isinstance(data, (str, bytes)):
            data = json.dumps(data)
        return self.unpack_json(self.send_request("post", uri_suffix, data=data).text)

class MetaCatClient(HTTPClient, TokenAuthClientMixin):
    
    Version = "1.0"
    
    def __init__(self, server_url=None, auth_server_url=None, max_concurrent_queries = 5,
                token = None, token_file = None, timeout = None):    

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
        timeout : int or float
            Request timeout in seconds. Default: None - use default timeout, which is 300 seconds
        """

        self.TokenLib = self.Token = None
        self.TokenFile = token_file

        if token_file and token is None:
            token = self.resfresh_token()

        if token is not None:
            if isinstance(token, (str, bytes)):
                token = SignedToken.decode(token)
            self.Token = token

        server_url = server_url or os.environ.get("METACAT_SERVER_URL")
        if not server_url:
            raise RuntimeError("MetaCat server URL unspecified")

        if token is None:
            self.TokenLib = TokenLib()
            token = self.TokenLib.get(server_url)

        HTTPClient.__init__(self, server_url, token, timeout)
        self.AuthURL = auth_server_url or server_url + "/auth"
        self.MaxConcurrent = max_concurrent_queries
        self.QueryQueue = None
        
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
    
    def get_dataset(self, did=None, namespace=None, name=None):
        """Gets single dataset
        
        Parameters
        ----------
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
            return self.get_json(f"data/dataset?dataset={spec}")
        except NotFoundError:
            return None

    def get_dataset_files(self, did, namespace=None, name=None, with_metadata=False):
        """Gets single dataset
        
        Parameters
        ----------
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
            return self.get_json_stream(f"data/dataset_files?dataset={did}&with_metadata={with_metadata}")
        except NotFoundError:
            return None
        
        
    def create_dataset(self, spec, frozen=False, monotonic=False, metadata=None, metadata_requirements=None, 
            files_query=None, subsets_query=None,
            description=""):

        """Creates new dataset. Requires client authentication.

        Parameters
        ----------
        spec : str
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

        namespace, name = spec.split(":",1)     
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
        
        Parameters
        ----------
        parent_spec : str
            Parent namespace, name ("namespace:name")
        child_spec : str
            Child namespace, name ("namespace:name")
        """
        url = f"data/add_child_dataset?parent={parent_spec}&child={child_spec}"
        return self.get_text(url)
        
    def add_files(self, dataset, file_list=None, namespace=None, query=None):
        """Add existing files to an existing dataset. Requires client authentication.
        
        Parameters
        ----------
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
        list
            list of dictionaries, one dictionary per file with file ids: { "fid": "..." }
        
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
        if file_list:
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
        return out

    def declare_file(self, did=None, namespace=None, name=None, auto_name=None,
                     dataset_did=None, dataset_namespace=None,
                     dataset_name=None, size=0, metadata={}, fid=None, parents=[], checksums={},
                     dry_run=False):
        """Declare new file and add it to the dataset. Requires client authentication.
        
        Parameters
        ----------
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
            parents = parents
        )
        if not name and auto_name:
            info["auto_name"] = auto_name
        return self.declare_files(dataset_did, [info])[0]

    def declare_files(self, dataset, files, namespace=None, dry_run=False):
        """Declare new files and add them to an existing dataset. Requires client authentication.
        
        Parameters
        ----------
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
        
            Each file to be declared must be represented with a dictionary. The dictionary must contain elements:
                one of:
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
        out = self.post_json(url, lst)
        return out

    def update_file_meta(self, metadata, files=None, names=None, fids=None, namespace=None, dids=None, mode="update"):
        """Updates metadata for existing files. Requires client authentication.
        
        Parameters
        ----------
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
                yield ObjectSpec(namespace, name).to_dict()
            for did in (dids or []):
                spec = ObjectSpec(did)
                spec.validate()             # will raise ValueError
                yield spec.to_dict()
            for fid in (fids or []):
                yield ObjectSpec(fid=fid).to_dict()
            for item in (files or []):
                spec = ObjectSpec.from_dict(item)
                spec.validate()             # will raise ValueError
                yield spec.to_dict()

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

    def update_dataset(self, dataset, metadata=None, mode="update", frozen=None, monotonic=None, description=None):   
        """Update dataset. Requires client authentication.
        
        Parameters
        ----------
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
        
        Parameters
        ----------
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
        
        Parameters
        ----------
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

    def async_query(self, query, data=None, **args):
        """Run the query asynchronously. Requires client authentication if save_as or add_to are used.
        
        Parameters
        ----------
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
        
        if self.QueryQueue is None:
            try:    
                from pythreader import TaskQueue
            except ModuleNotFoundError:
                raise ModuleNotFoundError("pythreader module required for asynchronous queries. Use: pip install 'pythreader>=2.7.0'")
            self.QueryQueue = TaskQueue(self.MaxConcurrent)
            
        return self.QueryQueue.add_lambda(self.query, query, promise_data=data, **args)

    def wait_queries(self):
        """Wait for all issued asynchronous queries to complete
        """
        if self.QueryQueue is not None:
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
            Namespace information or None if the namespace was not found
        """
        
        try: 
            return self.get_json(f"data/namespace?name={name}")
        except NotFoundError:
            return None
        
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
        
    def list_namespaces(self, pattern=None, owner_user=None, owner_role=None, directly=False):
        """Creates new namespace
        
        Parameters
        ----------
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
    
