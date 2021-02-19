MetaCat Server REST API
=======================

Client authentication
---------------------

Obtain Token
~~~~~~~~~~~~

.. code-block:: bash

    curl --digest -u user:password -c cookie.jar "https://host:port/auth/auth?method=digest"
    curl -T password_file.txt -c cookie.jar      "https://host:port/auth/auth?method=ldap&username=user"
    curl --cert=... --key=... -c cookie.jar      "https://host:port/auth/auth?method=x509&username=user"
    
The token will be stored in the cookie.jar file
    
Check Token
~~~~~~~~~~~~

.. code-block:: bash

    curl -b cookie.jar "https://host:port/auth/whoami"
    
Extract token as string:
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    curl -b cookie.jar -o token.file "https://host:port/auth/token"

this will save the token in the "token.file"

Use Token
~~~~~~~~~

As a cookie from the cookie jar file:

.. code-block:: bash

    curl -b cookie.jar "http://host:port/data/create_dataset?dataset=test:test"
    
From saved token file:

.. code-block:: bash

    curl -H "X-Authentication-Token: `cat token.file`" http://localhost:8080/auth/whoami


REST Methods
------------

All data methods return JSON structure

Namespaces
~~~~~~~~~~

Create namespace
    .. code-block::

        GET /data/create_namespace
            name=<namespace name>
            [description=<description, URL quoted>]
            [owner_role=<role name>]

    Client authentication required
    
    If owner_role is specified, the created namespace will be owned by the role. Otherwise by the
    user associated with the client.
    
    Returns: Dictionary with namespace attributes
	
Get single namespace by name
    .. code-block::

        GET /data/namespace?name=<namespace name>
    
    Returns: Dictionary with namespace attributes

Get multiple namespaces
    .. code-block::

        GET/POST /data/namespaces
    
    Request body: JSON stricture, list of namespace names. If the request body is empty, then the method will return
    all namespaces.
    
    Returns: list of dictionaries, one dictionary per namespace with namespace attributes
    
Get namespace members counts

    .. code-block::

        GET /data/namespace_counts?name=<namespace name>
    
    Returns: Dictionary with counts of files, datasets and saved queries in the namespace
	
Datasets
~~~~~~~~

Get all datasets
    .. code-block::

        GET /data/datasets
            [with_file_counts=(yes|no) default="no"]

    Returns: list of dictionaries, one dictionary per dataset with dataset attributes. If with_file_counts=yes,
    each dictionary will include "file_count" field.

Get single dataset by name
    .. code-block::

        GET /data/dataset?dataset=<namespace>:<name>
    
    Returns: Dictionary with dataset attributes
    

Create dataset
    .. code-block::

        GET /data/create_dataset?dataset=<namespace>:<name>
            [description=<description, URL quoted>]
            [parent=<namespace>:<name>]
            [frozen=(yes|no), default="no"]
            [monotonic=(yes|no), default="no"]

    Client authentication required
    
    Returns: Dictionary with created dataset attributes
    
Update dataset metadata
    .. code-block::

        POST /data/update_dataset_meta?dataset=<namespace>:<name>
            [mode=(update|replace)]

    Request body: JSON list of dictionary with new metadata

    If mode="update", the dataset metadata will be updated with new values. Otherwise, it will be replaced.
    
    Returns: JSON dictionary with updated dataset information
	
Get file count in a dataset

    .. code-block::

        GET /data/dataset_count?dataset=<namespace>:<name>

    Returns: JSON dictionary ``{"file_count":n}``


File Metadata
~~~~~~~~~~~~~

Declare new files
    .. code-block::

        POST /data/declare_files?dataset=[<namespace>:]<name>
            [namespace=<default namespace name>]
        
    If specified, the defaut namespace will be used for the dataset and for all the files to be declared
    
    Request body: JSON list of dictionaries, one dictionary per file:
        
        .. code-block:: json

            [
                {       
                    "name": "file_test_1.dat",
                    "parents": [ "fid1", "fid2" ],             
                    "metadata": { "i":3, "x":3.14, "type":"data" }      
                },
                {       
                    "name": "file_test_1.dat",
                    "parents": [ "fid1", "fid2" ],             
                    "metadata": { "i":3, "x":3.14, "type":"data" }      
                },
                {       
                    "name": "namespace:file_test_3.dat",
                    "fid":"6452476294"
                }
            ]
        
    Each file dictionary contains the following fields:
    
        * name - required - Can be either <namespace>:<name>, or just <name> if the URI contains the default namespace
        * fid - optional - file id. If unspecified, MetaCat will generate new id.
        * parents - optional - list of parent file ids
        * metadata - optional - with file metadata dictionary
            
Add existing files to a dataset

    .. code-block::

        POST /data/declare_files?dataset=[<namespace>:]<name>
            [namespace=<default namespace name>]

    If specified, the defaut namespace will be used for the dataset and for all the files to be declared
    
    Request body: JSON list of dictionaries, one dictionary per file:
        
        .. code-block:: json

            [   
                {   "name": "file_test_1.dat",  },
                {   "name": "namespace:file_test_3.dat" },
                {   "fid":"6452476294"  }
            ]
        
    Each file dictionary must contain either file id or file namespace/name:
    
        * name - Can be either <namespace>:<name>, or just <name> if the URI contains the default namespace
        * fid - file id

Update file metadata:

    .. code-block::

        POST /data/update_file_meta
            [mode=(update|replace)]
            [namespace=<default namespace name>]

    If specified, the defaut namespace will be used for the dataset and for all the files to be declared.
    
    mode can be either "update" or "replace".
    
    Request body: JSON dictionary with the following frields:
        
        * names - optional - list of <namespace>:<name>'s or <name>'s for the files to be updated. If
          namespace is unspecified, then the default namespace from the URI will be used.
        * fids - optional - list of file ids for the files to be updates
        * metadata - required - dictionary with common metadata for all the files. If mode="replace",
          metadata for listed files will be replaced with new metadata. Otherwise, existing metadata
          will be updated.

          The dictionary must contain "names" or "fids" list or both.
          
Get file information

    .. code-block::

        GET /data/file?name=<namespace>:<name>
        GET /data/file?fid=<file id>
            [with_metadata=(yes|no), default="yes"]
            [with_provenance=(yes|no), default="yes"]
        
    Returns: JSON dictionary with file information
    
Get multiple files information

    .. code-block::

        POST /data/files
            [with_metadata=(yes|no), default="yes"]
            [with_provenance=(yes|no), default="yes"]
        
    Request body: JSON list of dictionaries, one dictionary per file:

    Returns: JSON dictionary with file information

        .. code-block:: json

            [   
                {   "name": "file_test_1.dat",  },
                {   "name": "namespace:file_test_3.dat" },
                {   "fid":"6452476294"  }
            ]
        
    Each file dictionary must contain either file id or file namespace/name:
    
        * name - <namespace>:<name>
        * fid - file id

    Returns: JSON list of dictionaries with file information

Queries
~~~~~~~

    .. code-block::

        GET /data/query?query=<url encoded query>
        POST /data/query
            [namespace=<default namespace>]
            [with_meta=(yes|no), default="no"]
            [with_provenance=(yes|no), default="no"]
            [add_to=[<dataset namespace>:]<dataset name>]
            [save_as=[<dataset namespace>:]<dataset name>]

    Query is specified either as URL-encoded ``query`` URI argument or as the request body.
    
    ``namespace`` is default namespace for the query and for ``save_as`` and ``add_to`` datasets.
    
    Returns: JSON list with query results, a dictionary per file ``with_meta`` and ``with_provenance`` control
    whether the file metadata and provenance will be included, respectively.

    If ``add_to`` is specfied, the selected files will be added to the existing dataset.
    
    If ``save_as`` is specified, the selected files will be saved as the new dataset. If the dataset already exists, 
    the request will fail with an error.
	
    
