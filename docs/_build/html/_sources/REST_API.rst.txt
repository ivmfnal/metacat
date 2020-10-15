Metaserver REST API
===================

Client authentication
---------------------

Obtain Token
~~~~~~~~~~~~

.. code-block:: bash

    curl --digest -u user:password -c cookie.jar "http://host:port/auth/auth"
    
The token will be stored in the cookie.jar file
    
Check Token
~~~~~~~~~~~~

.. code-block:: bash

    curl -b cookie.jar "http://host:port/auth/whoami"
    
Extract token as string:
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    curl -b cookie.jar -o token.file "http://host:port/auth/token"

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

List datasets: 
    
    .. code-block::

        GET /data/datasets?with_file_counts=(no|yes)

Show dataset:   

    .. code-block::

        GET /data/dataset?dataset=<namespace>:<name>
        GET /data/dataset/<namespace>:<name>
    
Dataset file count: 

    .. code-block::

        GET /data/dataset_count?dataset=<namespace>:<name>
        GET /data/dataset_count/<namespace>:<name>
    
File information:

    .. code-block::

        GET /data/file?spec=<namespace>:<name>
        GET /data/file?fid=<file id>
    
Query:

    .. code-block::

        GET /data/query?namespace=<default namespace>&with_meta=(yes|no)&query=<url encoded query>
        POST (with query as text file) /data/query?namespace=<default namespace>&with_meta=(yes|no)

    
Create dataset (authentication required): 

    .. code-block::
    
        POST /data/create_dataset?dataset=<namespace>:<name>
    
Add files to a dataset (authentication required):

    Create a JSON file:
    
    .. code-block:: json
    
        [
            {       
                "name": "file_test_1.dat",
                "parents": [ "fid1", "fid2" ],             
                "metadata": { "i":3, "x":3.14, "type":"data" }      
            },
            {       
                "name": "file_test_2.dat",
                "parents": [ "fid3", "fid4" ],             
                "metadata": { "i":5, "x":7.14, "type":"data" }      
            }
        ]
    
    .. code-block::
    
        POST (with the JSON file as the body) /data/add_files?namespace=<file namespace>&datasets=<namespace1>:<name1>,<namespace2>:<name2> 

Update multiple file metadata (authentication required):

    Create a JSON file:
    
    .. code-block:: json
    
        [
            {       
                "spec": "test:file_test_1.dat",
                "parents": [ "fid1", "fid2" ],             
                "metadata": { "i":3, "x":3.14, "type":"data" }      
            },
            {       
                "fid": "file_id",
                "parents": [ "fid3", "fid4" ],             
                "metadata": { "i":5, "x":7.14, "type":"data" }      
            }
        ]

    .. code-block::
    
        POST (with the JSON file as the body) /data/update_files 
    
Update single file metadata (authentication required):

    Create a JSON file with new metadata:
    
    .. code-block:: json
    
        { "i":3, "x":3.14, "type":"data" }      


    .. code-block::
    
        POST (with the JSON file as the body) /data/update_file?spec=<namespace>:<name>
                                           or /data/update_file?fid=<file id>
    
