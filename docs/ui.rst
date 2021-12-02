Command Line Interface
======================

Installation
------------

You will need Python 3.7 or newer.

To install the client side components:

  .. code-block:: shell

      $ git clone https://github.com/ivmfnal/metacat.git
      $ cd metacat
      $ python setup.py install --user
      
Make sure ~/.local/bin is in your PATH:

  .. code-block:: shell

      $ export PATH=${HOME}/.local/bin:$PATH
      
If you use your own Python installation, e.g. Anaconda or Miniconda, then you can do this instead:

  .. code-block:: shell

      $ python setup.py install


General CLI command syntax
--------------------------

General command looks like this:

    .. code-block:: shell
    
        $ metacat [-s <server URL>] [-a <auth server URL>] <command> [command options] [arguments ...]
    
    
-a is used to specify the URL for the authenticartion server. It is used only for authentication commands.
-s option specified the server URL. Alternativey, you can define the METACAT_AUTH_SERVER_URL and METACAT_SERVER_URL environment variables:

    .. code-block:: shell
    
        $ export METACAT_SERVER_URL="http://server:port/path"
        $ # optionally: export METACAT_AUTH_SERVER_URL="http://auth_server:port/auth_path"
        $ metacat <command> [command options] [arguments ...]
        
    
User Authentication
-------------------

Main purpose of authentication commands is to obtain an authentication token and store it in
the MetaCat *token library* located at ~/.metacat_tokens. The library may contain multiple
tokens, one per MetaCat server instance the user communicates with. The instances are identified
by their URL.

To obtain a new token, use ``metacat auth login`` command. Currently, 2 authentication mechanisms
are implemented: password and X.509 certificates. LDAP or MetacCat server "local" password can be used with the
password autentication. X.509 method supports both X.509 certificates and proxies.

Token obtained using CLI ``metacat auth login`` command can be further used by both CLI and API until it expires.

To obtain a new token using password authentication, use the following command:

.. code-block:: shell
    
    $ metacat auth login <username>           
	
To use X.805 authentication

.. code-block:: shell
    
    $ metacat auth login -m x509 -c <cert file> -k <key file> <username>
    $ metacat auth login -m x509 -c <proxy file> <username>

Environment variables X509_USER_CERT, X509_USER_KEY and X509_USER_PROXY can be used instead of -c and -k options:

.. code-block:: shell
    
    $ export X509_USER_PROXY=~/user_proxy
    $ metacat auth login -m x509 <username>

Before X.509 method is enabled for the user, the user needs to contact the MetaCat amdinistrator to enter their
subject DN into MetaCat user database. In order to obtain the DN of the user certificate, use ``metacat auth mydn`` command:

.. code-block:: shell
    
    $ metacat auth mydn -c my_cert.pem -k my_key.pem 
    CN=UID:jjohnson,CN=John Johnson,OU=People,O=Fermi National Accelerator Laboratory,C=US,DC=cilogon,DC=org

If you want to use your X.509 proxy, then you need to send the issuer DN instead of the subject DN to the administrator. Use ``-i``
option with ``mydn`` command to get the issuer DN:

.. code-block:: shell
    
    $ metacat auth mydn -c my_proxy -i
    CN=UID:jjohnson,CN=John Johnson,OU=People,O=Fermi National Accelerator Laboratory,C=US,DC=cilogon,DC=org


List available tokens

.. code-block:: shell
    
    metacat auth list

Export token to a file or to stdout

.. code-block:: shell
    
    metacat auth token [-o <token file>]
	
Verify a token

.. code-block:: shell
    
    metacat auth whoami [-t <token file>]
	


Namespaces
----------

.. code-block:: shell

    $ metacat namespace create my_namespace
    $ metacat namespace create -o owner_role my_namespace
    $ metacat namespace list "protodune*"
    $ metacat namespace show protodune-sp
    

Datasets
--------

.. code-block:: shell
    
    metacat dataset list [[<namespace pattern>:]<name pattern>]     - list datasets
    # examples:
    # metacat dataset list ns1:*
    # metacat dataset list *:A*
    
    metacat dataset create [-p <parent namespace>:<parent name>] <namespace>:<name>
    metacat dataset show <namespace>:<name>

Declaring new Files
-------------------

Create JSON file with metadata::

    [
        {   
            "namespace":"namespace",    # optional - use -N to specify default
            "name":"name",              # optional
            "auto_name":pattern,        # optional
            "fid":"...",                # optional - new will be generated if missing, will fail if specified and already exists
            "metadata": { ... },        # optional
            "parents":  [ "fid1", "fid2", ... ]     # optional, must be file ids         
            "size":   1234              # required
        },
        ...
    ]

You can get a sample of the JSON file:

.. code-block:: shell
    
    metacat file declare --sample
        
Declare files:

.. code-block:: shell

    declare [-N|--namespace <default namespace>] -j|--json <json file> [<dataset namespace>:]<dataset>
        
When declaring multiple files, the command accepts JSON file path. The JSON file provides information about the files being declared. The JSON structure in the file
must be a list of dictionaries, one dictionary per file to be declared. Each dictionary has the following items:

``fid`` : optional
    File ID for the new file. Must be unique for the MetaCat instance. 
    If unspecified, MetaCat will assign the hexadecimal representation of a random UUID (32 hex digits) as the file ID.

``namespace`` : optional
    Namespace for the file. If unspecified, the default namespace specified with ``-N`` will be used.
    
``name`` : optional
    File name. The file name must be unique within the namespace. If unspecified, the name will be auto-generated or the file ID will be used as the name.
    
``auto_name`` : optional
    Pattern to be used to generate new file name. The pattern is specified in terms of Python print-style format string. The following mapping keys will be pre-defined:
    
    * ``%(uuid)s`` - hexadecimal representation of a random UUID (32 hex digits)
    * ``%(uuid8)s`` - 4-bytes (8 hex digits) of a random UUID 
    * ``%(uuid16)s`` - 8-bytes (16 hex digits) of a random UUID 
    * ``%(clock)d`` - UNIX timestamp in milliseconds as integer ( int(time*1000) )
    * ``%(clock3)d`` - milliseconds portion of UNIX timestamp as integer
    * ``%(clock6)d`` - lowest 6 digits of UNIX timestamp in milliseconds as integer
    * ``%(clock9)d`` - lowest 9 digits of UNIX timestamp in milliseconds as integer

    For example, the pattern ``file_%(uuid8)s_%(clock6)d.dat`` may generate file name ``file_13d79a37_601828.dat``

    If neither ``name`` nor ``auto_name`` are provided, then ``file ID`` will be used as the file name.

``size`` : required
    File size in bytes
    
``metadata`` : optional
    File metadata as dictionary
    
``parents`` : optional
    List of parent files IDs
    
When declaring a file or multiple files, they must be added to a dataset.

Adding files to dataset
-----------------------

.. code-block:: shell
    
    metacat add -n <namespace>:<name>[,...] <dataset namespace>:<dataset name>
    metacat add -n @<file with names> <dataset namespace>:<dataset name>
    metacat add -n - <dataset namespace>:<dataset name>             # read file namesspace:name's from stdin 

    metacat add -i <file id>[,...] <dataset namespace>:<dataset name>
    metacat add -i @<file with ids> <dataset namespace>:<dataset name>
    metacat add -i - <dataset namespace>:<dataset name>             # read file ids from stdin 

    metacat add -j <JSON file> <dataset namespace>:<dataset name>
        
JSON file structure::
    
    [
        {   
            "name":"namespace:name"
        },
        {
            "fid":"..."
        },
        ...
    ]

Get a sample of the JSON file:

.. code-block:: shell
    
    metacat file add --sample

**Example:** add files from dataset A but not in dataset B to dataset C:

.. code-block:: shell

    $ metacat query -i -N test "files from A - files from B" > file_ids.txt
    $ metacat file add -i @file_ids.txt test:C

File Metadata
-------------

        
Updating
~~~~~~~~

Create JSON file with metadata values::

    {
        "x": 3.14,
        "run_type": "calibration"
    }

Update metadata:

.. code-block:: shell
    
    metacat update -n <namespace>:<name>[,...] @metadata.json
    metacat update -n @<file with names> @metadata.json
    metacat update -n - @metadata.json             # read file namesspace:name's from stdin 

    metacat update -i <file id>[,...] @metadata.json
    metacat update -i @<file with ids> @metadata.json
    metacat update -i - @metadata.json             # read file ids from stdin 
    
    or you can put new metadata inline:
    
    metacat update -n <namespace>:<name>[,...] '{"x": 3.14, "run_type": "calibration"}'
    ...
    
        
Retrieving
~~~~~~~~~~

.. code-block:: shell

    metacat file show <namespace>:<name>            # - by namespace/name
    metacat file show -i <fid>                      # - by file id

Query
-----

:doc:`/mql`

.. code-block:: shell

    metacat query <options> "<MQL query>"
    metacat query <options> -f <MQL query file>

    Options:
        -j|--json                           - print raw JSON output
        -p|--pretty                         - pretty-print metadata
        -l|--line                           - print all metadata on single line (good for grepping, ignored with -j and -p)
        -i|--ids                            - print file ids instead of names
        -s|--summary                        - print only summary information
        -m|--metadata=[<field>,...]         - print metadata fields
                                              overrides --summary
        -m|--metadata=all                   - print all metadata fields
                                              overrides --summary
        -P|--with-provenance                - include provenance information
        -N|--namespace=<default namespace>  - default namespace for the query
        -S|--save-as=<namespace>:<name>     - save files as a new datset
        -A|--add-to=<namespace>:<name>      - add files to an existing dataset
        

    

        
