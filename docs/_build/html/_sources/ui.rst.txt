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
    
        $ metacat [-s <server URL>] <command> [command options] [arguments ...]
    
    
-s option specified the server URL. Alternativey, you can define the METACAT_SERVER_URL environment variable:

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

To obtain a new token, use ``metacat auth login`` command. Currently, 3 authentication mechanisms
are implemented: LDAP password, "local" password and X.509 certificates. "Local" password authentication
hashes the user password first and then uses RFC2617 digest mechanism to authenticate the client.
LDAP password is sent as plain text over HTTPS connection.

LDAP password and "local" password authentication mechanisms are combined into single "password" method.
It tries LDAP mechanism first and then, if it fails, tries "local" password authentication.

Token obtained using CLI ``metacat auth login`` command can be used by both CLI and API.

To obtain a new token using password authentication, use the following command:

.. code-block:: shell
    
    metacat auth login <username>           
	
To use X.805 authentication

.. code-block:: shell
    
    metacat auth login -m x509 -c <cert file> -k <key file> <username>

Currently, only certificates issued by trusted CA will be accepted. Short-lived GLOBUS proxies are not accepted.

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
            "name":"namespace:name",    # required
            "fid":"...",                # optional - will fail if already exists
            "metadata": { ... },        # optional
            "parents":  [ "fid1", "fid2", ... ]     # optional, must be file ids         
        },
        ...
    ]

Get a sample of the JSON file:

.. code-block:: shell
    
    metacat file declare --sample
        
Declare files:

.. code-block:: shell

    metacat file declare [-N <default namespace>] \
            metadata.json [<namespace>:]<dataset>
        

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
        

    

        
