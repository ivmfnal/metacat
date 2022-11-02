MetaCat Client
===============

MetaCat client consists of Command Line Interface (CLI) and Python API, described in a :ref:`separate section <API>`. 
CLI is in fact based on the Python API.
When you install MetaCat client, both CLI and Python API get installed.

Installation
------------

You will need Python 3.7 or newer.

Preferred way to install the client is using pip:

  .. code-block:: shell

    $ pip install metacat --user
    $ pip3 install metacat --user

Alternatively, it can be installed from github:

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

It is also possible to install MetaCat client from a tarball from https://cdcvs.fnal.gov/redmine/projects/metacat/files


General CLI command syntax
--------------------------

General command looks like this:

    .. code-block:: shell
    
        $ metacat [-s <server URL>] [-a <auth server URL>] <command> [command options] [arguments ...]
    
    
-a is used to specify the URL for the authenticartion server. It is used only for authentication commands.
-s option specified the server URL. Alternativey, you can define the METACAT_AUTH_SERVER_URL and METACAT_SERVER_URL environment variables:

    .. code-block:: shell
    
        $ export METACAT_SERVER_URL="http://server:port/path"
        $ export METACAT_AUTH_SERVER_URL="http://auth_server:port/auth_path"
        $ metacat <command group> <command> [command options] [arguments ...]
        

Versions
--------

To quickly check the connectivity to the MetaCat server and see what software versions are used on the server
and the client sides, use the ``version`` command:

    .. code-block:: shell

        $ metacat version
        MetaCat Server URL:         https://metacat.fnal.gov:9443/dune_meta_demo/app
        Authentication server URL:  https://metacat.fnal.gov:8143/auth/dune
        Server version:             3.9.1
        Client version:             3.9.1

User Authentication
-------------------

Main purpose of MetaCat authentication commands is to obtain an authentication token and store it in
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
	
On successful authentication, the following command will show your username and the token expiration:

.. code-block:: shell
    
    $ metacat auth whoami [-t <token file>]
    User:    jdoe
    Expires: Fri Jul 20 12:35:10 2022


Namespaces
----------

.. code-block:: shell

    $ metacat namespace create <namespace>                     # create namespace owned by me
    $ metacat namespace create -o <owner_role> <namespace>     # create namespace owned by a role
    $ metacat namespace show <namespace>

To list existing namespaces:

.. code-block:: shell

    $ metacat namespace list [options] <pattern>
        <pattern> is a UNIX shell style pattern (*?[])
        -u|--user <username>        - list namespaces owned by the user
        -d                          - exclude namespaces owned by the user via a role
        -r|--role <role>            - list namespaces owned by the role


Parameter Categories
--------------------

To list existing parameter categories:

.. code-block:: shell

        $ metacat category list [options] [<root category>]
                  -j|--json           - print as JSON

To get particular categiry information:

.. code-block:: shell

        $ metacat category show [options] <category>
                  -j|--json           - print as JSON

Datasets
--------

To create dataset in a namespace or to modify the dataset content or metadata, the user must be an owner of the dataset's namespace, 
either directly or through a role.

Creating a dataset
~~~~~~~~~~~~~~~~~~

.. code-block:: shell

    $ metacat dataset create [<options>] <namespace>:<name> [<description>]
      -M|--monotonic
      -F|--frozen
      -m|--metadata '<JSON expression>'
      -m|--metadata @<JSON file>
      -f|--file-query '<MQL file query>'          - run the query and add files to the dataset
      -f|--file-query @<file_with_query>          - run the query and add files to the dataset
      -r|--meta-requirements '<JSON expression>'  - add metadata requirements
      -r|--meta-requirements @<JSON file>         - add metadata requirements

A multi-word description does not have to be put in quotes. E.g., the following two commands are equivalent:

.. code-block:: shell

    $ metacat dataset create scope:name Carefully selected files
    $ metacat dataset create scope:name "Carefully selected files"
    
``-f`` option can be used to create a dataset with files matching the MQL query. The query can be given inline or read from a file.

``-r`` is used to create a dataset with specified metadata requirements. They are specified as a JSON dictionary (to be documented...)

Adding files to dataset
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

    $ metacat dataset add-files [options] <dataset namespace>:<dataset name>
  
      list files by DIDs or namespace/names
      -N|--namespace <default namespace>           - default namespace for files
      -d|--names <file namespace>:<file name>[,...]
      -d|--names -            - read the list from stdin
      -d|--names @<file>      - read the list from file
  
      list files by file id
      -i|--ids <file id>[,...]
      -i|--ids -              - read the list from stdin
      -i|--ids @<file>        - read the list from file
  
      read file list from JSON file
      -j|--json <json file>
      -j|--json -             - read JSON file list from stdin
      -s|--sample             - print JOSN file list sample
  
      add files matching a query
      -q|--query "<MQL query>"
      -q|--query @<file>      - read query from the file

There are several ways to specify the list of files to be added to the dataset:

``-d`` option is used to specify s list of file DIDs ("namespace:name"). ``-i`` option specifies a list of file ids. 

``-j`` option can be used to specify the list of files as a JSON document. The JSON document must contain a list of dictionaries. E.g.:


.. code-block:: json

    [
        {   
            "did":"my_scope:file.data"
        },
        {   
            "namespace":"my_scope",
            "name":"file.data"
        },
        {
            "fid":"abcd1234"
        }
    ]

Each dictionary represents a single file to add. The dictionary must contain one of the following keys and corresponding values:

  - "did" - file DID
  - "namespace" and "name"
  - "fid" - file id

To add files which match an MQL query, use ``-q`` option.

An alternative way to add files matching a query is to pipe the outout of ``query`` command into ``dataset add``:

.. code-block:: shell

    $ metacat query -i files from scope:dataset1 | metacat dataset add-files -i - scope:dataset2

Using ``-q`` can be faster because piping involves sending the file list to the client and back to the server, 
whereas ``-q`` only sends the list of file ids to the client one way.

Note that it is not an error to attempt to add a file if it is already included in the dataset.

Listing existing datasets
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

    $ metacat dataset list [<options>] [[<namespace pattern>:]<name pattern>]
            -l|--long           - detailed output
            -c|--file-counts    - include file counts if detailed output
            

Namespace and name patterns are UNIX ls style patterns (recognizing \*?[]). Examples:

.. code-block:: shell

    $ metacat dataset list 'production:*.[0-3].dat'
    $ metacat dataset list *:A*


When using -l option, user can also use -c to request dataset file counts. In this case, it may take additional time to calculate the file counts for large datasets.


Updating a dataset metadata and flags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

    $ metacat dataset update <options> <namespace>:<name> [<description>]
            -M|--monotonic (yes|no) - set/reset monotonic flag
            -F|--frozen (yes|no)    - set/reset monotonic flag
            -r|--replace            - replace metadata, otherwise update
            -m|--metadata @<JSON file with metadata> 
            -m|--metadata '<JSON expression>' 
            
Listing files in the dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

    $ metacat dataset files [<options>] <dataset namespace>:<dataset name>
            -m|--with-metadata      - include file metadata
            -j                      - as JSON

Adding/removing subsets to/from a dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

    $ metacat dataset add-subset <parent dataset namespace>:<parent name> <child dataset namespace>:<child name> [<child dataset namespace>:<child name> ...]

When adding a dataset to another dataset, MetaCat checks whether the operation will create a circle in the ancestor/descendent relationship and refuses
to do so.

Declaring new files
-------------------

Declare single file
~~~~~~~~~~~~~~~~~~~

Create JSON file with file metadata, e.g.:

.. code-block:: json

    {
        "math.pi": 3.14,
        "processing.status": "done",
        "processing.version": "1.3.5"
    }

then decalre the file:

.. code-block:: shell

      $ metacat declare [options]    [<file namespace>:]<filename>          [<dataset namespace>:]<dataset>
      $ metacat declare [options] -a [<file namespace>:]<auto-name pattern> [<dataset namespace>:]<dataset>
          -d|--dry-run                        - dry run: run all the checks but stop short of actual file declaration
          -j|--json                           - print results as JSON
          -s|--size <size>                    - file size
          -c|--checksums <type>:<value>[,...] - checksums
          -N|--namespace <default namespace>
          -p|--parents   <parent>,...         - parents can be specified as file ids or DIDs
          -m|--metadata  <JSON metadata file> - if unspecified, file will be declared with empty metadata
          -a|--auto-name                      - generate file name automatically

Declare multiple files
~~~~~~~~~~~~~~~~~~~~~~

When declaring multiple files, the command accepts JSON file path. The JSON file provides information about the files to be declared. The JSON structure in the file
must be a list of dictionaries, one dictionary per file to be declared. Each dictionary has the following items:

.. code-block:: json

    [
        {   
            "namespace":"namespace",    # optional - use -N to specify default
            "name":"name",              # optional
            "auto_name":pattern,        # optional
            "fid":"...",                # optional - if missing, new will be generated. If specified, must be unique
            "metadata": { ... },        # optional
            "parents":  [ ... ]         # optional, list of parent file ids
            "size":   1234              # required - size of the file in bytes
        },
        ...
    ]
    

``fid`` : optional
    File ID for the new file. Must be unique for the MetaCat instance. 
    If unspecified, MetaCat will assign the hexadecimal representation of a random UUID (32 hex digits) as the file ID.

``namespace`` : optional
    Namespace for the file. If unspecified, the default namespace specified with ``-N`` will be used.
    
``name`` : optional
    File name. The file name must be unique within the namespace. If unspecified, the name will be auto-generated or the file ID will be used as the name.
    
``auto_name`` : optional
    Pattern to be used to generate new file name. The pattern is may include constant parts and parts to be replaced by the MetaCat in
    the following order:
    
    * $clock3   - lower 3 digits of UNIX timestamp in milliseconds as integer (milliseconds portion of the timestamp)
    * $clock6   - lower 6 digits of UNIX timestamp in milliseconds as integer
    * $clock    - entire UNIX UNIX timestamp in milliseconds as integer
    * $uuid8    - 8 hex digits of a random UUID 
    * $uuid16   - 16 hex digits of a random UUID 
    * $uuid     - 32 hex digits of a random UUID
    * $fid      - file ID

    For example, the pattern ``file_$uuid8_$clock6.dat`` may generate file name ``file_13d79a37_601828.dat``.

    If neither ``name`` nor ``auto_name`` are provided, then ``file ID`` will be used as the file name.

``size`` : required
    File size in bytes
    
``metadata`` : optional
    File metadata as dictionary
    
``parents`` : optional
    List of dictionaries, one dictionary per parent file, in one of 3 formats:

        - { "did": "<namespace>:<name>" }
        - { "namespace":"...", "name":"..." }
        - { "fid": "<file id>" }

    Individual parent dictionaries do not have to be in the same format.
    Specifing parents with list of string file ids instead of dictionaries **is deprecated**.

You can get a sample of the JSON file:

.. code-block:: shell
    
    $ metacat file declare-sample
        
Once you have the JSON file with files description, you can delare them:

.. code-block:: shell

    $ metacat file declare-many [options] <file list JSON file> [<dataset namespace>:]<dataset name>
    Declare multiple files:
          -d|--dry-run                        - dry run: run all the checks but stop short of actual file declaration
          -j|--json                           - print results as JSON
          -N|--namespace <default namespace>

Listing datasets the file is in
-------------------------------

This command will print namespace/name for all the datasets the file is in. Currently, not recursively.

.. code-block:: shell

    $ metacat file datasets [-j|-p] -i <file id>
    $ metacat file datasets [-j|-p] <namespace>:<name>
      -p pretty-print the list of datasets
      -j print the dataset list as JSON
      otherwise print <namespace>:<name> for each dataset


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

Retrieving single file metadata

.. code-block:: shell

        metacat file show [options] (-i <file id>|<namespace>:<name>)
          -m|--meta-only            - print file metadata only
          -n|--name-only            - print file namespace, name only
          -d|--id-only              - print file id only

          -j|--json                 - as JSON
          -p|--pretty               - pretty-print information

          -l|--lineage|--provenance (p|c)        - parents or children instead of the file itself
          -I|--ids                               - for parents and children, print file ids instead of namespace/names

Query
-----

MetaCat queries are written in :doc:`Metadata Query Language <mql>`.

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
        

    

        
