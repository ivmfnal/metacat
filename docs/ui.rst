Command Line Interface
======================

Installation
------------

You will need Python 3.7 or newer.

To install the client side components:

  .. code-block:: shell

      git clone https://github.com/ivmfnal/metacat.git
      mkdir ~/build
      cd metacat
      make # this will create ~/build/metadata and 
           # a tar file with contents of ~/build/metadata
      pip install --user requests pyyaml


You can use the client components from ~/metadata/build or from some other location where you untar the tar file created by make

Environment
-----------

To set the client environment:

  1. Create config YAML file:
  
      .. code-block:: yaml
      
          Server:
              URL:    http://host.fnal.gov:8080

  2. Set environment:
  
      .. code-block:: shell
      
          export PYTHONPATH=~/build/metadata/lib:~/build/metadata/ui:$PYTHONPATH
          export PATH=~/build/metadata/ui:$PATH
          export METACAT_CONFIG=/path/to/config.yaml

Authentication
--------------

.. code-block:: shell
    
    metacat auth login <username>           # login, will create/update ~/.metacat_tokens
    metacat auth whomi                      # shows current token username and expiration
        
Namespaces
----------

Currently done via GUI only


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
        -i|--ids                            - print file ids instead of names
        -s|--summary                        - print only summary information
        -m|--metadata=[<field>,...]         - print metadata fields
                                              overrides --summary
        -m|--metadata=all                   - print all metadata fields
                                              overrides --summary
        -N|--namespace=<default namespace>  - default namespace for the query

    

        
