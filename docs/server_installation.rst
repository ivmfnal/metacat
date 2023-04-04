Server Installation
===================

Download and set up
-------------------

1. Clone MetaCat sources repository from github and install the client:

    .. code-block:: shell
    
        $ git clone https://github.com/ivmfnal/metacat
        $ cd metacat
        $ python setup.py install --user

2. Start a Postgres server version 14 or later. Once the server is running, create MetaCat database using Postgres ``createdb`` command
   or similar.

3. Create configuration directory and the server configuration file

    .. code-block:: shell

        $ mkdir /path/to/config
        $ cp <top of cloned MetaCat repository>/webserver/config.yaml.template /path/to/config/config.yaml
        $ vi /path/to/config/config.yaml

4. Initialize the MetaCat database

    .. code-block:: shell

        $ metacat admin -c /path/to/config/config.yaml [options] init
        
        Options:
    
            -o <owner role>     -- create database objects as owned by that role, default: same as the DB user from config
    
5. Create an admin user

    .. code-block:: shell

        $ metacat admin -c /path/to/config/config.yaml create <admin username> <admin password>
        
    Admin user is needed to create regular users. Also, the admin has privileges to manage various MetaCat objects.
    Once the MetaCat server is running, log in to GUI as the admin user and go to Users tab.
    In the future, you can add more admin users or make a regular user an admin.

Running MetaCat server as a Docker Container
--------------------------------------------

1. Build MetaCat server Docker image:

    .. code-block:: shell

        $ cd <top of cloned MetaCat repository>/docker/server
        $ ./build.sh
        
2. Create configuration directory and edit server configuration file or use the one you created earlier but make sure to place
   it into a separate directory. This directory will have to be mounted into the Docker container.

    .. code-block:: shell

        $ mkdir /path/to/config
        $ cp <top of cloned MetaCat repository>/docker/server/config.yaml.template /path/to/config/config.yaml
        $ vi /path/to/config/config.yaml
        
3. Run MetaCat server

    .. code-block:: shell

        $ cd <top of cloned MetaCat repository>/docker/server
        $ ./run.sh -c /path/to/config -p <external TCP port>


Configuring LDAP Authentication
-------------------------------

To enable LDAP authenticartion, add the following parameters to the ``authentication`` section of the server configuration file:


    .. code-block::

        authentication:
            ldap:
                server_url: ldaps://ldaps.domain.org
                dn_template: "cn=%s,ou=Users,dc=services,dc=domain,dc=org"


the ``dn_template`` is a template defining the conversion from username to LDAP DN. MetaCat server will substitute ``%s`` with the username.


Configuring WLCG Token Authentication
-------------------------------------

To enable WLCG token authentication, you need to add the list of trusted token issuers to the server configuration:

    .. code-block::

        authentication:
            sci_token_issuers:
                - https://cilogon.org/my_org
                - https://issuer.com/group

If the token issuer replaces username with some other user identifier, you will need to populate the database with the alternative
user identifier. The ``users`` database table has ``auid`` column. When MetaCat server authenticates the user, it goes through
the following steps:

    #. Verify the integrity of the token and check its expiration time;

    #. Get the user record from the ``users`` table of the MetaCat database by the username presented bu the client. If the user
       record with the given username does not exist - retrun with error;
    
    #. Get the ``subject`` from the token
    
    #. Compare ``username`` to the ``subject`` from the token. If they match, return with success;
    
    #. Compare ``auid`` field from the user record from the database to the ``subject``. If they match, return with success
    
    #. Return an error

Currently, there is a limitation that a user can have only one alternative user identifier.

