Server Installation
===================

Running in Docker Container
---------------------------

1. Clone MetaCat sources repository from github:

    .. code-block:: shell
    
        $ git clone https://github.com/ivmfnal/metacat
        $ cd metacat

2. Start a Postgres server version 14 or later. Once the server is running, create MetaCat database tables:

    .. code-block:: shell

        $ psql -h <database host> -p <port> -U <database user> -d <database name> -f metacat/db/schema_3.1.sql

3. Build MetaCat server Docker image:

    .. code-block:: shell

        $ cd docker/server
        $ ./build.sh
        
4. Create configuration directory and edit server configuration file:

    .. code-block:: shell

        $ mkdir /path/to/config
        $ cp docker/server/config.yaml.template /path/to/config/config.yaml
        $ vi /path/to/config/config.yaml
        
5. Run MetaCat server

    .. code-block:: shell

        $ cd docker/server
        $ ./run.sh -c /path/to/config -p <external TCP port>

Initializing MetaCat Database
-----------------------------

After creating the database schema, there is one more step needed to complete the database initialization - create
an admin user. This step is done by directly accessing the MetaCat database.

1. Install MetaCat client

    .. code-block:: shell

        $ pip install --user metacat
        
2. Create admin account

    .. code-block:: shell
    
        $ metacat admin -c /path/to/config/config.yaml create <username> <password>
        
Once an admin account is created, you can access MetaCat GUI at:  

    .. code-block:: shell

        http://host:<external TCP port>/gui/index

Log in as the admin account you just created. The admin account can be used to create regular users and use other functions.

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

