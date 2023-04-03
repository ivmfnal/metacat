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

