import sys, getopt
from metacat.common import password_digest_hash, ConnectionWithTransactions
from metacat.ui.cli import CLI, CLICommand

def connect(config):
    import psycopg2
    dbcfg = config["database"]
    connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s" % dbcfg
    if "password" in dbcfg:
        connstr += " password=%(password)s" % dbcfg
    conn = psycopg2.connect(connstr)
    schema = dbcfg.get("schema")
    if schema:
        conn.cursor().execute(f"set search_path to {schema}")
    return ConnectionWithTransactions(conn)

class ListCommand(CLICommand):

    Usage = """                                     -- list admins"""

    def __call__(self, command, config, opts, args):
        from metacat.db import DBUser
        db = connect(config)
        users = DBUser.list(db)
        for u in users:
            if u.is_admin():
                print(u.Username, u.Name)

class CreateAdminCommand(CLICommand):

    Opts = "r:"
    MinArgs = 2
    Usage = """<username> <password>                -- create new admin account
        -r <realm>                  -- realm to be used for the digest authentication
                                       can be also specified in the config file:
                                    
                                        authentication:
                                            realm: ...
    """

    def __call__(self, command, config, opts, args):
        from metacat.db import DBUser
        db = connect(config)
        realm = opts.get("-r", config.get("authentication", {}).get("realm"))
        if not realm:
            print("Digest authentication realm must be specified either in the config file (authentication->realm) or with -r",
                file = sys.stderr)
            sys.exit(2)
        username, password = args
        u = DBUser.get(db, username)
        if u is not None:
            print("User already exists. Leaving users status unchanged. Use 'metacat admin add ...'")
            sys.exit(1)
        u = DBUser(db, username, "Admin", "", "a", {}, None)
        u.set_password(realm, password)
        u.save()
        print("Admin user %s created" % (username,))

class PasswordCommand(CLICommand):
    
    MinArgs = 2
    Usage = """[-r <realm>] <username> <password>   -- change account password
        -r <realm>                  -- realm to be used for the digest authentication
                                       can be also specified in the config file:
                                        
                                        authentication:
                                            realm: ...
    """
    Opts = "r:"
    
    def __call__(self, command, config, opts, args):
        from metacat.db import DBUser
        db = connect(config)
        realm = opts.get("-r", config.get("authentication", {}).get("realm"))
        if not realm:
            print("Digest authentication realm must be specified either in the config file (authentication->realm) or with -r",
                file = sys.stderr)
            sys.exit(2)
        username, password = args
        u = DBUser.get(db, username)
        if u is None or not u.is_admin():
            print("User does not exist or is not an Admin. Leaving the password unchanged.")
            sys.exit(1)
        u.set_password(realm, password)
        #print("hashed password:", hashed)
        u.save()
        print("Password updated")

class AddCommand(CLICommand):

    MinArgs = 1
    Usage = """<username>                           -- add admin privileges to an existing account"""

    def __call__(self, command, config, opts, args):
        from metacat.db import DBUser
        username = args[0]
        db = connect(config)
        u = DBUser.get(db, username)
        if u is None or u.is_admin():
            print("User does not exist or is an Admin already.")
            sys.exit(1)
        u.Flags = (u.Flags or "") + "a"
        u.save()
        print("Admin privileges added")

class RemoveCommand(CLICommand):

    MinArgs = 1
    Usage = """<username>                           -- remove admin privileges from an account"""
    
    def __call__(self, command, config, opts, args):
        from metacat.db import DBUser
        username = args[0]
        db = connect(config)
        u = DBUser.get(db, username)
        if u is None or not u.is_admin():
            print("User does not exist or is not an Admin.")
            sys.exit(1)
        u.Flags = (u.Flags or "").replace("a", "")
        u.save()
        print("Admin privileges removed")

class GenerateCommand(CLICommand):
    
    Opts = "xl:"
    Usage = """[-x] [-l <length>]                   -- generate random password
        -x                  -- bytes, print as hex
        -l <length>         -- password length    
    """
    
    
    def __call__(self, command, config, opts, args):
        from metacat.db import DBUser
        import secrets
        length = int(opts.get("-l", 32))
        if "-x" in opts:
            key = secrets.token_hex(length)
        else:
            key = secrets.token_urlsafe(length)
        print (key)

class InitDtabaseCommand(CLICommand):

    Opts = "o:a:"
    Usage = """[options]                            -- initialize database
        -o <owner user>         -- create database objects as owned by that user or role, default: same as the DB user from config
        -a <access user>[,...]  -- grant read/write privileges to this database users or roles
    """

    def __call__(self, command, config, opts, args):
        from metacat.db import drop_tables_sql, create_schema_sql

        db = connect(config)        
        c = db.cursor()

        #
        # set role
        #
        role = opts.get("-o")
        if role:
            print(f"settig owner to {role} ...")
            c.execute(f"set role {role};")

        #
        # create schema if specified
        #
        schema = config["database"].get("schema")
        if schema:
            print(f"creating schema {schema}, if necessary ...")
            c.execute(f"""
                        create schema if not exists {schema};
                        set search_path to {schema};
            """)

        # 
        # drop tables and views
        #
        print(f"dropping existing objects ...")
        c.execute(drop_tables_sql)

        #
        # create tables and views
        #
        print(f"creating database objects ...")
        c.execute(create_schema_sql)

        c.execute("commit")
        
        if "-a" in opts:
            roles = opts.get("-a")
            c.execute("grant all on all tables in schema %s to %s" % (schema or "public", roles))
            c.execute("grant usage on schema %s to %s" % (schema or "public", roles))
            print("granted privileges to user(s)", roles)

        print("database initialized")


class AdminCLI(CLI):

    Opts = "c:"
    Hidden = True
    Usage = "-c <database config YAML file> <command> ..."
    
    def update_context(self, context, command, opts, args):
        import yaml
        if "-c" not in opts:
            print("Database configuration must be specified with -c", file=sys.stderr)
            sys.exit(2)
        cfg = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
        return cfg

AdminCLI = AdminCLI(
    "init",         InitDtabaseCommand(),
    "create",       CreateAdminCommand(),
    "add",          AddCommand(),
    "password",     PasswordCommand(),
    "list",         ListCommand(),
    "remove",       RemoveCommand(),
    "generate",     GenerateCommand()
    )



