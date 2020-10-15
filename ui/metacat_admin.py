import yaml, sys, getopt
from metacat.db import DBUser
import psycopg2

Usage="""
metacat -c <config file> admin  create <username> <password>   - create new admin account
                                password <username> <password> - change admins password
                                add <username>                 - add admin privileges
                                remove <username>              - remove admin privileges
                                list                           - list all admin accounts

Required direct access to the database. The config file must include:

    database:
        host: ...
        port: ...
        user: ...
        password: ...
        dbname: ...
"""

def connect(config):
    dbcfg = config["database"]
    connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % dbcfg
    return psycopg2.connect(connstr)
    

def do_list(config, args):
    db = connect(config)
    users = DBUser.list(db)
    for u in users:
        if u.is_admin():
            print(u.Username, u.Name)

def do_create(config, args):
    if len(args) != 2:
        print(Usage)
        sys.exit(2)
    username, password = args
    db = connect(config)
    u = DBUser.get(db, username)
    if u is not None:
        print("User already exists. Leaving users status unchanged. Use 'metacat admin add ...'")
        sys.exit(1)
    u = DBUser(db, username, "Admin", "", flags="a")
    u.set_password(password)
    u.save()
    print("Admin user %s created" % (username,))

def do_password(config, args):
    if len(args) != 2:
        print(Usage)
        sys.exit(2)
    username, password = args
    db = connect(config)
    u = DBUser.get(db, username)
    if u is None or not u.is_admin():
        print("User does not exist or is not an Admin. Leaving the password unchanged.")
        sys.exit(1)
    u.set_password(password)
    u.save()
    print("Password updated")
    
def do_add(config, args):
    if len(args) != 1:
        print(Usage)
        sys.exit(2)
    username = args[0]
    db = connect(config)
    u = DBUser.get(db, username)
    if u is None or u.is_admin():
        print("User does not exist or is an Admin already.")
        sys.exit(1)
    u.Flags = (u.Flags or "") + "a"
    u.save()
    print("Admin privileges added")
    
def do_remove(config, args):
    if len(args) != 1:
        print(Usage)
        sys.exit(2)
    username = args[0]
    db = connect(config)
    u = DBUser.get(db, username)
    if u is None or not u.is_admin():
        print("User does not exist or is not an Admin.")
        sys.exit(1)
    u.Flags = (u.Flags or "").replace("a", "")
    u.save()
    print("Admin privileges removed")
    
def do_admin(config, args):
    
    if not args:
        print(Usage)
        sys.exit(2)
        
    command = args[0]
    return {
        "list":     do_list,
        "create":   do_create,
        "password":     do_password,
        "add":     do_add,
        "remove":     do_remove,
    }[command](config, args[1:])
    
 

    


