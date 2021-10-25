import psycopg2, sys, getopt
from dbobjects2 import DBUser, DBRole

Usage = """python init_db.py <connstr> <admin user> <password>
"""

args = sys.argv[1:]
if len(args) != 3:
    print(Usage)
    sys.exit(1)

connstr, username, password = args

db = psycopg2.connect(connstr)

user = DBUser(db, username, "", "")
user.Authenticators["password"] = [password]
user.save()

admin = DBRole.get(db, "admin")
if not admin:
    admin = DBRole(db, "admin", "Administrator")

admin.Users.append(user)
admin.save()

print("User %s created and added to the admin role" % (username,))
