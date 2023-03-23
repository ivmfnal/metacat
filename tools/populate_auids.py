from metacat.auth import BaseDBUser
import psycopg2, getopt, sys, yaml, requests

Usage = """
python populate_auids.py [-v] <config file> <Ferry URL prefix> <VO>
    -v                                      - verbose output
    -d                                      - dry run - do not do any changes
    -c <cert or proxy file>                 - if HTTPS is used
    -k <key file>                           - optional if using proxy
"""

opts, args = getopt.getopt(sys.argv[1:], "vc:k:d")
opts = dict(opts)

if len(args) != 3:
    print(Usage)
    sys.exit(2)

verbose = "-v" in opts
dry_run = "-d" in opts

if dry_run:
    print("n\=== dry run ===\n")

config = yaml.load(open(args[0], "r"), Loader=yaml.SafeLoader)
ferry_prefix, vo = args[1], args[2]

cert_file = opts.get("-c")
key_file = opts.get("-k", cert_file)

if ferry_prefix.lower().startswith("https:") and not cert_file:
    print("\nCertificate and private key must be specified with HTTPS URL\n")
    print(Usage)
    sys.exit(1)

db_config = config["database"]
connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % db_config
db = psycopg2.connect(connstr)
schema = db_config.get("schema")
if schema:
    db.cursor().execute(f"set search_path to {schema}")

url = f"{ferry_prefix}/getAffiliationMembersRoles?unitname={vo}"
    
response = requests.get(url, verify=False, cert=(cert_file, key_file))
data = response.json()

status = data["ferry_status"]
if status != "success":
    print("\nFerry error:")
    for line in data["ferry_error"]:
        print(line)
    sys.exit(1)

ferry_users = {item["username"]: item for item in data["ferry_output"][vo]}
db_users = {user.Username: user for user in BaseDBUser.list(db)}

ldap_template = config.get("authentication", {}).get("ldap", {}).get("dn_template")

nmissing = nupdated = not_found = 0
for username, ferry_user in ferry_users.items():
    if username not in db_users:
        auth_info = {} if not ldap_template else {"ldap": ldap_template % (username,)}
        u = BaseDBUser(db, username, ferry_user.get("fullname", ""), None, "", auth_info, ferry_user.get("uuid"))
        nmissing += 1
        if verbose:
            print("Missing user:", u, "  auth_info:", u.AuthInfo)
        if not dry_run:
            u.save()
            print("- created")

for username, db_user in db_users.items():
    ferry_user = ferry_users.get(username)
    if ferry_user:
        do_update = False
        update_reason = ""
        uuid = ferry_user.get("uuid")
        fullname = ferry_user.get("fullname")
        if uuid and uuid != db_user.AUID:
            db_user.AUID = uuid
            do_update = True
            update_reason = " uuid mismatch"
        if fullname and db_user.Name != fullname:
            db_user.Name = fullname
            do_update = True
            update_reason += " name mismatch"
        if do_update:
            nupdated += 1
            if verbose:
                print("User needs to be updated:", db_user, update_reason)
            if not dry_run:
                db_user.save()
                print("- updated")
    else:
        not_found += 1
        if verbose:
            print("User not found in Ferry:", db_user)

print("added:", nmissing, "\nupdated:", nupdated, "\nnot found:", not_found)
