import sys, getopt, os, json, pickle, time
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str, SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, TokenLib
from metacat.webapi import MetaCatClient, MCAuthenticationError
import getpass

Usage = """
Usage: 

    metacat [-a <authentication server url>] <subcommand> ...
    
    subommands and options:
    
        login [-m <mechanism>] <username>               - request authentication token
            mechanisms are: password, x509
            with x509, use:
                -c <cert or proxy file> [-k <private key file>]
                   X509_USER_PROXY, X509_USER_CERT, X509_USER_KEY environment variables are supported too
        whoami [-t <token file>]                        - verify and show token
        mydn [-i] -c <cert or proxy file> [-k <private key file>]  - print my X.509 subject DN
                -i prints the issuer DN instead
        export [-o <token file>]                        - export token
        import [-i <token file>] [-s <server_url>]      - import token and use it for new server
        list                                            - list tokens
"""

def time_delta(dt):
    dt = int(dt)
    if dt < 0:
        return "-"
    if dt <= 60:
        return "%ds" % (dt,)
    elif dt <= 3600:
        m = dt//60
        s = dt%60
        return "%dm%ds" % (m, s)
    elif dt <= 24*3600:
        h = dt//3600
        m = (dt//60)%60
        return "%dh%dm" % (h, m)
    else:
        d = dt//(3600*24)
        h = (dt % (3600*24))/3600
        return "%dd%dh" % (d, h)

def do_list(client, args):
    tl = client.TokenLib
    for url, token in tl.items():
        print(url, token)
    now = time.time()
    lst = [(token.tid, url, token.subject or "", time.ctime(token.expiration), time_delta(token.expiration - now)) 
                        for url, token in tl.items()
                        if token.expiration is None or token.expiration > time.time()]
    max_tid, max_url, max_user, max_exp = len("Token id"), len("Server URL"), len("User"), len("Expiration")
    for tid, url, user, et, delta in lst:
        print(tid, url, user, et, delta)
        max_tid = max(len(tid), max_tid)
        max_url = max(len(url), max_url)
        max_user = max(len(user), max_user)
        max_exp = max(len(et)+len(delta)+1+2, max_exp)
    
    format = f"%-{max_tid}s %-{max_url}s %-{max_user}s %s"
    print(format % ("Token id", "Server URL", "User", "Expiration"))
    print("-"*max_tid, "-"*max_url, "-"*max_user, "-"*max_exp)
    for tid, url, user, et, delta in lst:
        exp = "%s (%s)" % (et, delta)
        print(format % (tid, url, user, exp))

def do_whoami(client, args):
    opts, args = getopt.getopt(args, "t:")
    opts = dict(opts)
    user = None
    if "-t" in opts:
        token = SignedToken.from_bytes(open(opts["-t"], "rb").read())
        user, expiration = token.subject, token.expiration
    else:
        try:    user, expiration = client.auth_info()
        except MCAuthenticationError as e:
            print(e)
    if user:
        print ("User:   ", user)
        print ("Expires:", time.ctime(expiration))
        
def get_x509_cert_key(opts):
    cert = opts.get("-c") or os.environ.get("X509_USER_PROXY") or os.environ.get("X509_USER_CERT")
    key = opts.get("-k") or os.environ.get("X509_USER_KEY") or cert
    if not cert:
        print("X.509 certificate file is unspecified.\n")
        print("  Use -c <cert file> or set env. variable X509_USER_PROXY or X509_USER_CERT")
        print(Usage)
        sys.exit(2)
    return cert, key

def do_mydn(client, args):
    opts, args = getopt.getopt(args, "ik:c:")
    opts = dict(opts)
    cert, key = get_x509_cert_key(opts)
    names = client.my_x509_dn(cert, key)
    print (names.get("issuer" if "-i" in opts else "subject", "not recognized"))

def do_login(client, args):
    opts, args = getopt.getopt(args, "m:c:k:d")
    opts = dict(opts)
    mechanism = opts.get("-m", "password")
    if mechanism == "password":
        username = args[0]
        password = getpass.getpass("Password:")
        user, expiration = client.login_password(username, password)
    elif mechanism == "x509":
        cert, key = get_x509_cert_key(opts)
        username = args[0]
        user, expiration = client.login_x509(username, cert, key=key)
    else:
        print(f"Unknown authentication mechanism {mechanism}")
        sys.exit(2)
    print ("User:   ", user)
    print ("Expires:", time.ctime(expiration))
    
def do_export(client, args):
    opts, args = getopt.getopt(args, "o:")
    opts = dict(opts)
    
    token = client.Token
    if token is None:
        sys.stderr.write("Token not found\n")
        sys.exit(1)

    out_path = opts.get("-o")
    if out_path:
        fd = os.open(out_path, os.O_CREAT + os.O_WRONLY + os.O_TRUNC, 0o700)
        out_file = os.fdopen(fd, "wb")
        out_file.write(token.encode())
        out_file.close()
        os.chmod(out_path, 0o700)
    else:
        print(to_str(token.encode()))
        
def do_import(client, args):
    opts, args = getopt.getopt(args, "i:s:")
    opts = dict(opts)

    target_server_url = opts.get("-s", client.ServerURL)
    inp_file = open(opts["-i"], "r") if "-i" in opts else sys.stdin
    token = inp_file.read().strip()
    
    tl = client.TokenLib
    tl[target_server_url] = token
    
def do_auth(server_url, auth_server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)
        
    client = MetaCatClient(server_url, auth_server_url)
    command = args[0]
    return {
        "list":         do_list,
        "login":        do_login,
        "whoami":       do_whoami,
        "mydn":         do_mydn,
        "export":       do_export,
        "import":       do_import
    }[command](client, args[1:])

