import sys, getopt, os, json, pickle, time
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str, SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, TokenLib
from metacat.webapi import MetaCatClient, MCAuthenticationError
import getpass

Usage = """
Usage: 
    metacat auth subommands and options:
    
        login [-m <mechanism>] <username>               - request authentication token
            Currently, only "digest" mechanism is implemented
        whoami                                          - verify token
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
    tl = client.TL
    now = time.time()
    lst = [(token.TID, url, token["user"], time.ctime(token.Expiration), time_delta(token.Expiration - now)) for url, token in tl.items()
                if token.Expiration > now]
    max_tid, max_url, max_user, max_exp = len("Token id"), len("Server URL"), len("User"), len("Expiration")
    for tid, url, user, et, delta in lst:
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
    try:    user, expiration = client.auth_info()
    except MCAuthenticationError as e:
        print(e)
    else:
        print ("User:   ", user)
        print ("Expires:", time.ctime(expiration))

def do_login(client, args):
    opts, args = getopt.getopt(args, "m:")
    opts = dict(opts)
    mechanism = opts.get("-m", "digest")
    if mechanism == "password": mechanism = "digest"
    username = args[0]
    password = getpass.getpass("Password:")
    if mechanism == "ldap":
        user, expiration = client.login_ldap(username, password)
    elif mechanism == "digest":
        user, expiration = client.login_password(username, password)
    else:
        print(f"Unknown authentication mechanism {mechanism}")
        sys.exit(2)
    print ("User:   ", user)
    print ("Expires:", time.ctime(expiration))
    
def do_auth(server_url, auth_server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)
        
    client = MetaCatClient(server_url, auth_server_url)
    command = args[0]
    return {
        "list":         do_list,
        "login":        do_login,
        "whoami":       do_whoami
    }[command](client, args[1:])

