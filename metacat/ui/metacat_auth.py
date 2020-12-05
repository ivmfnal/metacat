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
            Currently, only "password" mechanism is implemented
        whoami                                          - verify token
        list                                            - list tokens
"""

def do_list(client, args):
    tl = client.TL
    for url, token in tl.items():
            print("%s %s %s %s" % (token.TID, url, token["user"], time.ctime(token.Expiration)))

def do_whoami(client, args):
    try:    user, expiration = client.auth_info()
    except MCAuthenticationError as e:
        print(e)
    else:
        print ("User:   ", user)
        print ("Expires:", time.ctime(expiration))

def do_login(client, args):
    username = args[0]
    password = getpass.getpass("Password:")
    user, expiration = client.login_password(username, password)
    print ("User:   ", user)
    print ("Expires:", time.ctime(expiration))
    
def do_auth(server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)
        
    client = MetaCatClient(server_url)
    command = args[0]
    return {
        "list":         do_list,
        "login":        do_login,
        "whoami":       do_whoami
    }[command](client, args[1:])

