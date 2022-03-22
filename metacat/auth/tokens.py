from .token_lib import TokenLib
from .signed_token_jwt import SignedToken
from .auth_client import TokenAuthClientMixin
import sys, getopt, time

class Client(TokenAuthClientMixin):
    
    def __init__(self, server_url, auth_server_url):
        TokenAuthClientMixin.__init__(self, server_url, None, None, auth_url=auth_server_url)

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
    tl = TokenLib()
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
        
if __name__ == "__main__":
    do_list(None, [])