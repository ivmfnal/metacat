import sys, getopt, os, json, pickle, time
from urllib.request import urlopen, Request
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_bytes, to_str
from metacat.auth import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, TokenLib
from metacat.webapi import MetaCatClient, AuthenticationError
import getpass
from metacat.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments

Usage = """
Usage: 

    metacat [-a <authentication server url>] <subcommand> ...
    
    subommands and options:
    
        login [-m <mechanism>] <username>                   - request authentication token
            mechanisms are: password, x509
            with x509, use:
                -c <cert or proxy file> [-k <private key file>]
                   X509_USER_PROXY, X509_USER_CERT, X509_USER_KEY environment variables are supported too
        whoami [-t <token file>]                            - verify and show token
        mydn [-i] -c <cert or proxy file> [-k <private key file>]  - print my X.509 subject DN
                -i prints the issuer DN instead
        list                                                - list tokens
        export [-o <token file>] [<token id>|<server url>]  - export token by token id or service URL
        import [-i <token file>] [-s <server url>]          - import token and use it for new server
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

class ListCommand(CLICommand):
    
    Usage="- list tokens"

    def __call__(self, command, client, opts, args):
        tl = client.TokenLib
        #for url, token in tl.items():
        #    print(url, token)
        now = time.time()
        lst = [(token.tid, url, token.subject or "", time.ctime(token.expiration), time_delta(token.expiration - now)) 
                            for url, token in tl.items()
                            if token.expiration is None or token.expiration > time.time()]
        max_tid, max_url, max_user, max_exp = len("Token id"), len("Server URL"), len("User"), len("Expiration")
        for tid, url, user, et, delta in lst:
            #print(tid, url, user, et, delta)
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

class WhoAmICommand(CLICommand):
    
    Opts = "t: token-file="
    Usage = "[-t|--token-file <token file>]"
     
    def __call__(self, command, client, opts, args):
        opts, args = getopt.getopt(args, "t:")
        opts = dict(opts)
        user = None
        if "-t" in opts or "--token-file" in opts:
            token = SignedToken.from_bytes(open(opts.get("-t", opts.get("--token-file")), "rb").read())
            user, expiration = token.subject, token.expiration
        else:
            try:    user, expiration = client.auth_info()
            except AuthenticationError as e:
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
    
class MyDNCommand(CLICommand):

    Opts = "c:k:i cert= key= issuer"
    Usage="""[-i|--issuer] -c|--cert <cert or proxy file> [-k|--key <private key file>]  - print my X.509 subject DN
        -i|--issuer     -   prints the issuer DN instead
    """

    def __call__(self, command, client, opts, args):
        cert, key = get_x509_cert_key(opts)
        names = client.my_x509_dn(cert, key)
        print_issuer = "-i" in opts or "--issuer" in opts
        print (names.get("issuer" if print_issuer else "subject", "certificate not recognized"))

class LoginCommand(CLICommand):
    
    Opts = "m:c:k: method= cert= key="
    Usage = """[-m|--method <method>] <username>                   - request authentication token
        methods are: password, x509
        with x509, use:
            -c|--cert <cert or proxy file> [-k|--key <private key file>]
            X509_USER_PROXY, X509_USER_CERT, X509_USER_KEY environment variables are supported.
    """

    def __call__(self, command, client, opts, args):
        opts, args = getopt.getopt(args, "m:c:k:d")
        if not args:
            print(Usage)
            sys.exit(2)
        opts = dict(opts)
        mechanism = opts.get("-m", "password")
        try:
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
        except AuthenticationError:
            print("Authentication failed")
            sys.exit(1)
        print ("User:   ", user)
        print ("Expires:", time.ctime(expiration))
    
class ExportCommand(CLICommand):
    
    Usage = """[-o|--out <token file>] [<token id>|<server url>]  - export token by token id or service URL
        if no output is specified, exports to stdout
    """
    Opts = "o: out="
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        if not args:
            token = client.Token
            if token is None:
                print(Usage)
                sys.exit(2)
        else:
            tl = client.TokenLib
            token_id_or_url = args[0]
            for url, token in tl.items():
                if token_id_or_url in (url, token.tid):
                    break
            else:
                print("Token not found", file=sys.stderr)
                sys.exit(1)

        out_path = opts.get("-o", opts.get("--out"))
        if out_path:
            fd = os.open(out_path, os.O_CREAT + os.O_WRONLY + os.O_TRUNC, 0o700)
            out_file = os.fdopen(fd, "wb")
            out_file.write(token.encode())
            out_file.close()
            os.chmod(out_path, 0o700)
        else:
            print(to_str(token.encode()))
        
class ImportCommand(CLICommand):
    
    Usage = """[-i|--input <token file>] [-s|--server <server url>]          - import token and use it for new server
        if no input is specified, import from stdin
    """
    Opts = "i:s: input= server="
    
    def __call__(self, command, client, opts, args):
        target_server_url = opts.get("-s", opts.get("--server", client.ServerURL))
        inp_file = opts.get("-i", opts.get("--input"))
        inp_file = open(inp_file, "r") if inp_file else sys.stdin
        token = inp_file.read().strip()
    
        tl = client.TokenLib
        tl[target_server_url] = token
    
AuthCLI = CLI(
    "whoami",   WhoAmICommand(),
    "login",    LoginCommand(),
    "mydn",     MyDNCommand(),
    "list",     ListCommand(),
    "export",   ExportCommand(),
    "import",   ImportCommand()
)