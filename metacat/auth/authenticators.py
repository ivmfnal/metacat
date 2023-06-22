import hashlib, re, traceback
from metacat.util import to_str, to_bytes
from metacat.common import (
    password_digest_hash, 
    SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError
)

class Authenticator(object):
    
    def __init__(self, config, user_info):
        self.Config = config
        self.Info = user_info        # User authentication info stored in the DB, e.g. hashed password or X.509 DNs

    def authenticate(self, user, presented):
        # presented is the user authentication info presented by the client, e.g. unhashed password
        raise NotImplementedError()
        # returns yes/no, reason, expiration

    def enabled(self):
        return True
        
    def update_auth_info(self, *params, **args):
        raise NotImplementedError()
        # returns updated info

class PasswordAuthenticator(Authenticator):
    
    #
    # passwords are stored as MD5("<username>:<realm>:<password>"), suitbale for RFC2617 digest authentication
    #
    
    def __init__(self, realm, db_info):
        #print("PasswordAuthenticator.__init__: realm:", realm)
        self.DBInfo = (db_info or {}).copy()
        self.Realm = realm
        self.DBHashed = self.DBInfo.get(self.Realm)
    
    def authenticate(self, user, password):
        # will accept both hashed and not hashed password
        username = user.Username
        return password == self.DBHashed or self.password_hash(username, password) == self.DBHashed, None, None

    def password_hash(self, username, password):
        hashed = password_digest_hash(self.Realm, username, password).hex().lower()
        #print("PasswordAuthenticator: realm:", self.Realm, "   hashed:", hashed)
        return hashed
        
    def enabled(self):
        return self.DBHashed is not None
        
    def update_auth_info(self, username, password, hashed=False):
        hashed_password = password if hashed else self.password_hash(username, password)
        self.DBInfo[self.Realm] = self.DBHashed = hashed_password
        return self.DBInfo


class LDAPAuthenticator(Authenticator):
    
    def authenticate(self, user, password):
        import ldap
        username = user.Username
        config = self.Config
        if config is None or not "server_url" in config:
            #print("server not configured")
            return False, "LDAP not configured", None
        dn = self.Info             # LDAP DN
        if not dn and "dn_template" in self.Config:
            dn = self.Config["dn_template"] % (username,)
        if not dn:
            #print("no dn")
            return False, "LDAP not configured", None        # not allowed
        ld = ldap.initialize(config["server_url"])
        #print("ldap dn:", dn, "  password:", password)
        try:
            ld.simple_bind_s(dn, password)
            result = True
        except ldap.INVALID_CREDENTIALS:
            result = False
        return result, None, None
        
    def enabled(self):
        return self.Info is not None or "dn_template" in self.Config


class SciTokenAuthenticator(Authenticator):
    
    def authenticate(self, user, encoded):
        #print("SciTokenAuthenticator.authenticate: token:", encoded)
        import scitokens
        #scitokens.configure(cache_location="__memory__")
        issuers = self.Config
        subject = issuer = expiration = None
        
        if False:
            try:    
                token = SignedToken.from_bytes(encoded)
                expiration = token.expiration
            except Exception as e:
                token = None
                return False, None, expiration

        try:
            token = scitokens.SciToken.deserialize(encoded)
            #print("token:", token)
            subject = token["sub"]
            issuer = token["iss"]
            expiration = token["exp"]
        except Exception as e:
            #print("SciTokenAuthenticator.authenticate: error:", e, type(e))
            traceback.print_exc()
            subject = None

        #print("SciTokenAuthenticator.authenticate:", subject, issuer)

        return (
            subject and issuer
                and issuer in issuers
                and subject in (user.Username, user.AUID),
            None, expiration
        )

class SignedTokenAuthenticator(Authenticator):
    
    def authenticate(self, user, encoded):
        issuer = self.Config["issuer"]
        secret = self.Config["secret"]
        token = expiration = None
        try:    
            token = SignedToken.from_bytes(encoded)
            token.verify(secret)
            expiration = token.expiration
        except (
                    SignedTokenExpiredError, 
                    SignedTokenImmatureError,
                    SignedTokenUnacceptedAlgorithmError, 
                    SignedTokenSignatureVerificationError
                ):
            token = None

        return (
            token is not None \
                and token.get("iss") == issuer \
                and token.get("sub") == user.Username, 
            None, expiration
        )

class DN(object):

    def __init__(self, inp):
        self.Text = inp
        self.Fields = self.parse(inp)

    def __str__(self):
        return f"DN({self.Text})"

    __repr__ = __str__

    RFCRE=re.compile("(\s*,\s*)?([A-Z]+)=")
    LegacyRE=re.compile("\s*/([A-Z]+)=")
    
    def items(self):
        yield from sorted(self.Fields.items())
    
    __iter__ = items

    @staticmethod
    def parse(text):
        fields = {}
        if text.startswith('/'):
            parts = DN.LegacyRE.split(text)[1:]
            pairs = [(parts[i], parts[i+1]) for i in range(0, len(parts), 2)]
        else:
            parts = DN.RFCRE.split(text)[1:]
            pairs = [(parts[i+1], parts[i+2]) for i in range(0, len(parts), 3)]
        for name, value in pairs:
            name = name.upper()
            if name == "CN":
                # if this is numric CN, e.g. CN=12345, ignore it
                try:    _=int(value)
                except: pass
                else:   continue        # if it's an integer, skip it
            fields.setdefault(name, []).append(value)
        return {name: sorted(lst) for name, lst in fields.items()}

    def __eq__(self, other):
        return self.Fields == other.Fields

    def __ge__(self, other):
        for name, lst in self.Fields.items():
            lst1 = other.Fields.get(name, [])
            if any(v not in lst for v in lst1):
                return False
        else:
            return True

    def __le__(self, other):
        return other >= self

    def legacy(self):
        out = []
        for name, values in self:
            for value in values:
                out.append(f"/{name}={value}")
        return ''.join(out)

    def rfc(self):
        out = []
        for name, values in self:
            for value in values:
                out.append(f"{name}={value}")
        return ','.join(out)


class X509Authenticator(Authenticator):
    
    def authenticate(self, user, request_env):
        known_dns = self.Info or []
        #log = open("/tmp/x509.log", "w")
        #print("known_dns:", known_dns, file=log)
        subject = request_env.get("SSL_CLIENT_S_DN")
        issuer = request_env.get("SSL_CLIENT_I_DN")
        
        if not subject or not issuer or not known_dns:
            return False, "SSL info not found", None

        subject = DN(subject)
        issuer = DN(issuer)
        known_dns = [DN(dn) for dn in known_dns]

        #print("subject:", subject)
        #print("issuer:", issuer)
        #print("subject in known_dns:", subject in known_dns, file=log)
        #print("issuer in known_dns:", issuer in known_dns, file=log)
        #print("issuer in subject:", issuer in subject, file=log)
        return (
            any(subject == dn for dn in known_dns) or                     # cert
            any(issuer == dn for dn in known_dns) and issuer <= subject   # proxy
        ), None, None
        
    def enabled(self):
        return not not self.Info

def authenticator(method, config, user_info):
    #print("authenticator(): method:", method)
    if method == "password":        a = PasswordAuthenticator(config, user_info)
    elif method == "x509":          a = X509Authenticator(config, user_info)
    elif method == "ldap":          a = LDAPAuthenticator(config, user_info)
    elif method == "scitoken":      a = SciTokenAuthenticator(config, user_info)
    elif method == "jwttoken":      a = SignedTokenAuthenticator(config, user_info)
    else:
        raise ValueError(f"Unknown autenticator type {method}")
    return a
    