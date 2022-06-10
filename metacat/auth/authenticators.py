import hashlib
from .py3 import to_str, to_bytes
from .password_hash import password_digest_hash

class Authenticator(object):
    
    def __init__(self, config, db_info):
        self.Config = config
        self.Info = db_info        # User authentication info stored in the DB, e.g. hashed password or X.509 DNs

    def authenticate(self, username, presented):
        # info is the DB representation of the secret, e.g. hashed password
        # presented is the user authentication info presented by the client, e.g. unhashed password
        raise NotImplementedError()
        # returns yes/no, info

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
        self.DBInfo = (db_info or {}).copy()
        self.Realm = realm
        self.DBHashed = self.DBInfo.get(self.Realm)
    
    def authenticate(self, username, password):
        # will accept both hashed and not hashed password
        return password == self.DBHashed or self.password_hash(username, password) == self.DBHashed, None

    def password_hash(self, username, password):
        return password_digest_hash(self.Realm, username, password).hex().lower()
        
    def enabled(self):
        return self.DBHashed is not None
        
    def update_auth_info(self, username, password, hashed=False):
        hashed_password = password if hashed else self.password_hash(username, password)
        self.DBInfo[self.Realm] = self.DBHashed = hashed_password
        return self.DBInfo

class LDAPAuthenticator(Authenticator):
    
    def authenticate(self, username, password):
        import ldap
        config = self.Config
        if config is None or not "server_url" in config:
            #print("server not configured")
            return False
        dn = self.Info             # LDAP DN
        if not dn and "dn_template" in self.Config:
            dn = self.Config["dn_template"] % (username,)
        if not dn:
            #print("no dn")
            return False        # not allowed

        ld = ldap.initialize(config["server_url"])
        #print("ldap password:", password)
        try:
            ld.simple_bind_s(dn, password)
            result = True
        except ldap.INVALID_CREDENTIALS:
            result = False
        #print("ldap:", result)
        return result, None
        
    def enabled(self):
        return self.Info is not None or "dn_template" in self.Config
        
class X509Authenticator(Authenticator):
    
    def authenticate(self, username, request_env):
        known_dns = self.Info or []
        #log = open("/tmp/x509.log", "w")
        #print("known_dns:", known_dns, file=log)
        subject = request_env.get("SSL_CLIENT_S_DN")
        issuer = request_env.get("SSL_CLIENT_I_DN")
        #print("subject:", subject, file=log)
        #print("issuer:", issuer, file=log)
        #print("subject in known_dns:", subject in known_dns, file=log)
        #print("issuer in known_dns:", issuer in known_dns, file=log)
        #print("issuer in subject:", issuer in subject, file=log)
        return (
            subject in known_dns or                     # cert
            issuer in known_dns and issuer in subject   # proxy
        ), None
        
    def enabled(self):
        return not not self.Info

def authenticator(method, config, user_info):
    if method == "password":   a = PasswordAuthenticator(config, user_info)
    elif method == "x509":     a = X509Authenticator(config, user_info)
    elif method == "ldap":     a = LDAPAuthenticator(config, user_info)
    else:
        raise ValueError(f"Unknown autenticator type {method}")
    return a
    