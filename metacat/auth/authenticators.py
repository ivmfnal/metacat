import hashlib
from .py3 import to_str, to_bytes

class Authenticator(object):
    
    def __init__(self, config, db_info):
        self.Config = config
        self.Info = db_info        # User authentication info stored in the DB, e.g. hashed password or X.509 DNs

    def authenticate(self, username, presented):
        # info is the DB representation of the secret, e.g. hashed password
        # presented is the user authentication info presented by the client, e.g. unhashed password
        raise NotImplementedError()
        # returns yes/no, info

    def to_db(self, username, ext_info, **args):
        # convert external representation of the auth info to DB, e.g. hash the password
        return info
        
    def enabled(self):
        return True
        
class DigestAuthenticator(Authenticator):
    
    def __init__(self, config, db_info):
        self.Domain = config["domain"]
        self.GetPassword = config["get_password"]

    def authenticate(self, username, request_env):
        # username not used, comes from the request_env
        return digest_server(self.Domain, request_env, self.GetPassword)

class PasswordAuthenticator(Authenticator):
    
    HashAlgorithm = "sha1"
    
    @staticmethod
    def hash(user, password, alg=None):
        alg = alg or PasswordAuthenticator.HashAlgorithm
        hashed = hashlib.new(alg)
        hashed.update(to_bytes(user))
        hashed.update(b":")
        hashed.update(to_bytes(password))
        return hashed.hexdigest()
        
    def authenticate(self, username, password):
        secret = self.Info          # password as stored in DB
        if not secret:
            return False
        if secret.startswith("$") and ":" in secret:
            alg, hashed_password = secret[1:].split(":", 1)
            try: return hashed_password == self.hash(username, password, alg)
            except: return False, None
        else:
            return secret == password, None   # not hashed
        
    def to_db(self, username, password, alg=None):
        alg = alg or PasswordAuthenticator.HashAlgorithm
        hashed = PasswordAuthenticator.hash(user, password, alg)
        return f"${alg}:{hashed}"
        
    def password_for_digest(self):
        return self.Info

    def enabled(self):
        return self.Info is not None

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
        
    def to_db(self, username, enabled):
        if enabled:
            if config is None or not "dn_template" in config:
                raise ValueError("dn_template is not configured")
            return config["dn_template"] % (username,)
        else:
            return None

    def enabled(self):
        return self.Info is not None or "dn_template" in self.Config
        
class X509Authenticator(Authenticator):
    
    def authenticate(self, username, request_env):
        known_dns = self.Info or []
        subject = request_env.get("SSL_CLIENT_S_DN")
        issuer = request_env.get("SSL_CLIENT_I_DN")
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
