import hashlib
from metacat.util import to_str, to_bytes

class Authenticator(object):
    
    def __init__(self, username, info=None):
        self.Username = username
        self.Info = info        # DB representation
    
    def authenticate(self, config, secret):
        raise NotImplementedError()
        
    def set_info(self, config, info):
        # format info to be stored in the DB, e.g. hash the password
        self.Info = info
        return self.Info
        
    def enabled(self):
        return not not self.Info
        
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
        
    def authenticate(self, config, password):
        secret = self.Info
        if not secret:
            return False
        if secret.startswith("$") and ":" in secret:
            alg, hashed_password = secret[1:].split(":", 1)
            try: return hashed_password == self.hash(self.Username, password, alg)
            except: return False
        else:
            return secret == password   # not hashed
        
    @staticmethod
    def make_password_for_digest(user, password, alg=None):
        alg = alg or PasswordAuthenticator.HashAlgorithm
        hashed = PasswordAuthenticator.hash(user, password, alg)
        return f"${alg}:{hashed}"
        
    def password_for_digest(self):
        return self.Info

    def set_info(self, _, password):
        self.Info = self.make_password_for_digest(self.Username, password)
        return self.Info
        
class LDAPAuthenticator(Authenticator):
    
    def authenticate(self, config, password):
        import ldap
        if config is None or not "server_url" in config:
            print("server not configured")
            return False
        dn = self.Info
        if not dn:
            print("no dn")
            return False        # not allowed

        ld = ldap.initialize(config["server_url"])
        try:
            ld.simple_bind_s(dn, password)
            result = True
        except ldap.INVALID_CREDENTIALS:
            result = False
        print("ldap:", result)
        return result
        
        
    def set_info(self, config, enabled):
        if enabled:
            if config is None or not "dn_template" in config:
                raise ValueError("dn_template is not configured")
            self.Info = config["dn_template"] % (self.Username,)
        else:
            self.Info = None
        return self.Info
        
class X509Authenticator(Authenticator):
    
    def authenticate(self, config, dn):
        return dn in (self.Info or [])
        
def authenticator(username, method, info=None):
    if method == "password":   a = PasswordAuthenticator(username, info)
    elif method == "x509":   a = X509Authenticator(username, info)
    elif method == "ldap":   a = LDAPAuthenticator(username, info)
    else:
        raise ValueError(f"Unknown autenticator type {method}")
    return a
