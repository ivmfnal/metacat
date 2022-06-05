from .authenticators import authenticator
from .password_hash import password_digest_hash
import json

class BaseDBUser(object):

    def __init__(self, db, username, name, email, flags=""):
        self.Username = username
        self.Name = name
        self.EMail = email
        self.Flags = flags
        self.DB = db
        self.AuthInfo = {}        # type -> [secret,...]        # DB representation
        self.RoleNames = None
        
    def __str__(self):
        return "BaseDBUser(%s, %s, %s, %s)" % (self.Username, self.Name, self.EMail, self.Flags)
        
    __repr__ = __str__
    
    def save(self, do_commit=True):
        c = self.DB.cursor()
        auth_info = json.dumps(self.AuthInfo)
        c.execute("""
            insert into users(username, name, email, flags, auth_info) values(%s, %s, %s, %s, %s)
                on conflict(username) 
                    do update set name=%s, email=%s, flags=%s, auth_info=%s;
            """,
            (self.Username, self.Name, self.EMail, self.Flags, auth_info,
                            self.Name, self.EMail, self.Flags, auth_info
            ))
        
        if do_commit:
            c.execute("commit")
        return self
        
    def authenticate(self, method, auth_config, presented):
        a = authenticator(method, auth_config, self.AuthInfo.get(method))
        if a is None or not a.enabled():
            return False
        result, reason = a.authenticate(self.Username, presented)
        #print(f"BaseDBUser.authenticate({method}):", result)
        return result
        
    def set_password(self, realm, password, hashed=False):
        auth_info = self.AuthInfo.get("password", {})
        a = authenticator("password", realm, auth_info)
        self.AuthInfo["password"] = a.update_auth_info(self.Username, password, hashed=hashed)
        
    def get_password(self, realm):
        #print("DBUser.get_password(", realm, "): info:", self.AuthInfo)
        return self.AuthInfo.get("password", {}).get(realm)
        
    def get_dns(self):
        return self.AuthInfo.get("x509", [])

    def set_dns(self, dn_list):
        self.AuthInfo["x509"] = dn_list[:]

    def auth_method_enabled(self, method):
        return method in self.AuthInfo
        
    def ___auth_method_enabled(self, method, config):
        a = authenticator(method, config, self.AuthInfo.get(method))
        return a is not None and a.enabled()
        #return self.authenticator(method).enabled()

    @staticmethod
    def get(db, username):
        c = db.cursor()
        c.execute("""select u.name, u.email, u.flags, u.auth_info, array(select ur.role_name from users_roles ur where ur.username=u.username)
                        from users u
                        where u.username=%s""",
                (username,))
        tup = c.fetchone()
        if not tup: return None
        (name, email, flags, auth_info, roles) = tup
        u = BaseDBUser(db, username, name, email, flags)
        u.AuthInfo = auth_info
        u.RoleNames = roles
        return u
        
    def is_admin(self):
        return "a" in (self.Flags or "")
    
    @staticmethod 
    def list(db):
        c = db.cursor()
        c.execute("""select u.username, u.name, u.email, u.flags, array(select ur.role_name from users_roles ur where ur.username=u.username)
            from users u
        """)
        for username, name, email, flags, roles in c.fetchall():
            u = BaseDBUser(db, username, name, email, flags)
            u.RoleNames = roles
            #print("DBUser.list: yielding:", u)
            yield u
