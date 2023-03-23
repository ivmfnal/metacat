from .authenticators import authenticator
from .password_hash import password_digest_hash
import json

def fetch_generator(c):
    while True:
        tup = c.fetchone()
        if tup is None: break
        yield tup

class DBObject(object):
    
    def __init__(self, db):
        self.DB = db

    @classmethod
    def from_tuple(cls, db, dbtup):
        h = cls(db, *dbtup)
        return h
    
    @classmethod
    def columns(cls, table_name=None, as_text=True, exclude=[]):
        if isinstance(exclude, str):
            exclude = [exclude]
        clist = [c for c in cls.Columns if c not in exclude]
        if table_name:
            clist = [table_name+"."+cn for cn in clist]
        if as_text:
            return ",".join(clist)
        else:
            return clist

    @classmethod
    def get(cls, db, *pk_vals):
        pk_cols_values = [f"{c} = %s" for c in cls.PK]
        where = " and ".join(pk_cols_values)
        cols = ",".join(cls.Columns)
        c = db.cursor()
        c.execute(f"select {cols} from {cls.Table} where {where}", pk_vals)
        tup = c.fetchone()
        if tup is None: return None
        else:   return cls.from_tuple(db, tup)


class DBAuthenticator(DBObject):
    
    Table = "authenticators"
    Columns = ["username", "type", "issuer", "user_info"]
    PK = ["username", "type", "issuer"]
    
    def __init__(self, db, username, type, issuer, info):
        DBObject.__init__(self, db)
        self.Username = username
        self.Type = type
        self.Issuer = issuer
        self.Info = info

    @staticmethod
    def list(db, username=None, type=None, issuer=None):
        table = DBAuthenticator.Table
        c = db.cursor()
        columns = DBAuthenticator.columns()
        c.execute(f"""
            select {columns}
                from {table}
                where (%(username)s is null or username=%(username)s)
                    and (%(type)s is null or username=%(type)s)
                    and (%(issuer)s is null or username=%(issuer)s)
        """, dict(username=username, type=type, issuer=issuer))
        return (DBAuthenticator.from_tuple(db, tup) for tup in fetch_generator(c))


class BaseDBUser(DBObject):
    
    Table = "users"
    Columns = "username,name,email,flags,auth_info,auid".split(",")
    PK = ["username"]

    def __init__(self, db, username, name, email, flags, auth_info, auid):
        DBObject.__init__(self, db)
        self.Username = username
        self.Name = name
        self.AUID = auid
        self.EMail = email
        self.Flags = flags
        self.AuthInfo = auth_info or {}        # type -> [secret,...]        # DB representation
        self.RoleNames = None
        
    def __str__(self):
        return "BaseDBUser(%s, %s, %s, %s, %s)" % (self.Username, self.Name, self.EMail, self.Flags, self.AUID)
        
    __repr__ = __str__
    
    def save(self, do_commit=True):
        c = self.DB.cursor()
        auth_info = json.dumps(self.AuthInfo)
        columns = self.columns("u")
        c.execute(f"""
            insert into users({columns}) values(%s, %s, %s, %s, %s, %s)
                on conflict(username) 
                    do update set name=%s, email=%s, flags=%s, auth_info=%s, auid=%s;
            """,
            (self.Username, self.Name, self.EMail, self.Flags, auth_info, self.AUID,
                            self.Name, self.EMail, self.Flags, auth_info, self.AUID
            ))
        
        if do_commit:
            c.execute("commit")
        return self

    def authenticate(self, method, auth_config, presented):
        a = authenticator(method, auth_config, self.AuthInfo.get(method))
        if a is None or not a.enabled():
            return False
        result, reason, expiration = a.authenticate(self, presented)
        #print(f"BaseDBUser.authenticate({method}):", result)
        return result, reason, expiration
        
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
        columns = BaseDBUser.columns("u")
        c.execute(f"""select {columns}, array(select ur.role_name from users_roles ur where ur.username=u.username)
                        from users u
                        where u.username=%s""",
                (username,))
        tup = c.fetchone()
        if not tup: return None
        (username, name, email, flags, auth_info, auid, roles) = tup
        u = BaseDBUser(db, username, name, email, flags, auth_info, auid)
        u.RoleNames = roles
        return u
        
    def is_admin(self):
        return "a" in (self.Flags or "")
    
    @staticmethod 
    def list(db):
        columns = self.columns("u")
        c = db.cursor()
        c.execute(f"""select {columns}, array(select ur.role_name from users_roles ur where ur.username=u.username)
            from users u
            order by u.username
        """)
        for username, name, email, flags, auth_info, auid, roles in c.fetchall():
            u = BaseDBUser(db, username, name, email, flags, auth_info, auid)
            u.RoleNames = roles
            #print("DBUser.list: yielding:", u)
            yield u
