import json
from .authenticators import authenticator
from metacat.common import DBObject, DBManyToMany, password_digest_hash, transactioned
from metacat.util import fetch_generator

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
    
    @transactioned
    def save(self, transaction=None):
        auth_info = json.dumps(self.AuthInfo)
        columns = self.columns()
        transaction.execute(f"""
            insert into users({columns}) values(%s, %s, %s, %s, %s, %s)
                on conflict(username) 
                    do update set name=%s, email=%s, flags=%s, auth_info=%s, auid=%s;
            """,
            (self.Username, self.Name, self.EMail, self.Flags, auth_info, self.AUID,
                            self.Name, self.EMail, self.Flags, auth_info, self.AUID
            ))
        return self

    def authenticate(self, method, auth_config, presented):
        a = authenticator(method, auth_config, self.AuthInfo.get(method))
        #print("DBUser.authenticate: authenticator:", a)
        if a is None or not a.enabled():
            return False, "Authenticated method disabled", None
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
        columns = BaseDBUser.columns("u")
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

    @property
    def roles(self):
        return DBManyToMany(self.DB, "users_roles", "role_name", username = self.Username)
        
    def add_role(self, role):
        self.roles.add(role.Name if isinstance(role, BaseDBRole) else role)

    def remove_role(self, role):
        self.roles.remove(role.Name if isinstance(role, BaseDBRole) else role)


class BaseDBRole(DBObject):

    Table = "roles"
    Columns = ["name", "description"]
    PK = ["name"]

    def __init__(self, db, name, description=None, users=[]):
        DBObject.__init__(self, db)
        self.Name = name
        self.Description = description
            
    def __str__(self):
        return "[BaseDBRole %s %s]" % (self.Name, self.Description)
        
    __repr__ = __str__

    @property
    def members(self):
        return DBManyToMany(self.DB, "users_roles", "username", role_name=self.Name)

    @transactioned
    def save(self, transaction=None):
        transaction.execute("""
            insert into roles(name, description) values(%s, %s)
                on conflict(name) 
                    do update set description=%s
            """,
            (self.Name, self.Description, self.Description))
        return self
        
    @staticmethod
    def get(db, name):
        c = db.cursor()
        c.execute("""select r.description
                        from roles r
                        where r.name=%s
        """, (name,))
        tup = c.fetchone()
        if not tup: return None
        (desc,) = tup
        return BaseDBRole(db, name, desc)
        
    @staticmethod 
    def list(db, user=None):
        c = db.cursor()
        if isinstance(user, BaseDBUser):    user = user.Username
        if user:
            c.execute("""select r.name, r.description
                        from roles r
                            inner join users_roles ur on ur.role_name=r.name
                    where ur.username = %s
                    order by r.name
            """, (user,))
        else:
            c.execute("""select r.name, r.description
                            from roles r
                            order by r.name""")
        
        out = [BaseDBRole(db, name, description) for  name, description in fetch_generator(c)]
        #print("DBRole.list:", out)
        return out
        
    def add_member(self, user):
        self.members.add(user)
        return self
        
    def remove_member(self, user):
        self.members.remove(user)
        return self
        
    def set_members(self, users):
        self.members.set(users)
        return self
        
    def __contains__(self, user):
        if isinstance(user, BaseDBUser):
            user = user.Username
        return user in self.members
        
    def __iter__(self):
        return self.members.__iter__()
