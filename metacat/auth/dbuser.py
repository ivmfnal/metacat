from .authenticators import authenticator

class DBUser(object):

    def __init__(self, db, username, name, email, flags=""):
        self.Username = username
        self.Name = name
        self.EMail = email
        self.Flags = flags
        self.DB = db
        self.AuthInfo = {}        # type -> [secret,...]        # DB representation
        self.RoleNames = None
        
    def __str__(self):
        return "DBUser(%s, %s, %s, %s)" % (self.Username, self.Name, self.EMail, self.Flags)
        
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
        
    def authenticate(self, method, config, presented):
        a = authenticator(method, config, self.AuthInfo.get(method))
        return a is not None and a.enabled() and a.authenticate(self.Username, presented)
        
    def set_auth_info(self, method, info):  
        # info is in DB representation, e.g. unhashed password
        self.AuthInfo[method] = info
    
    def set_password(self, password):
        # for compatibility, password is DB representation, e.g. hashed
        self.set_auth_info("password", password)
    
    def auth_info(self, method):
        return self.AuthInfo.get(method)

    def auth_method_enabled(self, method, config):
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
        u = DBUser(db, username, name, email, flags)
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
            u = DBUser(db, username, name, email, flags)
            u.RoleNames = roles
            #print("DBUser.list: yielding:", u)
            yield u
