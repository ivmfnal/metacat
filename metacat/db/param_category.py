from .common import DBObject, fetch_generator

class DBParamCategory(DBObject):
    
    ColumnsText = "path,owner_user,owner_role,description,restricted,definitions,creator,created_timestamp"
    Columns = ColumnsText.split(",")

    """
        Definitions is JSON with the following structure:

        {
            "name": {
                "type":         'int','double','text','boolean',
                                        'int[]','double[]','text[]','boolean[]', 'dict', 'list', 'any'
                "values":       [ v1, v2, ...]      optional, ignored for boolean
                "min":          min value           optional, ignored if "values" present, ignored for boolean
                "max":          max value           optional, ignored if "values" present, ignored for boolean
            }
        }
    """

    Types =  ('int','float','text','boolean',
                'int[]','float[]','text[]','boolean[]','dict', 'list', 'any')


    def __init__(self, db, path, restricted=False, owner_role=None, owner_user=None, creator=None, definitions={}, description="", created_timestamp=None):
        self.Path = path
        self.DB = db
        self.OwnerUser = owner_user
        self.OwnerRole = owner_role
        self.Description = description
        self.Restricted = restricted
        self.Definitions = definitions         
        self.Creator = creator 
        self.CreatedTimestamp = created_timestamp
        
    def owners(self, directly=False):
        if self.OwnerUser is not None:
            return [self.OwnerUser]
        elif not directly and self.OwnerRole is not None:
            r = self.OwnerRole
            if isinstance(r, str):
                r = DBRole(self.DB, r)
            return r.members
        else:
            return []
            
    @staticmethod
    def list(db, parent=None):
        c = db.cursor()
        columns = DBParamCategory.columns()
        if parent:
            c.execute(f"""
                select {columns}
                    from parameter_categories
                    where path like '{parent}.%'
                    order by path
            """)
        else:
            c.execute(f"""
                select {columns}
                    from parameter_categories
                    order by path
            """)
        return (DBParamCategory.from_tuple(db, tup) for tup in fetch_generator(c))

    def owned_by_user(self, user, directly=False):
        if isinstance(user, DBUser):   user = user.Username
        return user in self.owners(directly)

    def owned_by_role(self, role):
        if isinstance(role, DBRole):   role = role.name
        return self.OwnerRole == role

    def save(self, do_commit=True):
        c = self.DB.cursor()
        defs = json.dumps(self.Definitions)
        print("db save:", self.OwnerUser, self.OwnerRole)
        columns = self.columns()
        c.execute(f"""
            insert into parameter_categories({columns}) 
                values(%(path)s, %(owner_user)s, %(owner_role)s, %(description)s, %(restricted)s, %(defs)s, %(creator)s, now())
                on conflict(path) 
                    do update 
                        set owner_user=%(owner_user)s, owner_role=%(owner_role)s, restricted=%(restricted)s, 
                        definitions=%(defs)s, description=%(description)s, creator=%(creator)s
            """,
            dict(path=self.Path, owner_user=self.OwnerUser, owner_role=self.OwnerRole, restricted=self.Restricted, defs=defs,
                    description=self.Description, creator=self.Creator))
        if do_commit:
            c.execute("commit")
        return self
    
    @staticmethod
    def from_tuple(db, tup):
        if tup is None: return None
        path, owner_user, owner_role, description, restricted, definitions, creator, created_timestamp = tup
        return DBParamCategory(db, path, owner_user=owner_user, owner_role=owner_role, description=description, 
                restricted=restricted, definitions=definitions, creator=creator, created_timestamp=created_timestamp)
        
    @staticmethod
    def get(db, path):
        c = db.cursor()
        columns = DBParamCategory.columns()
        c.execute(f"""
            select {columns}
                from parameter_categories where path=%s
                """, (path,)
        )
        tup = c.fetchone()
        return DBParamCategory.from_tuple(db, tup)

    @staticmethod
    def get_many(db, paths):
        c = db.cursor()
        columns = DBParamCategory.columns()
        c.execute(f"""
            select {columns}
                from parameter_categories where path=any(%s)
                """, (list(paths),)
        )
        return (DBParamCategory.from_tuple(db, tup) for tup in fetch_generator(c))

    @staticmethod
    def exists(db, path):
        return DBParamCategory.get(db, path) != None

    @staticmethod
    def category_for_path(db, path):
        # get the deepest category containing the path
        words = path.split(".")
        p = []
        paths = ['.']
        for w in words:
            if w:
                p.append(w)
                paths.append(".".join(p))
                
        c = db.cursor()
        columns = DBParamCategory.columns()
        c.execute(f"""
            select {columns}
                from parameter_categories where path = any(%s)
                order by path desc limit 1""", (paths,)
        )
        tup = c.fetchone()
        return DBParamCategory.from_tuple(db, tup)

    def validate_parameter(self, name, value):
        if not name in self.Definitions:    
            if self.Restricted:
                return False, f"Restricted category"
            else:
                return True, "Unrestricted"
        definition = self.Definitions[name]
        typ = definition["type"]

        if typ == "any":    return True, "valid"

        if typ == "int" and not isinstance(value, int): return False, f"scalar int value required instead of {value}"
        if typ == "float" and not isinstance(value, float): return False, f"scalar float value required instead of {value}"
        if typ == "text" and not isinstance(value, str): return False, f"scalar text value required instead of {value}"
        if typ == "boolean" and not isinstance(value, bool): return False, f"scalar boolean value required instead of {value}"
        if typ == "dict" and not isinstance(value, dict): return False, f"dict value required instead of {value}"
        if typ == "list" and not isinstance(value, list): return False, f"list value required instead of {value}"

        if typ == "int[]":
            if not isinstance(value, list): return False, f"list of ints required instead of {value}"
            if not all(isinstance(x, int) for x in value): return False, f"list of ints required instead of {value}"

        elif typ == "float[]":
            if not isinstance(value, list): return False, f"list of floats required"
            if not all(isinstance(x, float) for x in value): return False, f"list of floats required instead of {value}"
            
        elif typ == "text[]":
            if not isinstance(value, list): return False, f"list of strings required"
            if not all(isinstance(x, str) for x in value): return False, f"list of strings required instead of {value}"
            
        elif typ == "boolean[]":
            if not isinstance(value, list): return False, f"list of booleans required"
            if not all(isinstance(x, bool) for x in value): return False, f"list of booleans required instead of {value}"
            
        if not typ in ("boolean", "boolean[]", "list", "dict", "any"):
            if "values" in definition:
                values = definition["values"]
                if isinstance(value, list):
                    if not all(x in values for x in value): return False, f"value in {value} is not allowed"
                else:
                    if not value in values: return False, f"value {value} is not allowed"
            else:
                if "pattern" in definition:
                    r = re.compile(definition["pattern"])
                    if isinstance(value, list):
                        if not all(r.match(v) is not None for v in value):  return False, f"value in {value} does not match the pattern"
                    else:
                        if r.match(value) is None:
                            return False, f"value {value} does not match the pattern"
                if "min" in definition:
                    vmin = definition["min"]
                    if isinstance(value, list):
                        if not all(x >= vmin for x in value):   return False, f"value in {value} out of range"
                    else:
                        if value < vmin:    return False, f"value {value} out of range"
                if "max" in definition:
                    vmax = definition["max"]
                    if isinstance(value, list):
                        if not all(x <= vmax for x in value):   return False, f"value in {value} out of range"
                    else:
                        if value > vmax:    return False, f"value {value} out of range"
                        
        return True, "valid"
    
    @staticmethod
    def validate_metadata_bulk(db, items):
        #
        # items is a list of:
        #     dictionary {"fid":}
        #     DBFile objects with metadata() method
        #     DBDataset objects with metadata() method
        #
        # returns list of tuples:
        #     (item, {"param name":"error", ...})
        #
        
        category_paths = set()
        for item in items:
            meta = item if isinstance(item, dict) else item.metadata()
            for name in meta:
                if "." in name:
                    path, _ = name.rsplit(".", 1)
                    category_paths.add(path)

        categories = {path:DBParamCategory.category_for_path(db, path) for path in category_paths}

        errors = []
        for item in items:
            meta = item if isinstance(item, dict) else item.metadata()
            item_errors = {}
            for name, value in meta.items():
                if "." in name:
                    path, vname = name.rsplit(".", 1)
                    category_paths.add(path)
                    category = categories[path]
                    ok, error = category.validate_parameter(vname, value)
                    if not ok:
                        item_errors[name] = error
            if item_errors:
                errors.append((item, item_errors))
        return errors
