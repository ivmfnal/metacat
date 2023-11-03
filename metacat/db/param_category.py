from metacat.common import DBObject, transactioned
from metacat.db import DBRole, DBUser
import json
from metacat.util import epoch, validate_metadata, fetch_generator

class DBParamCategory(DBObject):

    Table = "parameter_categories"
    ColumnsText = "path,owner_user,owner_role,description,restricted,definitions,creator,created_timestamp"
    Columns = ColumnsText.split(",")
    PK = ["path"]

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

    def to_jsonable(self):
        return dict(
            path = self.Path,
            owner_user = self.OwnerUser,
            owner_role = self.OwnerRole,
            description = self.Description,
            restricted = self.Restricted,
            definitions = self.Definitions,
            creator = self.Creator,
            created_timestamp = epoch(self.CreatedTimestamp)
        )
            
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
    
    @transactioned
    def save(self, transaction=None):
        defs = json.dumps(self.Definitions)
        columns = self.columns()
        transaction.execute(f"""
            update parameter_categories
                set owner_user=%(owner_user)s, owner_role=%(owner_role)s, restricted=%(restricted)s, 
                    definitions=%(defs)s, description=%(description)s
                where path = %(path)s
            """,
            dict(path=self.Path, owner_user=self.OwnerUser, owner_role=self.OwnerRole, restricted=self.Restricted, defs=defs,
                    description=self.Description, creator=self.Creator))
        return self

    @transactioned
    def create(self, transaction=None):
        defs = json.dumps(self.Definitions)
        columns = self.columns(exclude="created_timestamp")
        transaction.execute(f"""
            insert into parameter_categories({columns}) 
                values(%(path)s, %(owner_user)s, %(owner_role)s, %(description)s, %(restricted)s, %(defs)s, %(creator)s)
                returning created_timestamp
            """,
            dict(path=self.Path, owner_user=self.OwnerUser, owner_role=self.OwnerRole, restricted=self.Restricted, defs=defs,
                    description=self.Description, creator=self.Creator)
        )
        self.CreatedTimestamp = transaction.fetchone()[0]
        return self
    
    @staticmethod
    def from_tuple(db, tup):
        if tup is None: return None
        path, owner_user, owner_role, description, restricted, definitions, creator, created_timestamp = tup
        return DBParamCategory(db, path, owner_user=owner_user, owner_role=owner_role, description=description, 
                restricted=restricted, definitions=definitions, creator=creator, created_timestamp=created_timestamp)

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
        errors = validate_metadata(self.Definitions, self.Restricted, name=name, value=value)
        if errors:
            return False, errors[0][1]
        else:
            return True, "valid"
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
        for index, item in enumerate(items):
            meta = item if isinstance(item, dict) else item.metadata()
            item_errors = []
            for name, value in meta.items():
                if "." in name:
                    path, vname = name.rsplit(".", 1)
                    category = categories[path]
                    if category is not None:
                        ok, error = category.validate_parameter(vname, value)
                        if not ok:
                            item_errors.append({"name": name, "reason": error, "value": value})
                else:
                    item_errors.append({"name": name, "reason": "Parameter name without a category", "value": value})
            if item_errors:
                errors.append((index, item_errors))
        return errors
