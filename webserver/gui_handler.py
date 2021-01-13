from webpie import WPApp, WPHandler, Response, WPStaticHandler
import psycopg2, json, time, secrets, traceback, hashlib, pprint
from metacat.db import DBFile, DBDataset, DBFileSet, DBNamedQuery, DBUser, DBNamespace, DBRole, DBParamCategory, \
        parse_name, AlreadyExistsError, IntegrityError
from wsdbtools import ConnectionPool
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_str, to_bytes, SignedToken
from metacat.mql import MQLQuery
from metacat import Version

from base_handler import BaseHandler

class GUICategoryHandler(BaseHandler):
    
    def categories(self, request, relpath, **args):
        db = self.connect()
        cats = sorted(list(DBParamCategory.list(db)), key=lambda c:c.Path)
        return self.render_to_response("categories.html", categories=cats, **self.messages(args))
        
    index = categories
        
    def show(self, request, relpath, path=None):
        me = self.authenticated_user()
        db = self.connect()
        cat = DBParamCategory.get(db, path)
        admin = me.is_admin() if me is not None else False
        edit = me is not None and (me.Username in cat.owners() or admin)
        roles = sorted([r.Name for r in DBRole.list(db)]) if admin else (
            list(me.roles) if me is not None else [])
        users = sorted(list(u.Username for u in DBUser.list(db))) if admin else [me.Username if me is not None else None]
        cats = list(DBParamCategory.list(db))
        for name, d in cat.Definitions.items():
            print(name, d)
        return self.render_to_response("category.html", category=cat, edit=edit, create=False, roles=roles, admin=admin, user=me,
            users = users,
            types = DBParamCategory.Types)
        
    def create(self, request, relpath):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/crteate")
        admin = me.is_admin()
        cats = list(DBParamCategory.list(db))
        if admin:
            roles = sorted([r.Name for r in DBRole.list(db)])
        else:
            roles = list(me.roles)
            cats = [c for c in cats if me.Username in c.owners()]
        cats = sorted([c.Path for c in cats])
        users = sorted(list(u.Username for u in DBUser.list(db))) if admin else [me.Username]
        return self.render_to_response("category.html", category=None, create=True, creator=me.Username, roles=roles, 
                users=users, admin=admin, parent_categories=cats, user=me, types = DBParamCategory.Types)
    
    def read_parameter_definitions(self, form):
        defs = {}
        removals = []
        for k, v in form.items():
            if k.startswith("param:") and k.endswith(":name"):
                param_id = k.split(":", 2)[1]
                name = form.get(f"param:{param_id}:name")
                print("param k, id, name:", k, param_id, name)
                if name:
                    if form.get(f"param:{param_id}:remove"):
                        removals.append(name)
                    else:
                        type = form.get(f"param:{param_id}:type")
                        print("name, type:", name, type)
                        values = form.get(f"param:{param_id}:values", "")
                        values = values.split(",") if values else None
                        minv = form.get(f"param:{param_id}:min", "").strip() or None
                        maxv = form.get(f"param:{param_id}:max", "").strip() or None
                
                        if type in ("int", "int[]"):
                            if minv is not None:    
                                try:    minv = int(minv)
                                except: minv = None
                            if maxv is not None:    
                                try:    maxv = int(maxv)
                                except: maxv = None
                            if values:  
                                try:    values = [int(x) for x in values]
                                except: values = None
                        elif type in ("float", "float[]"):
                            if minv is not None:    
                                try:    minv = float(minv)
                                except: minv = None
                            if maxv is not None:    
                                try:    maxv = float(maxv)
                                except: maxv = None
                            if values:  
                                try:    values = [float(x) for x in values]
                                except: values = None
                        elif type in ("boolean", "boolean[]", "any"):
                            minv = maxv = values = None     # meaningless
                            
                        pdef = {"type":type}

                        if type in ("text", "text[]"):
                            pattern = form.get(f"param:{param_id}:pattern", "").strip() or None
                            if pattern: pdef["pattern"] = pattern
                            
                        if minv is not None:    pdef["min"] = minv
                        if maxv is not None:    pdef["max"] = maxv
                        if values is not None:    pdef["values"] = values
                        defs[name] = pdef
                        #print("pdef:", pdef)
        for n in removals:
            if n in defs:
                del defs[n]
        return defs
    
    def do_create(self, user, request):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/index")
        rpath = request.POST["rpath"]
        if '.' in rpath and rpath != '.':
            self.redirect("./index?error=%s" % (quote_plus("Invaid relative category path. Can not contain dot."),))
        parent_path = request.POST.get("parent_path")
        if not parent_path:
            if not me.is_admin():
                self.redirect("./index?error=%s" % (quote_plus("Can not create top level category"),))
            path = rpath
        else:
            parent_cat = DBParamCategory.get(db, parent_path)
            if not (me.is_admin() or me.Username in parent_cat.owners()):
                self.redirect("./index?error=%s" % (quote_plus(f"No permission to create a category under {parent_path}"),))
            path = f"{parent_path}.{rpath}"

        if DBParamCategory.exists(db, path):
            self.redirect("./index?error=%s" % (quote_plus(f"Category {path} already exists"),))
            
        owner_role = owner_user = None
        owner = request.POST.get("owner")
        if owner:
            kind, owner = owner.split(":",1)
            if kind == "role":
                owner_role = owner
            else:
                owner_user = owner
        
        #print("owner_user, owner_role:", owner_user, owner_role)
        
        restricted = request.POST.get("restricted", False)
        definitions = self.read_parameter_definitions(request.POST)
        cat = DBParamCategory(db, path, restricted=restricted, owner_role=owner_role, 
            owner_user=owner_user,
            creator = me.Username, description=request.POST["description"],
            definitions = definitions)

        cat.save()
        self.redirect(f"./show?path={path}")
        
    def save(self, request, relpath):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/index")
        mode = request.POST["mode"]
        create = mode == "create"
        
        if create:
            return self.do_create(me, request)
        
        path = request.POST["path"]

        cat = DBParamCategory.get(db, path)
        if cat is None:
            self.redirect("./index?error=%s" % (quote_plus(f"Category does not exist"),))

        if not (me.is_admin() or me.Username in cat.owners()):
            self.redirect("./index?error=%s" % (quote_plus(f"Permission denied"),))
            
        if me.is_admin():
            new_owner = request.POST.get("owner")
            if new_owner:
                kind, owner = new_owner.split(":",1)
                if kind == "role":
                    cat.OwnerRole = owner
                    cat.OwnerUser = None
                else:
                    cat.OwnerUser = owner
                    cat.OwnerRole = None
        print("owner_user, owner_role:", cat.OwnerUser, cat.OwnerRole)
            
        cat.Description = request.POST["description"]
        cat.Restricted = "restricted" in request.POST
        defs = self.read_parameter_definitions(request.POST)
        cat.Definitions = defs
        cat.save()
        self.redirect(f"./show?path={path}")
        
    def remove_definition(self, request, relpath, path=None, param=None):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/index")
        cat = DBParamCategory.get(db, path)
        if cat is None:
            self.redirect("./index?error=%s" % (quote_plus(f"Category does not exist"),))
        if not (me.is_admin() or me in cat.Owner):
            self.redirect("./show?path=%s&error=%s" % (path, quote_plus(f"Permission denied"),))
        defs = cat.definitions
        
class GUIHandler(BaseHandler):
    
    def __init__(self, request, app):
        BaseHandler.__init__(self, request, app)
        self.categories = GUICategoryHandler(request, app)

    def jinja_globals(self):
        return {"GLOBAL_User":self.authenticated_user()}
        
    def index(self, request, relpath, **args):
        return self.redirect("./datasets")
        
    def mql(self, request, relpath, **args):
        namespace = request.POST.get("namespace") or self.App.DefaultNamespace
        query_text = request.POST.get("query")
        query = None
        parsed = assembled = optimized = with_sql = ""
        results = False
        if query_text:
        
            query = MQLQuery.parse(query_text)
            
            try:    parsed = query.parse().pretty()
            except:
                parsed = traceback.format_exc()
                
            db = self.App.connect()
            try:    assembled = query.assemble(db, namespace).pretty()
            except:
                assembled = traceback.format_exc()
                    
            try:    
                optimized = query.optimize()
                optimized = optimized.pretty()
                #print("Server: optimized:", optimized)
            except:
                optimized = traceback.format_exc()
                
            try:    with_sql = query.generate_sql()
            except:
                   with_sql = traceback.format_exc()
                   
            results = True
        
        return self.render_to_response("mql.html", namespace = namespace, show_results = results,
            query_text = query_text or "", parsed = parsed, assembled = assembled, optimized = optimized,
                    with_sql = with_sql)

    def show_file(self, request, relpath, fid=None, **args):
        db = self.connect()
        f = DBFile.get(db, fid=fid, with_metadata=True)
        #print(f.__dict__)
        return self.render_to_response("show_file.html", f=f)

    def _meta_stats(self, files):
        #
        # returns [ (meta_name, [(value, count), ...]) ... ]
        #
        stats = {}
        for f in files:
            for n, v in f.Metadata.items():
                if isinstance(v, list): v = tuple(v)
                elif isinstance(v, dict): v = repr(v)
                n_dict = stats.setdefault(n, {})
                count = n_dict.setdefault(v, 0)
                n_dict[v] = count + 1
        out = []
        for name, counts in stats.items():
            clist = []
            for v, c in counts.items():
                if isinstance(v, tuple):    v = list(v)
                clist.append((v, c))
            out.append((name, sorted(clist, key=lambda vc: (-vc[1], vc[0]))))
        return sorted(out)
    
    def query(self, request, relpath, query=None, namespace=None, **args):
            
        namespace = namespace or request.POST.get("default_namespace") or self.App.DefaultNamespace
        #print("namespace:", namespace)
        if query is not None:
            query_text = unquote_plus(query)
        elif "query" in request.POST:
            query_text = request.POST["query"]
        else:
            query_text = request.body_file.read()
        query_text = to_str(query_text or "")
        results = None
        url_query = None
        files = None
        datasets = None
        runtime = None
        meta_stats = None
        with_meta = True
        
        view_meta_as =  request.POST.get("view_meta_as","table")
        
        save_as_dataset = "save_as_dataset" in request.POST
        
        db = self.App.connect()
        #print("query: method:", request.method)
        error = None
        message = None
        query_type = None
        if request.method == "POST":
                if request.POST["action"] == "run":
                        with_meta = request.POST.get("with_meta", "off") == "on"
                        t0 = time.time()
                        if query_text:
                            url_query = query_text.replace("\n"," ")
                            while "  " in url_query:
                                url_query = url_query.replace("  ", " ")
                            url_query = quote_plus(url_query)
                            if namespace: url_query += "&namespace=%s" % (namespace,)
                            #print("with_meta=", with_meta)
                            parsed = MQLQuery.parse(query_text)
                            query_type = parsed.Type
                            #print("Server.query: with_meta:", with_meta)
                            results = parsed.run(db, filters=self.App.filters(), 
                                    default_namespace=namespace or None,
                                    limit=1000 if not save_as_dataset else None, 
                                    with_meta=with_meta)
                        else:
                            results = None
                            url_query = None
                        results = None if results is None else list(results)
                        if query_type=="dataset":
                            datasets = results
                        else:
                            files = results
                            
                        meta_stats = None if (not with_meta or parsed.Type=="dataset") else self._meta_stats(files)
                        #print("meta_stats:", meta_stats, "    with_meta:", with_meta, request.POST.get("with_meta"))
                            
                        #print("query: results:", len(files))
                        runtime = time.time() - t0
                elif request.POST["action"] == "load":
                        namespace, name = request.POST["query_to_load"].split(":",1)
                        query_text = DBNamedQuery.get(db, namespace, name).Source
                elif request.POST["action"] == "save" and query_text:
                    name = request.POST["save_name"]
                    namespace = request.POST["save_namespace"]
                    saved = DBNamedQuery(db, name=name, namespace=namespace, source=query_text).save()
                    message = "Query saved as %s:%s" % (namespace, name)
                            
        user = self.authenticated_user()
        namespaces = None
        if True:
            #print("Server.query: namespace:", namespace)
            queries = list(DBNamedQuery.list(db))
            queries = sorted(queries, key=lambda q: (q.Namespace, q.Name))
            if user:
                namespaces = list(DBNamespace.list(db, owned_by_user=user))
                
                #print("query: namespaces:", [ns.Name for ns in namespaces])
                
            if files is not None and "save_as_dataset" in request.POST:
                if user is None:
                    error = "Unauthenticated user"
                else:
                    dataset_namespace = request.POST["save_as_dataset_namespace"]
                    dataset_name = request.POST["save_as_dataset_name"]
                
                    existing_dataset = DBDataset.get(db, dataset_namespace, dataset_name)
                    if existing_dataset != None:
                        error = "Dataset %s:%s already exists" % (dataset_namespace, dataset_name)
                    else:
                        ns = DBNamespace.get(db, dataset_namespace)
                        if ns is None:
                            error = "Namespace is not found"
                        elif not ns.owned_by_user(user):
                            error = "User not authorized to access the namespace %s" % (dataset_namespace,)
                        else:
                            ds = DBDataset(db, dataset_namespace, dataset_name)
                            ds.save()
                            files = list(files)
                            ds.add_files(files)
                            message = "Dataset %s:%s with %d files created" % (dataset_namespace, dataset_name, len(files))
                            
        attr_names = set()
        if files is not None:
            lst = []
            for f in files:
                lst.append(f)
                if len(lst) >= 1000:
                    #if len(lst) % 100 == 0: print("lst:", len(lst))
                    break
            files = lst
            if with_meta:
                for f in files:
                    if f.Metadata:
                        if view_meta_as == "json":
                            f.meta_view = json.dumps(f.Metadata, indent="  ", sort_keys = True) 
                        elif view_meta_as == "pprint":
                            f.meta_view = pprint.pformat(f.Metadata, compact=True, width=180)
                        for n in f.Metadata.keys():
                            attr_names.add(n)
                            
        #print("Server.query: file list generated")
        
        
        resp = self.render_to_response("query.html", 
            view_meta_as = view_meta_as,
            query_type = query_type,
            attr_names = sorted(list(attr_names)),
            message = message, error = error,
            allow_save_as_dataset = user is not None, namespaces = namespaces,
            allow_save_query = user is not None and namespaces,
            named_queries = queries,
            query=query_text, url_query=url_query,
            show_files=files is not None, files=files, 
            show_datasets=datasets is not None,datasets = datasets,
            runtime = runtime, meta_stats = meta_stats, with_meta = with_meta,
            namespace=namespace or "")
        return resp
        
    def named_queries(self, request, relpath, namespace=None, error="", **args):
        db = self.App.connect()
        queries = list(DBNamedQuery.list(db, namespace))
        return self.render_to_response("named_queries.html", namespace=namespace,
            error = unquote_plus(error),
            queries = queries)
            
    def named_query(self, request, relpath, name=None, edit="no", **args):
        namespace, name = parse_name(name, None)
        db = self.App.connect()
        query = DBNamedQuery.get(db, namespace, name)
        return self.render_to_response("named_query.html", 
                query=query, edit = edit=="yes")

    def create_named_query(self, request, relapth, **args):
        me = self.authenticated_user()
        if me is None:   
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_named_query")
        
        
        return self.render_to_response("named_query.html", namespaces=me.namespaces(), create=True)

    def save_named_query(self, request, relpath, **args):
        name = request.POST["name"]
        namespace = request.POST["namespace"]
        source = request.POST["source"]
        create = request.POST["create"] == "yes"

        query = MQLQuery.parse(query_text)
        if query.Type != "file":
            self.redirect("./named_queries?error=%s" % (quote_plus("only file queries can be saved"),))
        
        db = self.App.connect()
        query = DBNamedQuery(db, name=name, namespace=namespace, source=source).save()
        
        return self.render_to_response("named_query.html", query=query, edit = True)
        
    def users(self, request, relpath, error="", **args):
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/users")
        db = self.App.connect()
        users = sorted(list(DBUser.list(db)), key=lambda u: u.Username)
        #print("Server.users: users:", users)
        
        index = None
        if len(users) > 30:
            alphabet = set(u.Username[0] for u in users)
            index = {}
            for u in users:
                a = u.Username[0]
                if not a in index:
                    index[a] = u.Username

        return self.render_to_response("users.html", users=users, error=unquote_plus(error), admin = me.is_admin(),
                index = index)
        
    def user(self, request, relpath, username=None, error="", message="", **args):
        db = self.App.connect()
        user = DBUser.get(db, username)
        me = self.authenticated_user()
        all_roles = DBRole.list(db)
        role_set = set(user.roles)
        #print("role_set:", role_set)
        roles = (DBRole.get(db, r) for r in role_set)
        #print("user: roles:", list(roles))
        ldap_config = self.App.auth_config("ldap")
        ldap_url = ldap_config and ldap_config["server_url"]
        return self.render_to_response("user.html", all_roles=all_roles, user=user, roles=roles, role_set=role_set, 
            ldap_url = ldap_url, 
            error = unquote_plus(error), message=unquote_plus(message),
            mode = "edit" if (me.is_admin() or me.Username==username) else "view", 
            admin=me.is_admin())
        
    def create_user(self, request, relpath, error="", **args):
        db = self.App.connect()
        me = self.authenticated_user()
        if not me.is_admin():
            self.redirect("./users?error=%s" % (quote_plus("Not authorized to create users")))
        return self.render_to_response("user.html", error=unquote_plus(error), mode="create", all_roles = DBRole.list(db))
        
    def save_user(self, request, relpath, **args):
        db = self.App.connect()
        username = request.POST["username"]
        me = self.authenticated_user()
        
        new_user = request.POST["new_user"] == "yes"
        
        u = DBUser.get(db, username)
        if u is None:   
            if not new_user:    
                self.redirect("./users?error=%s", quote_plus("user not found"))
            u = DBUser(db, username, request.POST["name"], request.POST["email"], request.POST["flags"])
        else:
            u.Name = request.POST["name"]
            u.EMail = request.POST["email"]
            if me.is_admin():   u.Flags = request.POST["flags"]
            
        if me.is_admin() or me.Username == u.Username:
            
            password = request.POST.get("password1")
            if password:
                u.set_auth_info("password", None, password)
                
            if me.is_admin():
                u.set_auth_info("ldap", self.App.auth_config("ldap"), "allow_ldap" in request.POST)
                
            u.save()
            if me.is_admin():
                # update roles
                new_roles = set()
                for k, v in request.POST.items():
                    #print("POST:", k, v)
                    if k.startswith("member:"):
                        r = k[len("member:"):]
                        if v == "on":
                            new_roles.add(r)
                u.roles.set(new_roles)
                                        
        self.redirect(f"./user?username={username}&message="+quote_plus("User updated"))
#
# --- namespaces
#

    def namespaces(self, request, relpath, all="no", **args):
        db = self.App.connect()
        all = all == "yes"
        if all:
            namespaces = DBNamespace.list(db)
        else:
            me = self.authenticated_user()
            namespaces = DBNamespace.list(db, owned_by_user=me)
        return self.render_to_response("namespaces.html", namespaces=namespaces, showing_all=all, **self.messages(args))
        
    def namespace(self, request, relpath, name=None, **args):
        db = self.App.connect()
        ns = DBNamespace.get(db, name)
        roles = []
        edit = False
        me = self.authenticated_user()
        admin = False
        if me is not None:
            admin = me.is_admin()
            edit = admin or ns.owned_by_user(me)
            roles = DBRole.list(db) if admin else [DBRole.get(db, r) for r in me.roles]
        datasets = DBDataset.list(db, namespace=name) if ns is not None else None
        #print("namespace: roles", roles)
        return self.render_to_response("namespace.html", user=me, namespace=ns, edit=edit, create=False, roles=roles, admin=admin, 
            datasets = datasets,
            **self.messages(args))
        
    def create_namespace(self, request, relpath, error="", **args):
        db = self.App.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_namespace")
        admin = me.is_admin()
        roles = DBRole.list(db) if admin else [DBRole.get(db, r) for r in me.roles]
        return self.render_to_response("namespace.html", user=me, roles=roles, create=True, edit=False, error=unquote_plus(error))
        
    def save_namespace(self, request, relpath, **args):
        print("save_namespace")
        db = self.App.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/namespaces")
            
        admin = me.is_admin()
        print("save_namespace: POST:", list(request.POST.items()))
        name = request.POST["name"]
        print("save_namespace: POST['name']:", name)
        description = request.POST["description"]
        create = request.POST["create"] == "yes"

        ns = DBNamespace.get(db, name)
        if ns is None and not create:
            self.redirect("./namespaces?error=%s" % (quote_plus("Namespace not found"),))
        elif ns is not None and create:
            self.redirect("./namespace?name=%s&error=%s" % (name, quote_plus("Namespace already exists")))            

        owner_role = owner_user = None
        owner = request.POST.get("owner", "")
        print("save_namespace: owner:", owner)
        if owner.startswith("u:"):  owner_user = owner[2:]
        elif owner.startswith("r:"):  owner_role = owner[2:]

        if ns is None:
            # create new
            if not admin:
                if owner_user and owner_user != me.Username or \
                    owner_role and not owner_role in me.roles:
                        self.redirect("./namespaces?error=%s" % (quote_plus("Not authorized"),))                    
            assert (owner_user is None) != (owner_role is None)
            ns = DBNamespace(db, name, owner_role=owner_role, owner_user=owner_user, description=description)
        else:
            if not admin and not ns.owned_by_user(me):
                self.redirect("./namespaces?error=%s" % (quote_plus("Not authorized"),))
            ns.Description = description
            if admin:
                assert (owner_user is None) != (owner_role is None)
                ns.OwnerUser = owner_user
                ns.OwnerRole = owner_role
        ns.save()
        self.redirect("./namespaces")
        
    def datasets(self, request, relpath, **args):
        db = self.App.connect()
        datasets = DBDataset.list(db)
        datasets = sorted(list(datasets), key=lambda x: (x.Namespace, x.Name))
        return self.render_to_response("datasets.html", datasets=datasets, **self.messages(args))

    def dataset_files(self, request, relpath, dataset=None, with_meta="no"):
        with_meta = with_meta == "yes"
        namespace, name = (dataset or relpath).split(":",1)
        db = self.App.connect()
        dataset = DBDataset.get(db, namespace, name)
        files = sorted(list(dataset.list(with_metadata=with_meta)), key=lambda x: (x.Namespace, x.Name))
        return self.render_to_response("dataset_files.html", files=files, dataset=dataset, with_meta=with_meta)
        
    def create_dataset(self, request, relpath, **args):
        user = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_dataset")
        admin = user.is_admin()
        db = self.App.connect()
        namespaces = list(DBNamespace.list(db, owned_by_user=user if not admin else None))
        #print("create_dataset: amdin:", admin, "   namespaces:", namespaces)
        if not namespaces:
            self.redirect("./create_namespace?error=%s" % (quote_plus("You do not own any namespace. Create one first"),))
        return self.render_to_response("dataset.html", namespaces=namespaces, edit=False, create=True)
        
    def dataset(self, request, relpath, namespace=None, name=None, **args):
        db = self.App.connect()
        dataset = DBDataset.get(db, namespace, name)
        if dataset is None: self.redirect("./datasets")

        nfiles = dataset.nfiles
        files = sorted(list(dataset.list_files(with_metadata=True, limit=1000)), key = lambda x: x.Name)
        #print ("files:", len(files))
        attr_names = set()
        for f in files:
            if f.Metadata:
                for n in f.Metadata.keys():
                    attr_names.add(n)
        attr_names=sorted(list(attr_names))

        user = self.authenticated_user()
        edit = False
        if user is not None:
            ns = DBNamespace.get(db, name=dataset.Namespace)
            edit = ns.owned_by_user(user)
        return self.render_to_response("dataset.html", dataset=dataset, files=files, nfiles=nfiles, attr_names=attr_names, edit=edit, create=False,
            **self.messages(args))
        
    def save_dataset(self, request, relpath, **args):
        #print("save_dataset:...")
        db = self.App.connect()
        user = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/datasets")
        admin = user.is_admin()
        namespace = request.POST["namespace"]
        if not admin:
            ns = DBNamespace.get(db, namespace)
            if not ns.owned_by_user(user):
                self.redirect("./datasets?error=%s" % (quote_plus(f"No permission to modify namespace {namespace}"),))

        if request.POST["create"] == "yes":
            ds = DBDataset(db, request.POST["namespace"], request.POST["name"])
            ds.Creator = user.Username
        else:
            ds = DBDataset.get(db, request.POST["namespace"], request.POST["name"])

        ds.Monotonic = "monotonic" in request.POST
        ds.Frozen = "frozen" in request.POST
        ds.save()
        self.redirect("./datasets")
        
#
# --- roles
#
        
    def roles(self, request, relpath, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        db = self.App.connect()
        roles = DBRole.list(db)
        admin = me.is_admin()
        return self.render_to_response("roles.html", roles=roles, edit=admin, create=admin, **self.messages(args))
        
    def role(self, request, relpath, name=None, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        admin = me.is_admin()
        db = self.App.connect()
        role = DBRole.get(db, name)
        users = sorted(list(role.members))
        #print("all_users:", all_users)
        return self.render_to_response("role.html", role=role, users=users, edit=admin or me in role, create=False, **self.messages(args))

    def create_role(self, request, relpath, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_role")
        if not me.is_admin():
            self.redirect("./roles")
        db = self.App.connect()
        all_users = list(DBUser.list(db))
        return self.render_to_response("role.html", all_users=all_users, edit=False, create=True)
        
    def save_role(self, request, relpath, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        db = self.App.connect()
        rname = request.POST["name"]
        role = DBRole.get(db, rname)
        if role is None:    # create
            if not me.is_admin():
                self.redirect("./roles?error=" + quote_plus("Unauthorized to create roles"))            
            role = DBRole(db, rname, "")
        else:
            if not me.is_admin() and not me in role:
                self.redirect("./roles?error=" + quote_plus("Unauthorized to edit role"))            
        role.Description = request.POST["description"]
        members = set()
        for k in request.POST.keys():
            if k.startswith("member:"):
                username = k.split(":", 1)[-1]
                members.add(username)
        #print("save_role: members:", members)
        role.save()
        role.set_members(members)
        self.redirect("./role?name=%s&message=Role+saved" % (rname,))
