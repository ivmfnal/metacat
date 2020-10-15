from webpie import WPApp, WPHandler, Response, WPStaticHandler
import psycopg2, json, time, secrets, traceback, hashlib
from metacat.db import DBFile, DBDataset, DBFileSet, DBNamedQuery, DBUser, DBNamespace, DBRole, parse_name
from wsdbtools import ConnectionPool
from urllib.parse import quote_plus, unquote_plus

from metacat.util import to_str, to_bytes, SignedToken
from metacat.mql import MQLQuery
from metacat import Version

class BaseHandler(WPHandler):
    
    def connect(self):
        return self.App.connect()

    def text_chunks(self, gen, chunk=100000):
        buf = []
        size = 0
        for x in gen:
            n = len(x)
            buf.append(x)
            size += n
            if size >= chunk:
                yield "".join(buf)
                size = 0
                buf = []
        if buf:
            yield "".join(buf)
            
    def authenticated_user(self):
        username = self.App.user_from_request(self.Request)
        if not username:    return None
        db = self.App.connect()
        return DBUser.get(db, username)
        
class GUICategoryHandler(BaseHandler):
    
    def categories(self, request, relpath):
        db = self.connect()
        cats = DBParamCategory.list(db)
        return self.render_to_response("categories.html", categories=cats)
        
    index = categories
        
    def show(self, request, relpath, path=None):
        me = self.authenticated_user()
        db = self.connect()
        cat = DBParamCategory.get(db, path)
        admin = me.is_admin()
        edit = me is not None and (me in ns.Owner or admin)
        roles = None
        if edit:
            if admin:
                roles = DBRole.list(db)
            else:
                roles = me.roles()
        return self.render_to_response("category.html", category=cat, edit=edit, create=False, roles=roles, admin=admin)
        
    def create(self, request, relpath):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/crteate")
        cats = list(DBParamCategory.list(db))
        admin = me.is_admin()
        if not admin:
            cats = [c for c in cats if me in c.Owner]
        return self.render_to_response("create_category.html", parents=cats, roles=me.roles(), admin=admin)
        
    def do_create(self, request, relpath):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/index")
        rpath = request.POST["rpath"]
        if '.' in rpath:
            self.redirect("./index?error=%s" % (quote_plus("Invaid relative category path. Can not contain dot."),))
        parent_path = reuest.POST["parent"]
        if not parent_path:
            if not me.is_admin():
                self.redirect("./index?error=%s" % (quote_plus("Can not create top level category"),))
        else:
            parent_cat = DBParamCategory.get(db, parent_path)
            if not (me.is_admin() or me in parent_cat.Owner):
                self.redirect("./index?error=%s" % (quote_plus(f"No permission to create a category under {parent_path}"),))
        
        path = f"{parent_path}.{rpath}"
        if DBParamCategory.exists(db, path):
            self.redirect("./index?error=%s" % (quote_plus(f"Category {path} already exists"),))
            
        cat = DBParamCategory(db, path, me)
        cat.Restricted = "restricted" in request.POST
        cat.save()
        self.redirect(f"./show?path={path}")
        
    def do_save(self, request, relpath):
        db = self.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/index")
        path = request.POST["path"]

        cat = DBParamCategory.get(db, path)
        if cat is None:
            self.redirect("./index?error=%s" % (quote_plus(f"Category does not exist"),))

        if not (me.is_admin() or me in cat.Owner):
            self.redirect("./index?error=%s" % (quote_plus(f"Permission denied"),))

        cat.Restricted = "restricted" in request.POST
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
        return self.render_to_response("show_file.html", f=f)

    def _meta_stats(self, files):
        #
        # returns [ (meta_name, [(value, count), ...]) ... ]
        #
        stats = {}
        for f in files:
            for n, v in f.Metadata.items():
                if isinstance(v, list): v = tuple(v)
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
                        elif not user in ns.Owner:
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
                        for n in f.Metadata.keys():
                            attr_names.add(n)
        #print("Server.query: file list generated")
        resp = self.render_to_response("query.html", 
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
        users = DBUser.list(db)
        #print("Server.users: users:", users)
        return self.render_to_response("users.html", users=users, error=unquote_plus(error), admin = me.is_admin())
        
    def user(self, request, relpath, username=None, error="", **args):
        db = self.App.connect()
        user = DBUser.get(db, username)
        me = self.authenticated_user()
        all_roles = DBRole.list(db)
        return self.render_to_response("user.html", all_roles=all_roles, user=user, 
            error = unquote_plus(error),
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
            
            if "password1" in request.POST and request.POST.get("password1"):
                if request.POST["password1"] != request.POST.get("password2"):
                    if new_user:
                        self.redirect("./create_user?error=%s" % (quote_plus("Password mismatch")))
                    else:
                        self.redirect("./user&error=%s" % (quote_plus("Password mismatch")))
                        
                u.set_password(request.POST["password1"])
                    
            if me.is_admin():
                # update roles
                all_roles = {r.Name:r for r in DBRole.list(db)}
                old_roles = set(r.Name for r in DBRole.list(db, u))
                print("old roles:", old_roles)
                new_roles = set()
                for k, v in request.POST.items():
                    print("POST:", k, v)
                    if k.startswith("member:"):
                        r = k[len("member:"):]
                        if v == "on":
                            new_roles.add(r)
                for rn in old_roles - new_roles:
                    r = all_roles[rn]
                    r.remove_user(u)
                    print("removing %s from %s" % (u.Username, r.Name))
                    r.save()
                for rn in new_roles - old_roles:
                    r = all_roles[rn]
                    r.add_user(u)
                    r.save()
                
            u.save()
                    
                    
        self.redirect("./users")
        
    def namespaces(self, request, relpath, **args):
        db = self.App.connect()
        namespaces = DBNamespace.list(db)
        return self.render_to_response("namespaces.html", namespaces=namespaces)
        
    def namespace(self, request, relpath, name=None, **args):
        db = self.App.connect()
        ns = DBNamespace.get(db, name)
        me = self.authenticated_user()
        admin = me.is_admin()
        edit = me is not None and (me in ns.Owner or admin)
        roles = list(DBRole.list(db) if admin else me.roles())
        #print("namespace: roles", roles)
        return self.render_to_response("namespace.html", namespace=ns, edit=edit, create=False, roles=roles)
        
    def create_namespace(self, request, relpath, error="", **args):
        db = self.App.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_namespace")
        admin = me.is_admin()
        roles = DBRole.list(db) if admin else me.roles()
        return self.render_to_response("namespace.html", roles=roles, create=True, edit=False, error=unquote_plus(error))
        
    def save_namespace(self, request, relpath, **args):
        db = self.App.connect()
        me = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/namespaces")
        if not me.is_admin():
            self.redirect("./namespaces?error=%s" % (quote_plus("Not authorized to modify roles")))
        name = request.POST["name"]
        role = request.POST.get("role")
        if role is not None:
            role = DBRole.get(db, role)
        ns = DBNamespace.get(db, name)
        if ns is None:
            assert role is not None
            ns = DBNamespace(db, name, role)
            ns.save()
        elif role is not None:
            ns.Owner = role
            ns.save()
        self.redirect("./namespaces")
        
    def datasets(self, request, relpath, **args):
        db = self.App.connect()
        datasets = DBDataset.list(db)
        datasets = sorted(list(datasets), key=lambda x: (x.Namespace, x.Name))
        return self.render_to_response("datasets.html", datasets=datasets)

    def dataset_files(self, request, relpath, dataset=None, with_meta="no"):
        with_meta = with_meta == "yes"
        namespace, name = (dataset or relpath).split(":",1)
        db = self.App.connect()
        dataset = DBDataset.get(db, namespace, name)
        files = sorted(list(dataset.list_files(with_metadata=with_meta)), key=lambda x: (x.Namespace, x.Name))
        return self.render_to_response("dataset_files.html", files=files, dataset=dataset, with_meta=with_meta)
        
    def create_dataset(self, request, relpath, **args):
        user = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_dataset")
        admin = user.is_admin()
        db = self.App.connect()
        namespaces = list(ns for ns in DBNamespace.list(db) if admin or (user in ns.Owner))
        #print("create_dataset: amdin:", admin, "   namespaces:", namespaces)
        if not namespaces:
            self.redirect("./create_namespace?error=%s" % (quote_plus("You do not own any namespace. Create one first"),))
        return self.render_to_response("dataset.html", namespaces=namespaces, edit=False, create=True)
        
    def dataset(self, request, relpath, namespace=None, name=None, **args):
        db = self.App.connect()
        dataset = DBDataset.get(db, namespace, name)
        if dataset is None: self.redirect("./datasets")

        nfiles = dataset.nfiles
        files = sorted(list(dataset.list_files(with_metadata=True, limit=100)), key = lambda x: x.Name)
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
            edit = ns.Owner == user
        return self.render_to_response("dataset.html", dataset=dataset, files=files, nfiles=nfiles, attr_names=attr_names, edit=edit, create=False)
        
    def save_dataset(self, request, relpath, **args):
        db = self.App.connect()
        if request.POST["create"] == "yes":
            ds = DBDataset(db, request.POST["namespace"], request.POST["name"]) # no parent dataset for now
        else:
            ds = DBDataset.get(db, request.POST["namespace"], request.POST["name"])
        ds.Monotonic = "monotonic" in request.POST
        ds.Frozen = "frozen" in request.POST
        ds.save()
        self.redirect("./datasets")
        
    def roles(self, request, relpath, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        db = self.App.connect()
        roles = DBRole.list(db)
        admin = me.is_admin()
        return self.render_to_response("roles.html", roles=roles, edit=admin, create=admin)
        
    def role(self, request, relpath, name=None, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        admin = me.is_admin()
        db = self.App.connect()
        role = DBRole.get(db, name)
        all_users = list(DBUser.list(db))
        #print("all_users:", all_users)
        return self.render_to_response("role.html", all_users=all_users, role=role, edit=admin, create=False)

    def create_role(self, request, relpath, **args):
        me = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_role")
        if not me.is_admin():
            self.redirect("./roles")
        db = self.App.connect()
        return self.render_to_response("role.html", all_users=list(DBUser.list(db)), edit=False, create=True)
        
    def save_role(self, request, relpath, **args):
        me = self.authenticated_user()
        if not me.is_admin():
            self.redirect("./roles")
        db = self.App.connect()
        rname = request.POST["name"]
        role = DBRole.get(db, rname)
        if role is None:
            role = DBRole(db, rname, "")
        role.Description = request.POST["description"]
        members = set()
        for k in request.POST.keys():
            if k.startswith("member:"):
                username = k.split(":", 1)[-1]
                members.add(username)
        print("save_role: members:", members)
        role.Usernames = sorted(list(members))
        role.save()
        self.redirect("./role?name=%s" % (rname,))
            
class DataHandler(BaseHandler):
    
    def __init__(self, request, app):
        BaseHandler.__init__(self, request, app)
        self.NamespaceAuthorization = {}                # namespace -> True/False
        
    def _namespace_authorized(self, db, user, namespace):
        auth = self.NamespaceAuthorization.get(namespace)
        if auth is None:
            ns = DBNamespace.get(db, namespace)
            self.NamespaceAuthorization[namespace] = auth = (user in ns.Owner)
        return auth

    def json_generator(self, lst):
        from collections.abc import Iterable
        assert isinstance(lst, Iterable)
        yield "["
        first = True
        for x in lst:
            j = json.dumps(x)
            if not first: j = ","+j
            yield j
            first = False
        yield "]"
        
    def json_chunks(self, lst, chunk=100000):
        return self.text_chunks(self.json_generator(lst), chunk)
        
    def namespaces(self, request, relpath, **args):
        db = self.App.connect()
        out = [ns.to_jsonable() for ns in DBNamespace.list(db)]
        return json.dumps(out), "text/json"

    def namespace(self, request, relpath, name=None, **args):
        name = name or relpath
        db = self.App.connect()
        ns = DBNamespace.get(db, name)
        if ns is None:
            return "Not found", 400
        return json.dumps(ns.to_jsonable()), "text/json"
        
    def create_namespace(self, request, relpath, name=None, owner=None, **args):
        db = self.App.connect()
        user = self.authenticated_user()
        if user is None:
            return 401
        if owner is None:
            user_roles = list(user.roles())
            if len(user_roles) == 1:
                owner_role = user_roles[0]
            else:
                return "Owner role must be specified", 400
        else:
            owner_role = DBRole.get(db, owner)
            if not user in owner_role:
                return 403

        if DBNamespace.exists(db, name):
            return "Namespace already exists", 400
            
        ns = DBNamespace(db, name, owner_role.Name)
        ns.save()
        return json.dumps(ns.to_jsonable()), "text/json"
            
    def datasets(self, request, relpath, with_file_counts="no", **args):
        with_file_counts = with_file_counts == "yes"
        db = self.App.connect()
        datasets = DBDataset.list(db)
        out = []
        for ds in datasets:
            dct = ds.to_jsonable()
            if with_file_counts:
                dct["file_count"] = ds.nfiles
            out.append(dct)
        return json.dumps(out), "text/json"

    def dataset(self, request, relpath, dataset=None, **args):
        db = self.App.connect()
        namespace, name = (dataset or relpath).split(":", 1)
        dataset = DBDataset.get(db, namespace, name)
        dct = dataset.to_jsonable()
        dct["file_count"] = dataset.nfiles
        return json.dumps(dct), "text/json"
            
    def dataset_count(self, request, relpath, dataset=None, **args):
        namespace, name = (dataset or relpath).split(":", 1)
        db = self.App.connect()
        nfiles = DBDataset(db, namespace, name).nfiles
        return '{"nfiles":%d}\n' % (nfiles,), {"Content-Type":"text/json",
            "Access-Control-Allow-Origin":"*"
        } 
        
            
    def create_dataset(self, request, relpath, dataset=None, parent=None, **args):
        user = self.authenticated_user()
        if user is None:
            return 401
        db = self.App.connect()
        namespace, name = dataset.split(":",1)
        namespace = DBNamespace.get(db, namespace)
        if not user in namespace.Owner:
            return 403
        if DBDataset.get(db, namespace, name) is not None:
            return "Already exists", 409
        parent_ds = None
        if parent:
                parent_namespace, parent_name = parent.split(":",1)
                parent_ns = DBNamespace.get(db, parent_namespace)
                if not user in parent_ns.Owner:
                        return 403
                parent_ds = DBDataset.get(db, parent_namespace, parent_name)
                if parent_ds is None:
                        return "Parent dataset not found", 404
                dataset = DBDataset(db, namespace, name, 
                        parent_namespace=parent_namespace, parent_name=parent_name).save()
        else:
                dataset = DBDataset(db, namespace, name).save()
        try:    out = dataset.to_json(), "text/json" 
        except Exception as e:
            print (e)
         
        return dataset.to_json(), "text/json"  
        
    def add_files(self, request, relpath, namespace=None, dataset=None, **args):
        #
        # add existing files to a dataset
        #
        user = self.authenticated_user()
        if user is None:
            return 401
        db = self.App.connect()
        default_namespace = namespace
        ds_namespace, ds_name = parse_name(dataset, default_namespace)
        if ds_namespace is None:
            return "Dataset namespace unspecified", 400
        if not self._namespace_authorized(db, user, ds_namespace):
            return f"Permission to add files dataset {dataset} denied", 403
        ds = DBDataset.get(db, ds_namespace, ds_name)
        if ds is None:
            return "Dataset not found", 404
        if ds.Frozen:
            return "Dataset is frozen", 403
        file_list = json.loads(request.body) if request.body else []
        if not file_list:
                return "Empty file list", 400
        files = []
        for file_item in file_list:
            fid = file_item.get("fid")
            if fid is not None:
                f = DBFile.get(db, fid=fid)
                if f is None:
                    return "File with id '%s' not found" % (fid,), 404
            else:
                spec = file_item.get("name")
                if not spec:
                    return "File id or namespace:name must be specified", 400
                namespace, name = parse_name(spec, default_namespace)
                if not namespace:
                    return "File namespace unspecified", 400
                f = DBFile.get(db, name=name, namespace=namespace)
                if f is None:
                    return f"File {namespace}:{name} not found", 404
            #namespace = f.Namespace
            #if not self._namespace_authorized(db, user, namespace):
            #    return f"Permission to add files from namespace {namespace} is denied", 403
            files.append(f)
        if files:
            ds.add_files(files, do_commit=True)
        return json.dumps([f.FID for f in files]), "text/json"
        
    def declare_files(self, request, relpath, namespace=None, dataset=None, **args):
        # Declare new files, add to the dataset
        # request body: JSON with list:
        #
        # [
        #       {       
        #               name: "namespace:name",   or "name", but then default namespace must be specified
        #               fid: "fid",               // optional
        #               parents:        [fid,...],              // optional
        #               metadata: { ... }       // optional
        #       },...
        # ]
        #               
        default_namespace = namespace
        user = self.authenticated_user()
        if user is None:
            return 401
            
        if dataset is None:
            return "Dataset not specified", 400
            
        verified_namespaces = set()

        db = self.App.connect()

        ds_namespace, ds_name = parse_name(dataset, default_namespace)
        if not ds_namespace in verified_namespaces:
            ns = DBNamespace.get(db, ds_namespace)
            if ns is None:
                return f"Namespace {ds_namespace} does not exist", 404
            if not user in ns.Owner:
                return f"Permission to declare files in namespace {namespace} denied", 403
            verified_namespaces.add(namespace)

        ds = DBDataset.get(db, namespace=ds_namespace, name=ds_name)
        if ds is None:
            return f"Dataset {ds_namespace}:{ds_name} does not exist", 404
            
        file_list = json.loads(request.body) if request.body else []
        if not file_list:
                return "Empty file list", 400
        files = []
        for file_item in file_list:
            name = file_item.get("name")
            fid = file_item.get("fid")
            if name is None:
                return "Missing file namespace/name", 400
            namespace, name = parse_name(file_item["name"], default_namespace)
            if DBFile.exists(db, namespace=namespace, name=name):
                return "File %s:%s already exists" % (namespace, name), 400
            if fid is not None and DBFile.exists(db, fid=fid):
                return "File with fid %s already exists" % (fid,), 400
                
            if not self._namespace_authorized(db, user, namespace):
                return f"Permission to declare files to namespace {namespace} denied", 403
            
            f = DBFile(db, namespace=namespace, name=name, fid=file_item.get("fid"), metadata=file_item.get("metadata"))
            f.create(do_commit=False)
            
            parents = file_item.get("parents")
            if parents:
                f.add_parents(parents, do_commit=False)
            files.append(f)
            
        ds.add_files(files, do_commit=True)
        
        out = [
                    dict(
                        name="%s:%s" % (f.Namespace, f.Name), 
                        fid=f.FID
                    )
                    for f in files
        ]
        return json.dumps(out), "text/json"
        
    def update_meta_bulk(self, db, user, data, mode, default_namespace):
        new_meta = data["metadata"]
        ids = data.get("fids")
        names = data.get("names")
        new_meta = data["metadata"]
        if (names is None) == (ids is None):
            return "Either file ids or names must be specified, but not both", 400
            
        if ids:
            file_set = DBFileSet.from_id_list(db, ids)
        else:
            file_set = DBFileSet.from_name_list(db, names, default_namespace=default_namespace)
        file_set = list(file_set)
        verified_namespaces = set()
        out = []
        for f in file_set:
            namespace = f.Namespace
            if not namespace in verified_namespaces:
                ns = DBNamespace.get(db, namespace)
                if not user in ns.Owner:
                    return f"Permission to declare files to namespace {namespace} denied", 403
                verified_namespaces.add(namespace)

            meta = new_meta
            if mode == "update":
                meta = {}
                meta.update(f.metadata())   # to make a copy
                meta.update(new_meta)
            f.Metadata = meta
            
            out.append(                    
                dict(
                        name="%s:%s" % (f.Namespace, f.Name), 
                        fid=f.FID,
                        metadata=meta
                    )
            )
        
        DBFile.update_many(db, file_set, do_commit=True)
        return json.dumps(out), 200
        
                
    def update_meta(self, request, relpath, namespace=None, mode="update", **args):
        # mode dan be "update" - add/pdate metadata with new values
        #             "replace" - discard old metadata and update with new values
        # 
        # Update metadata for existing files
        #
        # mode1: metadata for each file is specified separately
        # [
        #       {       
        #               name: "namespace:name",   or "name", but then default namespace must be specified
        #               fid: "fid",               // optional
        #               parents:        [fid,...],              // optional
        #               metadata: { ... }       // optional
        #       }, ... 
        # ]
        #
        # mode2: common changes for many files, cannot be used to update parents
        # {
        #   names: [ ... ], # either names or fids must be present, but not both
        #   fids:  [ ... ],
        #   metadata: { ... }
        # }
        #
        default_namespace = namespace
        user = self.authenticated_user()
        if user is None:
            return 403
        db = self.App.connect()
        data = json.loads(request.body)
        verified_namespaces = set()

        if isinstance(data, dict):
            return self.update_meta_bulk(db, user, data, mode, default_namespace)
        else:
            return "Not implemented", 400
            
        

        file_list = json.loads(request.body) if request.body else []
        if not file_list:
                return "Empty file list", 400
        files = []
        for file_item in file_list:
            fid, spec = None, None
            if "fid" in file_item:
                fid = file_item.get("fid")
                f = DBFile.get(db, fid=fid)
            else:
                spec = file_item.get("name")
                if spec is None:
                    return "Either file namespace:name or file id must be specified for each file", 400
                namespace, name = parse_name(spec, default_namespace)
                f = DBFile.get(db, namespace=namespace, name=name)
            if f is None:
                return "File %s not found" % (fid or spec,), 404
            namespace = f.Namespace
            if not namespace in verified_namespaces:
                ns = DBNamespace.get(db, namespace)
                if not user in ns.Owner:
                    return f"Permission to declare files to namespace {namespace} denied", 403
                verified_namespaces.add(namespace)
            if "metadata" in file_item:
                f.Metadata = file_item["metadata"]
            files.append((f, file_item.get("parents")))

        for f, parents in files:
            if parents is not None:
                f.set_parents(parents, do_commit=False)
                
        files = [f for f, _ in files]
                
        DBFile.update_many(db, files)
        
        out = [
                    dict(
                        name="%s:%s" % (f.Namespace, f.Name), 
                        fid=f.FID,
                        metadata=f.Metadata,
                        parents=[p.FID for p in f.parents()]
                    )
                    for f in files
        ]
        return json.dumps(out), "text/json"
                
            
    def file(self, request, relpath, name=None, fid=None, with_metadata="yes", with_relations="yes", **args):
        if name:
            namespace, name = parse_name(name, None)
            if not namespace:
                return "Namespace is not specfied", 400
        else:
            if not fid:
                return "Either namespace/name or fid must be specified", 400
        
        
        with_metadata = with_metadata == "yes"
        with_relations = with_relations == "yes"
        
        db = self.App.connect()
        if fid:
            f = DBFile.get(db, fid = fid)
        else:
            f = DBFile.get(db, namespace=namespace, name=name)
        return f.to_json(with_metadata=with_metadata, with_relations=with_relations), "text/json"
            
    def query(self, request, relpath, query=None, namespace=None, with_meta="no", 
                    add_to=None, save_as=None, expiration=None, **args):
        with_meta = with_meta == "yes"
        namespace = namespace or self.App.DefaultNamespace
        if query is not None:
            query_text = unquote_plus(query)
            #print("query from URL:", query_text)
        elif "query" in request.POST:
            query_text = request.POST["query"]
            #print("query from POST:", query_text)
        else:
            query_text = request.body
            #print("query from body:", query_text)
        query_text = to_str(query_text or "")
        
        add_namespace = add_name = ds_namespace = ds_name = None

        db = self.App.connect()
        user = self.authenticated_user()

        if save_as:
            if user is None:
                return 401
            ds_namespace, ds_name = parse_name(save_as, namespace)
            ns = DBNamespace.get(db, ds_namespace)

            if ns is None:
                return f"Namespace {ds_namespace} does not exist", 404

            if not user in ns.Owner:
                return f"Permission to create a dataset in the namespace {namespace} denied", 403

            if DBDataset.exists(db, ds_namespace, ds_name):
                return "Already exists", 409

        if add_to:
            if user is None:
                return 401
            add_namespace, add_name = parse_name(add_to, namespace)
            ns = DBNamespace.get(db, add_namespace)

            if ns is None:
                return f"Namespace {add_namespace} does not exist", 404

            if not DBDataset.exists(db, add_namespace, add_name):
                return f"Dataset {add_namespace}:{add_name} does not exist", 404

            if not user in ns.Owner:
                return f"Permission to add files to dataset in the namespace {add_namespace} denied", 403

        t0 = time.time()
        if not query_text:
            return "[]", "text/json"
            
        query = MQLQuery.parse(query_text)
        query_type = query.Type
        results = query.run(db, filters=self.App.filters(), with_meta=with_meta, default_namespace=namespace or None)

        if not results:
            return "[]", "text/json"

        if query_type == "file":
            
            if save_as:
                results = list(results)
                ds = DBDataset(db, ds_namespace, ds_name)
                ds.save()
                ds.add_files(results)            
            
            if add_to:
                results = list(results)
                ds = DBDataset(db, add_namespace, add_name)
                ds.add_files(results)      
            
            if with_meta:
                data = (
                    { 
                        "name":"%s:%s" % (f.Namespace, f.Name),
                        "fid":f.FID,
                        "metadata": f.Metadata or {}
                    } for f in results 
                )
            else:
                data = (
                    { 
                        "name":"%s:%s" % (f.Namespace, f.Name),
                        "fid":f.FID
                    } for f in results 
                )
        else:
            data = (
                    { 
                        "name":"%s:%s" % (d.Namespace, d.Name),
                        "parent":   None if not d.ParentName else "%s:%s" % (d.ParentNamespace, d.ParentName),
                        "metadata": {}
                    } for d in results 
            )
            
        return self.json_chunks(data), "text/json"
        
    def named_queries(self, request, relpath, namespace=None, **args):
        db = self.App.connect()
        queries = list(DBNamedQuery.list(db, namespace))
        data = ("%s:%s" % (q.Namespace, q.Name) for q in queries)
        return self.json_chunks(data), "text/json"
            
class AuthHandler(WPHandler):

    def whoami(self, request, relpath, **args):
        return str(self.App.user_from_request(request)), "text/plain"
        
    def token(self, request, relpath, **args):
        return self.App.encoded_token_from_request(request)+"\n"
        
    def auth(self, request, relpath, redirect=None, **args):
        from rfc2617 import digest_server
        # give them cookie with the signed token
        
        ok, data = digest_server("metadata", request.environ, self.App.get_password)
        if ok:
            resp = self.App.response_with_auth_cookie(data, redirect)
            return resp
        elif data:
            return Response("Authorization required", status=401, headers={
                'WWW-Authenticate': data
            })

        else:
            return 403, "Authentication failed"
            
    def logout(self, request, relpath, redirect=None, **args):
        return self.App.response_with_unset_auth_cookie(redirect)

    def login(self, request, relpath, message="", error="", redirect=None, **args):
        return self.render_to_response("login.html", error=unquote_plus(error), 
                message=unquote_plus(message), redirect=redirect)
        
    def do_login(self, request, relpath, **args):
        username = request.POST["username"]
        password = request.POST["password"]
        redirect = request.POST.get("redirect", self.scriptUri() + "/gui/index")
        #print("redirect:", redirect)
        db = self.App.connect()
        u = DBUser.get(db, username)
        if not u:
            #print("authentication error")
            self.redirect("./login?message=User+%s+not+found" % (username,))
        ok, reason = u.verify_password(password)
        if not ok:
            self.redirect("./login?error=%s" % (quote_plus("Authentication error"),))
            
        #print("authenticated")
        return self.App.response_with_auth_cookie(username, redirect)

    def verify(self, request, relpath, **args):
        username = self.App.user_from_request(request)
        return "OK" if username else ("Token verification error", 403)

class RootHandler(WPHandler):
    
    def __init__(self, *params, **args):
        WPHandler.__init__(self, *params, **args)
        self.data = DataHandler(*params, **args)
        self.gui = GUIHandler(*params, **args)
        self.auth = AuthHandler(*params, **args)
        self.static = WPStaticHandler(*params, root=self.App.StaticLocation)

    def index(self, req, relpath, **args):
        return self.redirect("./gui/index")
        
class App(WPApp):

    Version = Version

    def __init__(self, cfg, root, static_location="./static", **args):
        WPApp.__init__(self, root, **args)
        self.StaticLocation = static_location
        self.Cfg = cfg
        self.DefaultNamespace = cfg.get("default_namespace")
        
        self.DBCfg = cfg["database"]
        
        connstr = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % self.DBCfg
        
        self.DB = ConnectionPool(postgres=connstr, max_idle_connections=3)
        self.Filters = {}
        if "filters" in cfg:
            import metacat.filters as filters_mod
            #modname = cfg["filters"].get("module", "metacat.filters")
            
            #filters_mod = __import__(cfg["filters"].get("module", "metacat.filters"), 
            #                    globals(), locals(), [], 0)
            for n in cfg["filters"].get("names", []):
                self.Filters[n] = getattr(filters_mod, n)
                
        #
        # Authentication/authtorization
        #        
        self.Users = cfg["users"]       #   { username: { "passwrord":password }, ...}
        secret = cfg.get("secret") 
        if secret is None:    self.TokenSecret = secrets.token_bytes(128)     # used to sign tokens
        else:         
            h = hashlib.sha256()
            h.update(to_bytes(secret))      
            self.TokenSecret = h.digest()
        self.Tokens = {}                # { token id -> token object }

    def connect(self):
        conn = self.DB.connect()
        #print("conn: %x" % (id(conn),), "   idle connections:", ",".join("%x" % (id(c),) for c in self.DB.IdleConnections))
        return conn
        
    def get_password(self, realm, username):
        # realm is ignored for now
        return self.Users.get(username).get("password")

    TokenExpiration = 24*3600*7

    def user_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: return None
        try:    token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except: return None             # invalid token
        return token.Payload.get("user")

    def encoded_token_from_request(self, request):
        encoded = request.cookies.get("auth_token") or request.headers.get("X-Authentication-Token")
        if not encoded: return None
        try:    token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except: return None             # invalid token
        return encoded

    def response_with_auth_cookie(self, user, redirect):
        #print("response_with_auth_cookie: user:", user, "  redirect:", redirect)
        token = SignedToken({"user": user}, expiration=self.TokenExpiration).encode(self.TokenSecret)
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        #print ("response:", resp, "  reditrect=", redirect)
        resp.headers["X-Authentication-Token"] = to_str(token)
        resp.set_cookie("auth_token", token, max_age = int(self.TokenExpiration))
        return resp

    def response_with_unset_auth_cookie(self, redirect):
        if redirect:
            resp = Response(status=302, headers={"Location": redirect})
        else:
            resp = Response(status=200, content_type="text/plain")
        try:    resp.set_cookie("auth_token", "-", max_age=100)
        except: pass
        return resp

    def verify_token(self, encoded):
        try:
            token = SignedToken.decode(encoded, self.TokenSecret, verify_times=True)
        except Exception as e:
            return False, e
        return True, None
        

    def filters(self):
        return self.Filters
       
import yaml, os
import sys, getopt

opts, args = getopt.getopt(sys.argv[1:], "c:")
opts = dict(opts)
config = opts.get("-c", os.environ.get("METACAT_SERVER_CFG"))
if not config:
    print("Configuration file must be provided either using -c command line option or via METADATA_SERVER_CFG environment variable")
    sys.exit(1)
    
config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)  
cookie_path = config.get("cookie_path", "/metadata")
static_location = os.environ.get("METACAT_SERVER_STATIC_DIR", "./static")
static_location = config.get("static_location", static_location)
application=App(config, RootHandler, static_location=static_location)

templdir = config.get("templates", ".")
if templdir.startswith("$"):
    templdir = os.environ[templdir[1:]]

print("templdir=", templdir)

application.initJinjaEnvironment(
    tempdirs=[templdir, "."],
    globals={
        "GLOBAL_Version": Version, 
        "GLOBAL_SiteTitle": config.get("site_title", "DEMO Metadata Catalog")
    }
)
port = int(config.get("port", 8080))

if __name__ == "__main__":
    application.run_server(port)
else:
    # running under uwsgi
    pass
    
    
        
