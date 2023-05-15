from webpie import WPApp, WPHandler, Response, WPStaticHandler, sanitize
import psycopg2, json, time, secrets, traceback, hashlib, pprint
from metacat.db import DBFile, DBDataset, DBFileSet, DBNamedQuery, DBUser, DBNamespace, DBRole, DBParamCategory, \
        parse_name, AlreadyExistsError, IntegrityError
from wsdbtools import ConnectionPool
from urllib.parse import quote_plus, unquote_plus, unquote, quote
from metacat.util import to_str, to_bytes
from metacat.mql import MQLQuery, MQLError
from metacat import Version
from common_handler import MetaCatHandler, SanitizeException

class GUICategoryHandler(MetaCatHandler):
    
    def categories(self, request, relpath, **args):
        db = self.connect()
        cats = sorted(list(DBParamCategory.list(db)), key=lambda c:c.Path)
        return self.render_to_response("categories.html", categories=cats, **self.messages(args))
        
    index = categories

    @sanitize()
    def show(self, request, relpath, path=None):
        me, auth_error = self.authenticated_user()
        db = self.connect()
        cat = DBParamCategory.get(db, path)
        admin = me.is_admin() if me is not None else False
        edit = me is not None and (me.Username in cat.owners() or admin)
        roles = sorted([r.Name for r in DBRole.list(db)]) if admin else (
            list(me.roles) if me is not None else [])
        users = sorted(list(u.Username for u in DBUser.list(db))) if admin else [me.Username if me is not None else None]
        cats = list(DBParamCategory.list(db))
        print("GUICategoryHandler.show(): category definitions:")
        for name, d in cat.Definitions.items():
            print(name, d)
        return self.render_to_response("category.html", category=cat, edit=edit, create=False, roles=roles, admin=admin, user=me,
            users = users,
            types = DBParamCategory.Types)
        
    @sanitize()
    def create(self, request, relpath):
        db = self.connect()
        me, auth_error = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/create")
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
                #print("param k, id, name:", k, param_id, name)
                if name:
                    if form.get(f"param:{param_id}:remove"):
                        removals.append(name)
                    else:
                        type = form.get(f"param:{param_id}:type")
                        #print("name, type:", name, type)
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
        me, auth_error = self.authenticated_user()
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
        cat.create()
        self.redirect(f"./show?path={path}")
        
    @sanitize(exclude="description")
    def save(self, request, relpath):
        db = self.connect()
        me, auth_error = self.authenticated_user()
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
        #print("owner_user, owner_role:", cat.OwnerUser, cat.OwnerRole)
            
        cat.Description = request.POST["description"]
        cat.Restricted = "restricted" in request.POST
        defs = self.read_parameter_definitions(request.POST)
        cat.Definitions = defs
        cat.save()
        self.redirect(f"./show?path={path}")
        
    @sanitize()
    def remove_definition(self, request, relpath, path=None, param=None):
        db = self.connect()
        me, auth_error = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/categories/index")
        cat = DBParamCategory.get(db, path)
        if cat is None:
            self.redirect("./index?error=%s" % (quote_plus(f"Category does not exist"),))
        if not (me.is_admin() or me in cat.Owner):
            self.redirect("./show?path=%s&error=%s" % (path, quote_plus(f"Permission denied"),))
        defs = cat.definitions
        
class GUIHandler(MetaCatHandler):
    
    def __init__(self, request, app):
        MetaCatHandler.__init__(self, request, app)
        self.categories = GUICategoryHandler(request, app)
        
    @sanitize()
    def index(self, request, relpath, error=None, message=None, **args):
        url = "./datasets"
        if error or message:
            messages = []
            if error:   messages.append("error=" + error)       # assume they are quoted already
            if message:   messages.append("message=" + message)
            url += "?" + "&".join(messages)
        return self.redirect(url)
        
    @sanitize()
    def mql(self, request, relpath, **args):
        namespace = request.POST.get("namespace")
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

    @sanitize()
    def show_file(self, request, relpath, fid=None, namespace=None, name=None, did=None, show_form="no", **args):
        db = self.connect()
        f = None
        namespace=namespace and unquote(namespace)
        #name=name and unquote(name)
        #did=did and unquote(did)
        #fid=fid and unquote(fid)
        if fid:
            f = DBFile.get(db, fid=fid, with_metadata=True)
        else:
            if did:
                try:    namespace, name = parse_name(did)
                except Exception as e:
                    #print(e)
                    self.redirect("./show_file?error=%s&show_form=yes" % (quote_plus("invalid DID format"),))
            if namespace and name:
                f = DBFile.get(db, namespace=namespace, name=name, with_metadata=True)
        show_form = show_form == "yes"
        return self.render_to_response("show_file.html", f=f, show_form=show_form, namespace=namespace, name=name, did=did, fid=fid)
        
    @sanitize()
    def find_file(self, request, relpath, **args):
        self.redirect("./show_file?show_form=yes")

    def _meta_stats(self, files):
        #
        # returns [ (meta_name, [(value, count), ...]) ... ]
        #
        stats = {}
        for f in files:
            for n, v in f.Metadata.items():
                if isinstance(v, (dict, list)): v = repr(v)
                elif v is None: v = "null"
                n_dict = stats.setdefault(n, {})
                try:
                    count = n_dict.setdefault(v, 0)
                except:
                    v = repr(v)
                    count = n_dict.setdefault(v, 0)
                n_dict[v] = count + 1
        out = []
        for name, counts in stats.items():
            clist = []
            for v, c in counts.items():
                if isinstance(v, tuple):    v = list(v)
                clist.append((v, c))
            out.append((name, sorted(clist, key=lambda vc: (-vc[1], str(vc[0])))))
        return sorted(out)
        
    def filters(self, request, relpath, **args):
        return self.render_to_response("filters.html", standard=self.App.StandardFilters, custom=self.App.CustomFilters)

    @sanitize(exclude="query")
    def query(self, request, relpath, query="", namespace=None, action="show", 
                    include_retired_files="off", view_meta_as="json", **args):
        
        db = self.App.connect()
        user, auth_error = self.authenticated_user()
        user_namespace = None
        if user is not None:
            if DBNamespace.exists(db, user.Username):
                user_namespace = user.Username
        namespace = namespace or request.POST.get("default_namespace") \
            or user_namespace or None
        #print("namespace:", namespace)
        query_text = unquote(query)
        results = None
        url_query = None
        files = None
        datasets = None
        runtime = None
        meta_stats = None
        namespaces = None
        
        #print("query: method:", request.method)
        error = None
        message = None
        query_type = None
        
        include_retired_files = include_retired_files in ("on", "yes")
        save_as_dataset = request.GET.get("save_as_dataset", "no") == "on"
        with_meta = not not view_meta_as

        if not query:   action = "show"

        if action == "run":
            t0 = time.time()
            if query_text:
                url_query = query_text.replace("\n"," ")
                while "  " in url_query:
                    url_query = url_query.replace("  ", " ")
                url_query = quote_plus(url_query)
                if namespace: url_query += "&namespace=%s" % (namespace,)
                #print("with_meta=", with_meta)
                parsed = MQLQuery.parse(query_text, 
                                        db=db, 
                                        default_namespace=namespace or None, 
                                        include_retired_files=include_retired_files
                )
                query_type = parsed.Type
                #print("Server.query: with_meta:", with_meta)
                try:
                    results = parsed.run(db, filters=self.App.filters(),
                        limit=1000 if not save_as_dataset else None, 
                        with_meta=with_meta)
                except MQLError as e:
                    error = str(e)
                    results = []
                    url_query = None
            else:
                results = None
                url_query = None

            results = None if results is None else list(results)
            if query_type=="dataset":
                datasets = results
            else:
                files = results
            
            #print("files:", type(files), files)
            meta_stats = None if (not with_meta or parsed.Type=="dataset") else self._meta_stats(files)
            #print("meta_stats:", meta_stats, "    with_meta:", with_meta, request.POST.get("with_meta"))
                
            #print("query: results:", len(files))
            runtime = time.time() - t0

        #print("Server.query: namespace:", namespace)
        if user:
            namespaces = list(DBNamespace.list(db, owned_by_user=user))
            
            #print("query: namespaces:", [ns.Name for ns in namespaces])
        
        
        if files is not None and save_as_dataset:
            if user is None:
                error = "Unauthenticated user"
            else:
                dataset_namespace = request.GET["save_as_dataset_namespace"]
                dataset_name = request.GET["save_as_dataset_name"]
            
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
                        ds = DBDataset(db, dataset_namespace, dataset_name, creator=user.Username)
                        ds.create()
                        files = list(files)
                        ds.add_files(files)
                        message = "Dataset %s:%s with %d files created" % (dataset_namespace, dataset_name, len(files))
                            
        attr_names = set()
        if files is not None:
            files = files[:1000]
            #print("GUI server.query(): files[:10]:", files[:10])
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
            query=query_text, url_query=url_query,
            show_files=files is not None, files=files, 
            show_datasets=datasets is not None,datasets = datasets,
            runtime = runtime, meta_stats = meta_stats, with_meta = with_meta,
            include_retired_files = include_retired_files,
            namespace=namespace or "")
        return resp
        
    @sanitize()
    def named_queries(self, request, relpath, namespace=None, **args):
        me, auth_error = self.authenticated_user()
        db = self.App.connect()
        queries = list(DBNamedQuery.list(db, namespace))
        return self.render_to_response("named_queries.html", namespace=namespace, queries = queries, logged_in = me is not None,
                **self.messages(args))
            
    @sanitize()
    def named_query(self, request, relpath, name=None, edit="no", **args):
        namespace, name = parse_name(name, None)
        db = self.App.connect()
        query = DBNamedQuery.get(db, namespace, name)
        return self.render_to_response("named_query.html", query=query, edit = edit=="yes")

    def create_named_query(self, request, relapth, **args):
        me, auth_error = self.authenticated_user()
        if me is None:   
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_named_query")
        return self.render_to_response("named_query.html", namespaces=me.namespaces(), create=True)

    @sanitize(exclude="text")
    def save_named_query(self, request, relpath, **args):
        user, auth_error = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/named_queries")

        admin = user.is_admin()
        name = request.POST["name"]
        namespace = request.POST["namespace"]
        db = self.App.connect()
        if not admin:
            ns = DBNamespace.get(db, namespace)
            if not ns.owned_by_user(user):
                self.redirect(f"./named_queries?error=%s" % (quote_plus(f"No permission to modify namespace {namespace}"),))

        create = request.POST["create"] == "yes"
        query_text = request.POST["text"]

        query = MQLQuery.parse(query_text)
        if query.Type != "file":
            self.redirect("./named_queries?error=%s" % (quote_plus("Only file queries can be saved"),))
        
        if create:
            query = DBNamedQuery(db, name=name, namespace=namespace, source=query_text)
            query.Creator = user.Username
            query.create()
        else:
            query = DBNamedQuery.get(db, namespace, name)
            query.Source = query_text
            query.save()
        
        return self.render_to_response("named_query.html", query=query, edit = True)
        
    @sanitize()
    def users(self, request, relpath, error="", **args):
        me, auth_error = self.authenticated_user()
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
        
    @sanitize()
    def user(self, request, relpath, username=None, error="", message="", **args):
        username = username or relpath
        #print("GUI.user(): username:", username)
        db = self.App.connect()
        user = DBUser.get(db, username)
        me, auth_error = self.authenticated_user()
        if me is None:
            self.redirect("../auth/login")
        all_roles = DBRole.list(db)
        role_set = set(user.roles)
        #print("role_set:", role_set)
        roles = (DBRole.get(db, r) for r in role_set)
        #print("user: roles:", list(roles))
        ldap_config = self.App.auth_config("ldap")
        ldap_url = ldap_config and ldap_config["server_url"]
        dn_list = user.get_dns()
        return self.render_to_response("user.html", all_roles=all_roles, user=user, roles=roles, role_set=role_set, 
            ldap_url = ldap_url, realm = self.App.Realm,
            error = unquote_plus(error), message=unquote_plus(message),
            mode = "edit" if (me.is_admin() or me.Username==username) else "view", 
            its_me = me.Username==username,
            digest_realm = self.App.Realm,
            dn_list = dn_list, 
            admin=me.is_admin()
        )
            
    @sanitize()
    def create_user(self, request, relpath, error="", **args):
        db = self.App.connect()
        me, auth_error = self.authenticated_user()
        if not me.is_admin():
            self.redirect("./users?error=%s" % (quote_plus("Not authorized to create users")))
        return self.render_to_response("user.html", error=error, mode="create", all_roles = DBRole.list(db),
            digest_realm = self.App.Realm)
        
    @sanitize()
    def save_user(self, request, relpath, **args):
        db = self.App.connect()
        username = request.POST["username"]
        me, auth_error = self.authenticated_user()
        if not (me and (me.is_admin() or me.Username == username)):
            self.redirect(f"./user?username={username}&message="+quote_plus("Not authorized"))
        
        new_user = request.POST["new_user"] == "yes"
        
        u = DBUser.get(db, username)
        if u is None:   
            if not new_user:    
                self.redirect("./users?error=%s", quote_plus("user not found"))
            u = DBUser(db, username, request.POST["name"], request.POST["email"], request.POST["flags"], {}, request.POST.get("auid") or None)
        else:
            u.Name = request.POST["name"]
            u.EMail = request.POST["email"]
            if me.is_admin():   u.Flags = request.POST["flags"]
            u.AUID = request.POST.get("auid") or None
            
        if "save_user" in request.POST:
            hashed_password = request.POST.get("hashed_password")
            if hashed_password:
                u.set_password(self.App.Realm, hashed_password, hashed=True)
            
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
        elif "add_dn" in request.POST:
            dn = request.POST.get("new_dn")
            if dn:
                dn_list = u.get_dns()
                if not dn in dn_list:
                    dn_list.append(dn)
                    u.set_dns(dn_list)
                    u.save()
        else:
            for k in request.POST:
                if k.startswith("remove_dn:"):
                    dn_to_remove = k.split(":",1)[-1]
                    break
            else:
                dn_to_remove = None
            if dn_to_remove:
                dn_list = u.get_dns()
                while dn_to_remove in dn_list:
                    dn_list.remove(dn_to_remove)
                u.set_dns(dn_list)
                u.save()
        self.redirect(f"./user?username={username}")

    def generate_token(self, request, relpath, **args):
        db = self.App.connect()
        me, auth_error = self.authenticated_user()
        expiration = int(request.POST["token_expiration"])
        token, encoded = self.App.generate_token(me.Username, expiration=expiration)
        n = len(encoded)
        encoded_lines = [encoded[i:i+64] for i in range(0, n, 64)]
        #print(payload)
        return self.render_to_response("show_token.html", user=me, token=token, encoded=encoded, encoded_lines=encoded_lines)

#
# --- namespaces
#

    def namespaces(self, request, relpath, all="no", **args):
        user, auth_error = self.authenticated_user()
        db = self.App.connect()
        all = all == "yes"
        if all:
            namespaces = DBNamespace.list(db)
        else:
            me, auth_error = self.authenticated_user()
            namespaces = DBNamespace.list(db, owned_by_user=me)
        namespaces = sorted(namespaces, key=lambda ns: ns.Name)
        return self.render_to_response("namespaces.html", namespaces=namespaces, logged_in=user is not None, showing_all=all, **self.messages(args))

    @sanitize()
    def namespace(self, request, relpath, name=None, **args):
        db = self.App.connect()
        ns = DBNamespace.get(db, name)
        roles = []
        edit = False
        me, auth_error = self.authenticated_user()
        admin = False
        users = None
        if me is not None:
            admin = me.is_admin()
            edit = admin or ns.owned_by_user(me)
            roles = DBRole.list(db) if admin else [DBRole.get(db, r) for r in me.roles]
            users = DBUser.list(db) if admin else [me]
        datasets = DBDataset.list(db, namespace=name) if ns is not None else None
        #print("namespace: roles", roles)
        return self.render_to_response("namespace.html", user=me, namespace=ns, edit=edit, create=False, roles=roles, users=users, admin=admin, 
            datasets = datasets,
            **self.messages(args))
        
    def create_namespace(self, request, relpath, error="", **args):
        db = self.App.connect()
        me, auth_error = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_namespace")
        admin = me.is_admin()
        roles = DBRole.list(db) if admin else [DBRole.get(db, r) for r in me.roles]
        users = DBUser.list(db) if admin else [me]
        return self.render_to_response("namespace.html", user=me, roles=roles, users=users, create=True, edit=False, error=unquote_plus(error))

    @sanitize(exclude="description")
    def save_namespace(self, request, relpath, **args):
        db = self.App.connect()
        me, auth_error = self.authenticated_user()
        if not me:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/namespaces")
            
        admin = me.is_admin()
        name = request.POST["name"]
        description = request.POST["description"]
        create = request.POST["create"] == "yes"

        ns = DBNamespace.get(db, name)
        if ns is None and not create:
            self.redirect("./namespaces?error=%s" % (quote_plus("Namespace not found"),))
        elif ns is not None and create:
            self.redirect("./namespace?name=%s&error=%s" % (name, quote_plus("Namespace already exists")))            

        owner_user = owner_role = None
        ownership = request.POST["ownership"]
        if ownership == "user":
            owner_user = request.POST["owner_user"]
        else:
            owner_role = request.POST["owner_role"]
        assert (owner_user is None) != (owner_role is None)

        if ns is None:
            # create new
            if not admin:
                if owner_user and owner_user != me.Username or \
                    owner_role and not owner_role in me.roles:
                        self.redirect("./namespaces?error=%s" % (quote_plus("Not authorized"),))                    
            ns = DBNamespace(db, name, owner_role=owner_role, owner_user=owner_user, description=description).create()
        else:
            if not admin and not ns.owned_by_user(me):
                self.redirect("./namespaces?error=%s" % (quote_plus("Not authorized"),))
            ns.Description = description
            if admin:
                ns.OwnerUser = owner_user
                ns.OwnerRole = owner_role
            ns.save()
        self.redirect("./namespaces")
    
    @sanitize()
    def datasets(self, request, relpath, selection=None, **args):
        user, auth_error = self.authenticated_user()
        admin = user is not None and user.is_admin()
        db = self.App.connect()

        all_namespaces = {ns.Name:ns for ns in DBNamespace.list(db)}
        owned_namespaces = []
        other_namespaces = sorted(all_namespaces.keys())
        selection = selection or ("user" if user is not None else None) or "all"
        
        if user is not None:
            owned_namespaces = sorted([ns.Name for ns in DBNamespace.list(db, owned_by_user=user.Username)])
            other_namespaces = sorted([name for name in all_namespaces.keys() if name not in owned_namespaces])
        if selection == "user":
            datasets = DBDataset.list(db, namespaces=owned_namespaces)
        elif selection.startswith("namespace:"):
            ns = selection[len("namespace:"):]
            datasets = DBDataset.list(db, namespace=ns)
        else:
            # assume selection == "all"
            datasets = DBDataset.list(db)
            
        datasets = sorted(datasets, key=lambda x: (x.Namespace, x.Name))
                
        for ds in datasets:
            ns = all_namespaces[ds.Namespace]
            ds.GUI_OwnerUser = ns.OwnerUser
            ds.GUI_OwnerRole = ns.OwnerRole
            ds.GUI_Authorized = user is not None and (admin or self._namespace_authorized(db, ds.Namespace, user))
            #ds.GUI_Children = sorted(ds.children(), key=lambda x: (x.Namespace, x.Name))
            #ds.GUI_Parents = sorted(ds.parents(), key=lambda x: (x.Namespace, x.Name))
        return self.render_to_response("datasets.html", datasets=datasets,
            owned_namespaces = owned_namespaces, other_namespaces=other_namespaces,
            selection=selection, user=user, **self.messages(args))

    def create_dataset(self, request, relpath, **args):
        user, auth_error = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_dataset")
        admin = user.is_admin()
        db = self.App.connect()
        namespaces = sorted(DBNamespace.list(db, owned_by_user=user if not admin else None), 
            key=lambda x, u=user: (0 if x.owned_by_user(u, directly=True) else 1, x.Name)
        )
        #print("create_dataset: amdin:", admin, "   namespaces:", namespaces)
        if not namespaces:
            self.redirect("./create_namespace?error=%s" % (quote_plus("You do not own any namespace. Create one first"),))
        return self.render_to_response("dataset.html", namespaces=namespaces, edit=False, create=True, mode="create")
        
    @sanitize()
    def dataset(self, request, relpath, namespace=None, name=None, **args):
        db = self.App.connect()
        dataset = DBDataset.get(db, namespace, name)
        if dataset is None: self.redirect("./datasets")

        nfiles = dataset.nfiles(exact=True)
        files = sorted(list(dataset.list_files(with_metadata=True, limit=1000)), key = lambda x: x.Name)
        #print ("files:", len(files))
        attr_names = set()
        for f in files:
            if f.Metadata:
                for n in f.Metadata.keys():
                    attr_names.add(n)
        attr_names=sorted(list(attr_names))

        user, auth_error = self.authenticated_user()
        edit = False
        mode = "view"
        if user is not None:
            ns = DBNamespace.get(db, dataset.Namespace)
            edit = ns.owned_by_user(user)
            if edit:
                mode = "edit"
        namespaces = DBNamespace.list(db)
        namespaces = sorted(namespaces, key = lambda ns: (0 if ns.owned_by_user(user, directly=True) else 1, ns.Name))
        return self.render_to_response("dataset.html", dataset=dataset, files=files, nfiles=nfiles, attr_names=attr_names, 
            mode = mode,
            edit=edit, 
            create=False, namespaces=namespaces, **self.messages(args))
            
    @sanitize()
    def child_subset_candidates(self, request, relpath, namespace=None, prefix=None, ds_namespace=None, ds_name=None, **args):
        db = self.App.connect()
        datasets = set((d.Namespace, d.Name) for d in DBDataset.list(db, namespace=namespace))
        ds = DBDataset.get(db, ds_namespace, ds_name)
        if ds is not None:
            children = set((c.Namespace, c.Name) for c in ds.children())
            ancestors = set((a.Namespace, a.Name) for a in ds.ancestors())
            datasets = datasets - children - ancestors
        return json.dumps({"namespace": namespace, "names": sorted([name for namespace, name in datasets])}), "text/json"

    @sanitize()
    def delete_dataset(self, request, relpath, namespace=None, name=None, **args):
        user, auth_error = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/datasets")
        admin = user.is_admin()
        db = self.App.connect()
        if not (admin or self._namespace_authorized(db, namespace, user)):
            self.redirect("./datasets?error=%s" % (quote_plus("Not authorized"),))
        dataset = DBDataset.get(db, namespace, name)
        if dataset.has_children():
            self.redirect(f"./dataset?namespace={namespace}&name={name}&error=%s" % (quote_plus("Dataset has child datasets"),))
        dataset.delete()
        self.redirect("./datasets")

    def read_dataset_file_meta_requiremets(self, form):
        
        def cvt_value(x):
            if x is None:   return None
            if x == "true": x = True
            elif x == "false":  x = False
            elif x == "null":   x = None
            else:
                try:    x = int(x)
                except: 
                    try:    x = float(x)
                    except: pass
            if isinstance(x, str) and len(x) >= 2:
                if x[0] == "'" and x[-1] == "'" or \
                    x[0] == '"' and x[-1] == '"':
                        x = x[1:-1]
            return x
        
        reqs = {}
        removals = []
        for k, v in form.items():
            if k.startswith("param:") and k.endswith(":name"):
                param_id = k.split(":", 2)[1]
                name = form.get(f"param:{param_id}:name")
                #print("param k, id, name:", k, param_id, name)
                if name:
                    if form.get(f"param:{param_id}:remove"):
                        removals.append(name)
                    else:
                        required = form.get(f"param:{param_id}:required") and True
                        #print("name, type:", name, type)
                        values = form.get(f"param:{param_id}:values", "")
                        values = [cvt_value(x) for x in values.split(",")] if values else None
                        minv = cvt_value(form.get(f"param:{param_id}:min", "").strip() or None)
                        maxv = cvt_value(form.get(f"param:{param_id}:max", "").strip() or None)                            
                        pattern = form.get(f"param:{param_id}:pattern").strip() or None
                        req = {}
                        if required:            req["required"] = True
                        if minv is not None:    req["min"] = minv
                        if maxv is not None:    req["max"] = maxv
                        if values is not None:    req["values"] = values
                        if pattern is not None:    req["pattern"] = pattern
                        reqs[name] = req
                        #print("pdef:", pdef)
        for n in removals:
            if n in reqs:
                del reqs[n]
        return reqs

    @sanitize()
    def save_dataset(self, request, relpath, **args):
        #print("save_dataset:...")
        db = self.App.connect()
        user, auth_error = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/datasets")
        admin = user.is_admin()
        namespace = request.POST["namespace"]
        name = request.POST["name"]
        if not admin:
            ns = DBNamespace.get(db, namespace)
            if not ns.owned_by_user(user):
                self.redirect(f"./datasets?error=%s" % (quote_plus(f"No permission to modify namespace {namespace}"),))

        if request.POST["create"] == "yes":
            ds = DBDataset(db, namespace, name, creator=user.Username)
        else:
            ds = DBDataset.get(db, namespace, name)

        warning = None
        mode = request.POST["mode"]

        ds.Monotonic = "monotonic" in request.POST
        ds.Frozen = "frozen" in request.POST
        reqs = self.read_dataset_file_meta_requiremets(request.POST)
        ds.FileMetaRequirements = reqs

        if mode == "edit" and request.POST.get("add_child_dataset") == "add":
            child_namespace = request.POST["child_namespace"]
            child_name = request.POST["child_name"]
            if child_namespace and child_name:
                child = DBDataset.get(db, child_namespace, child_name)
                if child is None:
                    self.redirect("./datasets?error=%s" % (quote_plus(f"Child dataset {child_namespace}:{child_name} not found"),))
                ancestors = list(ds.ancestors())
                #print("Ancestors: of", ds, ":", *ancestors)
                subsets = list(ds.subsets())
                #print("Subsets:", *subsets)
                if any(a.Namespace == child_namespace and a.Name == child_name for a in ancestors):
                    self.redirect("./datasets?error=%s" % (quote_plus(f"Circular dependency detected"),))
                if any(a.Namespace == child_namespace and a.Name == child_name for a in subsets):
                    warning = f"Dataset {child_namespace}:{child_name} is already a subset of {namespace}:{name}"
                ds.add_child(child)
        elif mode == "create":
            ds.create()
        elif mode == "edit":
            ds.save()
            
        self.redirect(f"./dataset?namespace={namespace}&name={name}" + ("&message=" + quote_plus(warning) if warning else ""))
        
    @sanitize()
    def remove_child_dataset(self, request, relpath, namespace=None, name=None, child_namespace=None, child_name=None, **args):
        user, auth_error = self.authenticated_user()
        if not user:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/datasets")
        db = self.App.connect()
        admin = user.is_admin()
        dataset = DBDataset.get(db, namespace, name)
        if not dataset:
            self.redirect("./datasets")
        if not admin:
            ns = DBNamespace.get(db, namespace)
            if not ns.owned_by_user(user):
                self.redirect(f"./datasets?error=%s" % (quote_plus(f"No permission to modify dataset in namespace {namespace}"),))
        
        children = dataset.children()
        if not any(child.Namespace==child_namespace and child.Name==child_name for child in children):
            self.redirect(f"./dataset?namespace={namespace}&name={name}&error=%s" % (quote_plus(f"Child dataset {child_namespace}:{child_name} not found"),))
        dataset.remove_child(DBDataset(db, child_namespace, child_name))
        self.redirect(f"./dataset?namespace={namespace}&name={name}&message=%s" % (quote_plus(f"Child {child_namespace}:{child_name} removed"),))

#
# --- roles
#
    def roles(self, request, relpath, **args):
        me, auth_error = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        db = self.App.connect()
        roles = DBRole.list(db)
        admin = me.is_admin()
        return self.render_to_response("roles.html", roles=roles, edit=admin, create=admin, **self.messages(args))
        
    @sanitize()
    def role(self, request, relpath, name=None, **args):
        name = name or relpath
        me, auth_error = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/roles")
        admin = me.is_admin()
        db = self.App.connect()
        role = DBRole.get(db, name)
        users = sorted(list(role.members))
        #print("all_users:", all_users)
        return self.render_to_response("role.html", role=role, users=users, edit=admin or me in role, create=False, **self.messages(args))

    def create_role(self, request, relpath, **args):
        me, auth_error = self.authenticated_user()
        if me is None:
            self.redirect(self.scriptUri() + "/auth/login?redirect=" + self.scriptUri() + "/gui/create_role")
        if not me.is_admin():
            self.redirect("./roles")
        db = self.App.connect()
        all_users = list(DBUser.list(db))
        return self.render_to_response("role.html", all_users=all_users, edit=False, create=True)
        
    @sanitize(exclude="description")
    def save_role(self, request, relpath, **args):
        me, auth_error = self.authenticated_user()
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
