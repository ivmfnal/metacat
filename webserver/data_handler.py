from webpie import WPApp, WPHandler, Response, WPStaticHandler
import psycopg2, json, time, secrets, traceback, hashlib, pprint
from metacat.db import DBFile, DBDataset, DBFileSet, DBNamedQuery, DBUser, DBNamespace, DBRole, \
    DBParamCategory, parse_name, AlreadyExistsError, IntegrityError
from wsdbtools import ConnectionPool
from urllib.parse import quote_plus, unquote_plus
from metacat.util import to_str, to_bytes, SignedToken
from metacat.mql import MQLQuery
from metacat import Version

from base_handler import BaseHandler

class DataHandler(BaseHandler):
    
    def __init__(self, request, app):
        BaseHandler.__init__(self, request, app)
        self.NamespaceAuthorizations = {}                # namespace -> True/False
        self.Categories = None
        
    def load_categories(self):
        if self.Categories is None:
            db = self.App.connect()
            self.Categories = {c.Path:c for c in DBParamCategory.list(db)}
        return self.Categories
        
    def _namespace_authorized(self, db, namespace, user):
        authorized = self.NamespaceAuthorizations.get(namespace)
        if authorized is None:
            ns = DBNamespace.get(db, namespace)
            if ns is None:
                raise KeyError("Namespace %s does not exist")
            authorized = ns.owned_by_user(user)
            self.NamespaceAuthorizations[namespace] = authorized
        return authorized
        
    def json_generator(self, lst, page_size=10000):
        from collections.abc import Iterable
        assert isinstance(lst, Iterable)
        yield "["
        first = True
        n = 0
        page = []
        for x in lst:
            j = json.dumps(x)
            if not first: j = ","+j
            page.append(j)
            if len(page) >= page_size:
                yield "".join(page)
                page = []
            first = False
        if page:
            yield "".join(page)
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
        
    def create_namespace(self, request, relpath, name=None, owner_role=None, description=None, **args):
        db = self.App.connect()
        user = self.authenticated_user()
        if user is None:
            return 401
        owner_user = None
        if owner_role is None:
            owner_user = user.Name
        else:
            r = DBRole.get(db, owner_role)
            if not user.is_admin() and not user.Name in r.members:
                return 403

        if DBNamespace.exists(db, name):
            return "Namespace already exists", 400

        if description:
            description = unquote_plus(description)
            
        ns = DBNamespace(db, name, owner_role = owner_role, owner_user=owner_user, description=description)
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

    def namespace_counts(self, request, relpath, namespace=None, **args):
        db = self.App.connect()
        ns = DBNamespace.get(db, namespace)
        out = json.dumps(
            dict(
                nfiles = ns.file_count(),
                ndatasets = ns.dataset_count(),
                nqueries = ns.query_count()
            )
        )
        return out, {"Content-Type":"text/json",
            "Access-Control-Allow-Origin":"*"
        } 
        
    def create_dataset(self, request, relpath, dataset=None, parent=None, frozen="no", monotonic="no", description="", **args):
        frozen = frozen == "yes"
        monotonic = monotonic == "yes"
        user = self.authenticated_user()
        if user is None:
            return 401
        db = self.App.connect()
        namespace, name = dataset.split(":",1)
        namespace = DBNamespace.get(db, namespace)
        if not user.is_admin() and not namespace.owned_by_user(user):
            return 403
        if DBDataset.get(db, namespace, name) is not None:
            return "Already exists", 409
        parent_ds = None
        if parent:
                parent_namespace, parent_name = parent.split(":",1)
                parent_ns = DBNamespace.get(db, parent_namespace)
                if not user.is_admin() and not parent_ns.owned_by_user(user):
                        return 403
                parent_ds = DBDataset.get(db, parent_namespace, parent_name)
                if parent_ds is None:
                        return "Parent dataset not found", 404
                dataset = DBDataset(db, namespace, name, 
                        parent_namespace=parent_namespace, parent_name=parent_name)
        else:
                dataset = DBDataset(db, namespace, name)
        dataset.Creator = user.Username
        dataset.Frozen = frozen
        dataset.Monotonic = monotonic
        dataset.Description = description
        dataset.save()
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
                namespace, name = parse_name(file_item["name"], file_item.get("namespace") or default_namespace)
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
        
    def split_cat(self, path):
        if '.' in path:
            return tuple(path.rsplit(".",1))
        else:
            return ".", path
        
        
    def validate_metadata(self, data):
        categories = self.load_categories()
        invalid = []
        for k, v in data.items():
            path, name = self.split_cat(k)
            cat = categories.get(path)
            if cat is None:
                while True:
                    path, _ = self.split_cat(path)
                    if path in categories:
                        if categories[path].Restricted:
                            invalid.append({"name":k, "value":v, "reason":f"Category {path} is restricted"})
                        break
                    if path == ".":
                        break
            else:
                valid, reason = cat.validate_parameter(name, v)
                if not valid:
                    invalid.append({"name":k, "value":v, "reason":reason})
        return invalid
        
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
        #               checksums: {            // optional
        #                 "method":"value", ...
        #               }
        #       },...
        # ]
        #               
        default_namespace = namespace
        user = self.authenticated_user()
        if user is None:
            print("Unauthenticated user")
            return "Unauthenticated user", 401
            
        if dataset is None:
            return "Dataset not specified", 400
            
        db = self.App.connect()

        ds_namespace, ds_name = parse_name(dataset, default_namespace)
        try:
            if not self._namespace_authorized(db, ds_namespace, user):
                return f"Permission to declare files in namespace {namespace} denied", 403
        except KeyError:
            return f"Namespace {ds_namespace} does not exist", 404

        ds = DBDataset.get(db, namespace=ds_namespace, name=ds_name)
        if ds is None:
            return f"Dataset {ds_namespace}:{ds_name} does not exist", 404
            
        file_list = json.loads(request.body) if request.body else []
        if not file_list:
                return "Empty file list", 400
        files = []
        
        errors = []
        
        for file_item in file_list:
            name = file_item.get("name")
            fid = file_item.get("fid")
            if name is None:
                errors.append({
                    "message":"Missing filename",
                    "fid":fid
                })
                continue
            
            namespace, name = parse_name(file_item["name"], file_item.get("namespace") or default_namespace)
            if not namespace:
                errors.append({
                    "message":"Missing namespace",
                    "fid":fid
                })
                continue
                
            try:
                if not self._namespace_authorized(db, namespace, user):
                    errors.append({
                        "message":f"Permission to declare files to namespace {namespace} denied"
                    })
                    continue
            except KeyError:
                    errors.append({
                        "message":f"Namespace {namespace} does not exist"
                    })
                    continue
                    
            meta = file_item.get("metadata", {})
            metadata_errors = self.validate_metadata(meta)
            if metadata_errors:
                errors.append({
                    "message":"Metadata validation errors",
                    "metadata_errors":metadata_errors
                })
                continue
                
            f = DBFile(db, namespace=namespace, name=name, fid=file_item.get("fid"), metadata=meta)
            f.Parents = file_item.get("parents")
            f.Checksums = file_item.get("checksums")
            files.append(f)

        if errors:
            return json.dumps(errors), 400, "text/json"
            
        try:    results = DBFile.create_many(db, files)
        except IntegrityError as e:
            return f"Integrity error: {e}", 404
            
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
        
        metadata_errors = self.validate_metadata(new_meta)
        if metadata_errors:
            return json.dumps({
                "message":"Metadata validation errors",
                "metadata_errors":metadata_errors
            }), 400, "text/json"
        
        if (names is None) == (ids is None):
            return "Either file ids or names must be specified, but not both", 400
            
        if ids:
            file_set = DBFileSet.from_id_list(db, ids)
        else:
            file_set = DBFileSet.from_name_list(db, names, default_namespace=default_namespace)
        file_set = list(file_set)
        out = []
        for f in file_set:
            namespace = f.Namespace
            try:
                if not self._namespace_authorized(db, namespace, user):
                    return f"Permission to update files in namespace {namespace} denied", 403
            except KeyError:
                return f"Namespace {namespace} does not exist", 404
            
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
        
                
    def update_file_meta(self, request, relpath, namespace=None, mode="update", **args):
        # mode can be "update" - add/pdate metadata with new values
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
        #               metadata: { ... },       // optional
        #               checksums: { ...}       // optional
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

        if isinstance(data, dict):
            return self.update_meta_bulk(db, user, data, mode, default_namespace)
        else:
            return "Not implemented", 400
        
        file_list = json.loads(request.body) if request.body else []
        if not file_list:
                return "Empty file list", 400
        files = []
        
        errors = []
        
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
            try:
                if not self._namespace_authorized(db, namespace, user):
                    return f"Permission to update files in namespace {namespace} denied", 403
            except KeyError:
                return f"Namespace {namespace} does not exist", 404
            
            if "metadata" in file_item:
                if mode == "update":
                    f.Metadata.update(file_item["metadata"])
                else:
                    f.Metadata = file_item["metadata"]
            
            if "checksums" in file_item:
                if mode == "update":
                    f.Checksums.update(file_item["checksums"])
                else:
                    f.Checksums = file_item["checksums"]
                
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
        
    def update_dataset_meta(self, request, relapth, mode="update", dataset=None):
        #
        # update or replace single dataset metadata
        #
        if not mode in ("update","replace"):
            return "Invalid mode {mode}", 400
        namespace, name = parse_name(dataset, None)
        if not namespace:
            return "Namespace is not specfied", 400

        user = self.authenticated_user()
        if user is None:
            return 403
        db = self.App.connect()
        meta = json.loads(request.body)

        try:
            if not self._namespace_authorized(db, namespace, user):
                return f"Permission to update files in namespace {namespace} denied", 403
        except KeyError:
            return f"Namespace {namespace} does not exist", 404
                
        ds = DBDataset.get(db, namespace, name)
        if mode == "update":
            ds.Metadata.update(meta)
        else:
            ds.Metadata = meta
        
        ds.save()
        return json.dumps(ds.Metadata), "text/json"
                
    def file(self, request, relpath, name=None, fid=None, with_metadata="yes", with_provenance="yes", **args):
        if name:
            namespace, name = parse_name(name, None)
            if not namespace:
                return "Namespace is not specfied", 400
        else:
            if not fid:
                return "Either namespace/name or fid must be specified", 400
        
        
        with_metadata = with_metadata == "yes"
        with_provenance = with_provenance == "yes"
        
        db = self.App.connect()
        if fid:
            f = DBFile.get(db, fid = fid)
        else:
            f = DBFile.get(db, namespace=namespace, name=name)
        return f.to_json(with_metadata=with_metadata, with_provenance=with_provenance), "text/json"
            
    def query(self, request, relpath, query=None, namespace=None, 
                    with_meta="no", with_provenance="no", debug="no",
                    add_to=None, save_as=None, expiration=None, **args):
        with_meta = with_meta == "yes"
        with_provenance = with_provenance == "yes"
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

            if not ns.owned_by_user(user):
                return f"Permission to create a dataset in the namespace {ds_namespace} denied", 403

            if DBDataset.exists(db, ds_namespace, ds_name):
                return f"Dataset {ds_namespace}:{ds_name} already exists", 409

        if add_to:
            if user is None:
                return 401
            add_namespace, add_name = parse_name(add_to, namespace)
            ns = DBNamespace.get(db, add_namespace)

            if ns is None:
                return f"Namespace {add_namespace} does not exist", 404

            if not DBDataset.exists(db, add_namespace, add_name):
                return f"Dataset {add_namespace}:{add_name} does not exist", 404

            if not ns.owned_by_user(user):
                return f"Permission to add files to dataset in the namespace {add_namespace} denied", 403

        t0 = time.time()
        if not query_text:
            return "[]", "text/json"
            
        query = MQLQuery.parse(query_text)
        query_type = query.Type
        results = query.run(db, filters=self.App.filters(), with_meta=with_meta, with_provenance=with_provenance, default_namespace=namespace or None,
            debug = debug == "yes"
        )

        if not results:
            return "[]", "text/json"

        if query_type == "file":

            def format_file_data(f, with_meta, with_provenance):
                data = { 
                    "name":"%s:%s" % (f.Namespace, f.Name),
                    "fid":f.FID,
                }
                if with_meta:
                    data["metadata"] = f.Metadata or {}
                if with_provenance:
                    data["parents"] = f.Parents
                    data["children"] = f.Children
                
            if save_as:
                results = list(results)
                ds = DBDataset(db, ds_namespace, ds_name)
                ds.save()
                ds.add_files(results)            
            
            if add_to:
                results = list(results)
                ds = DBDataset(db, add_namespace, add_name)
                ds.add_files(results)      
            
            data = (f.to_jsonable(with_metadata=with_meta, with_provenance=with_provenance) for f in results)

        else:
            data = (
                    { 
                        "name":"%s:%s" % (d.Namespace, d.Name),
                        "parent":   None if not d.ParentName else "%s:%s" % (d.ParentNamespace, d.ParentName),
                        "metadata": d.Metadata if with_meta else {}
                    } for d in results 
            )
            
        return self.json_chunks(data), "text/json"
        
    def named_queries(self, request, relpath, namespace=None, **args):
        db = self.App.connect()
        queries = list(DBNamedQuery.list(db, namespace))
        data = ("%s:%s" % (q.Namespace, q.Name) for q in queries)
        return self.json_chunks(data), "text/json"
            
