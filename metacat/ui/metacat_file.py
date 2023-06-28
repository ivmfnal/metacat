import sys, getopt, os, json, pprint, time
from textwrap import dedent
from metacat.webapi import MetaCatClient, MCWebAPIError, MCInvalidMetadataError, MCError
from metacat.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments
from metacat.util import ObjectSpec, undid
from datetime import timezone, datetime

def read_file_list(opts):
    default_namespace = opts.get("-N")
    if "-i" in opts or "--ids" in opts:
        field = "fid"
        source = opts.get("-i") or opts.get("--ids")
    elif "-n" in opts or "--names" in opts:
        field = "did"
        source = opts.get("-n") or opts.get("--names")
    elif "-j" in opts or "--json" in opts:
        field = "dict"
        json_file = opts.get("-j") or opts.get("--json")
        source = json.load(sys.stdin if json_file == "-" else open(json_file, "r"))
    else:
        raise InvalidArguments("File list must be specified either with -n(--names) or -i(--ids)")
        
    if isinstance(source, str):
        if source == "-":
            lst = ({field:x.strip(), "source":x} for x in sys.stdin.readlines())
        elif source.startswith("@"):
            lst = ({field:x.strip(), "source":x} for x in open(source[1:], "r").readlines())
        else:
            lst = ({field:x.strip(), "source":x} for x in source.split(","))
    elif isinstance(source, list):
        lst = source
    else:
        raise InvalidArguments("Unrecognized file list specification")
        
    out = []
    for item in lst:
        spec = ObjectSpec.from_dict(item)

        try:    spec.validate()
        except ValueError:
            InvalidArguments("Invalid file specification:", item.get("source", item))
            
        out.append(spec.as_dict())
    return out
                

class DeclareSampleCommand(CLICommand):
    Usage = """-- print sample input for declare-many command
    """
    
    DeclareSample = json.dumps([
        {        
            "namespace":"test",
            "name":"file1.dat",
            "metadata": {
                "pi": 3.14,
                "version":"1.0",
                "format":"raw",
                "done":True
            },
            "size": 1234,
            "parents":[
                {"fid":"4722545"},
                {"did":"my_scope:file_name.data"}, 
                {"namespace":"my_files", "name":"file_name.data"} 
            ]
        },
        {        
            "namespace":"test",
            "name":"file2.dat",
            "metadata": {
                "e": 2.718,
                "version":"1.0",
                "format":"raw",
                "done":False
            },
            "size": 1734
        },
        {        
            "namespace":"test2",
            "auto_name":"files_$clock.dat",
            "metadata": {
                "e": 2.718,
                "version":"1.0",
                "format":"raw",
                "done":False
            },
            "size": 1734
        }
    ], indent=4, sort_keys=True)
    
    def __call__(self, command, client, opts, args):
        print(self.DeclareSample)
        return

class DeclareSingleCommand(CLICommand):
    
    DeclareSample = dedent("""\
        {        
            "metadata": {
                "pi": 3.14,
                "version":"1.0",
                "format":"raw",
                "done":true
            },
            "size": 1234,
            "parents":[
                {"fid":"4722545"},
                {"did":"my_scope:file_name.data"}, 
                {"namespace":"my_files", "name":"file_name.data"} 
            ],
            "checksums": {
                "adler32": "1234abcd"
            }
        }
    """
    )
    
    Opts = ("N:p:m:c:da:s:P:jvf:", ["namespace=", "parents=", "metadata=", "checksums=", "dry-run", "auto-name", "size=", "json",
                    "file-description", "sample", "verbose"])
    Usage = """[options] [[<file namespace>:]<filename>] [<dataset namespace>:]<dataset name>
    Declare signle file:
        declare [options] [[<file namespace>:]<filename>] [<dataset namespace>:]<dataset name>

            -f|--file-description <JSON file>   - JSON file with description, including file attributes and metadata

            The following options can be used to override the values coming from the file description (-f)
            
            -s|--size <size>                    - file size
            -c|--checksums <type>:<value>[,...] - checksums
            -N|--namespace <default namespace>
            -p|--parents <parent>[,...]         - parents can be specified with their file ids or DIDs.
                                                  if the item contains colon ':', it is interpreted as DID
            -m|--metadata <JSON metadata file>  - if unspecified, file will be declared with empty metadata
            -a|--auto-name [[<namespace>:]<pattern>]   - generate file name automatically

            -d|--dry-run                        - dry run: run all the checks but stop short of actual file declaration
            -j|--json                           - print results as JSON
            -v|--verbose                        - verbose output

            --sample                            - print JSON file description sample
            
        Note that file attributes from command line override those from JSON file description. 
        If explicit file name is specified, auto-name is ignored.
    """

    def __call__(self, command, client, opts, args):
        
        if "--sample" in opts:
            print(self.DeclareSample)
            return

        if len(args) == 1:
            file_spec, dataset_spec = None, args[0]
        elif len(args) == 2:
            file_spec, dataset_spec = args
        else:
            raise InvalidArguments("Invalid number of arguments")

        auto_name = file_namespace = file_name = None
        default_namespace = opts.get("-N") or opts.get("--namespace")
        
        dry_run = "-d" in opts or "--dry-run" in opts
        
        file_description = opts.get("-f") or opts.get("--file-description")
        if file_description is not None:
            file_description = json.load(open(file_description, "r"))
        else:
            file_description = {}
        
        size = int(opts.get("-s", opts.get("--size", 0))) or file_description.get("size", 0)
        if size < 0:
            raise InvalidArguments("File size must be non-negative integer")
        file_description["size"] = size

        if file_spec:
            file_namespace, file_name = undid(file_spec, default_namespace)
            file_description["name"] = file_name

        metadata_file = opts.get("-m") or opts.get("--metadata")
        if metadata_file:
            metadata = json.load(open(metadata_file, "r"))
            if not isinstance(metadata, dict):
                raise InvalidArguments("Metadata must be a dictionary")
            file_description["metadata"] = metadata

        dataset_namespace, dataset_name = undid(dataset_spec, default_namespace)
        if not dataset_namespace:
            raise InvalidArguments("Dataset namespace not specified")

        parent_specs = opts.get("-p") or opts.get("--parents")
        if parent_specs:
            parents = []
            for item in parent_specs.split(","):
                if ':' in item:
                    ns, n = undid(item)
                    parents.append({"namespace": ns, "name": n})
                else:
                    parents.append({"fid": item})
            file_description["parents"] = parents

        checksums = opts.get("-c") or opts.get("--checksums")
        if checksums:
            ckdict = {}
            for item in checksums.split(","):
                name, value = item.split(":", 1)
                ckdict[name] = value
            file_description["checksums"] = ckdict

        auto_name_spec = opts.get("-a") or opts.get("--auto-name")
        if auto_name_spec:
            ns, auto_name = undid(auto_name_spec, default_namespace)
            file_namespace = file_namespace or ns
            file_description.pop("name", None)
            file_description["auto_name"] = auto_name

        if file_namespace:
            file_description["namespace"] = file_namespace
            
        if not file_description.get("namespace"):
            file_description["namespace"] = default_namespace

        if not file_description.get("namespace"):
            raise InvalidArguments("File namespace not specified")

        if "-v" in opts or "--verbose" in opts:
            if dry_run:
                print("--- dry run mode ---")
            print(f"File description to be declared and added to dataset {dataset_namespace}:{dataset_name}")
            print(json.dumps(file_description, indent=4, sort_keys=True))
            
        response = list(client.declare_files(f"{dataset_namespace}:{dataset_name}", 
                [file_description], 
                dry_run = dry_run))[0]

        if "-j" in opts or "--json" in opts:
            print(json.dumps(response, indent=4, sort_keys=True))
        else:
            print(response["fid"], response["namespace"], response["name"])

class DeclareManyCommand(CLICommand):
    
    MinArgs = 2
    Opts = ("N:dj", ["namespace=", "dry-run", "json"])
    Usage = """[options] <file list JSON file> [<dataset namespace>:]<dataset name>
    Declare multiple files:
            -d|--dry-run                        - dry run: run all the checks but stop short of actual file declaration
            -j|--json                           - print results as JSON
            -N|--namespace <default namespace>
    """

    def __call__(self, command, client, opts, args):
        json_file, dataset_spec = args
        default_namespace = opts.get("-N") or opts.get("--namespace")

        files = json.load(open(json_file, "r"))       # parse to validate JSON

        dataset_namespace, dataset_name = undid(dataset_spec, default_namespace)

        if dataset_namespace is None:
            raise InvalidArguments("dataset not specified")
            sys.exit(1)

        try:
            response = client.declare_files(f"{dataset_namespace}:{dataset_name}", files, namespace = default_namespace,
                dry_run = "-d" in opts)
        except MCError as e:
            print(e)
            sys.exit(1)

        if "-j" in opts or "--json" in opts:
            print(json.dumps(response, indent=4, sort_keys=True))



        else:
            for f in response:
                print(f["fid"], f["namespace"], f["name"])


class DatasetsCommand(CLICommand):

    Opts = "jpi:"
    Usage = """[-j|-p] (-i <file id>|<namespace>:<name>)
            -p pretty-print the list of datasets
            -j print the list as JSON
            otherwise print dataset DIDs
    """

    def __call__(self, command, client, opts, args):
        did = fid = None
    
        if args:
            did = args[0]
        else:
            if "-i" not in opts:
                raise InvalidArguments("File specification error")
            fid = opts["-i"]
        try:
            data = client.get_file(did=did, fid=fid, with_provenance=False, with_metadata=False, with_datasets=True)
        except MCError as e:
            print(e)
            sys.exit(1)

        if data is None:
            print("file not found", file=sys.stderr)
            sys.exit(1)
        datasets = sorted(data.get("datasets", []), key = lambda ds: (ds["namespace"], ds["name"]))
        if "-j" in opts:
            print(json.dumps(datasets, indent=4, sort_keys=True))
        elif "-p" in opts:
            pprint.pprint(datasets)
        else:
            for item in datasets:
                print(item["namespace"] + ":" + item["name"])
                
class FileIDCommand(CLICommand):

    MinArgs = 1
    Usage = """<namespace>:<name>|<namespace> <name>  - print file id
    """

    def __call__(self, command, client, opts, args):
        did = namespace = name = None
        if len(args) == 1:
            did = args[0]
            if ':' not in did:
                raise InvalidArguments("Invalid DID: " + did)
        elif len(args) == 2:
            namespace, name = args
        else:
            raise InvalidArguments("Too many arguments")
        
        try:
            data = client.get_file(did=did, namespace=namespace, name=name, with_provenance=False, with_metadata=False,
                               with_datasets=False)
        except MCError as e:
            print(e)
            sys.exit(1)
        if data is None:
            print("File not found", file=sys.stderr)
            sys.exit(1)

        print(data["fid"])

class RetireCommand(CLICommand):

    MinArgs = 1
    Opts = "u unretire"
    Usage = """[-u|--unretire] (<namespace>:<name>|<namespace> <name>)  - retire/unretire file
        -u - unretire the file
    """

    def __call__(self, command, client, opts, args):
        did = namespace = name = None
        if len(args) == 1:
            did = args[0]
            if ':' not in did:
                raise InvalidArguments("Invalid DID: " + did)
        elif len(args) == 2:
            namespace, name = args
        else:
            raise InvalidArguments("Too many arguments")

        try:
            data = client.retire_file(did=did, namespace=namespace, name=name, retire="-u" not in opts)
        except MCError as e:
            print(e)
            sys.exit(1)
        if data is None:
            print("File not found", file=sys.stderr)
            sys.exit(1)

class NameCommand(CLICommand):

    MinArgs = 1
    Opts = "jd json did"
    Usage = """[options] <file id>  - print namespace, name
        -j|--json                   - as JSON {"namespace":..., "name":...}
        -d|--did                    - as DID (namespace:name)
    """

    
    def __call__(self, command, client, opts, args):
        fid = args[0]
    
        try:
            data = client.get_file(fid=fid, with_provenance=False, with_metadata=False, with_datasets=False)
        except MCError as e:
            print(e)
            sys.exit(1)

        if data is None:
            print("File not found", file=sys.stderr)
            sys.exit(1)

        namespace, name = data["namespace"], data["name"]
        if "-j" in opts or "--json" in opts:
            print(f'{{ "namespace": "{namespace}", "name": "{name}" }}')
        elif "-d" in opts or "--did" in opts:
            print(f'{namespace}:{name}')
        else:
            print("Namespace: ", namespace)
            print("Name:      ", name)
        

class ShowCommand(CLICommand):

    Opts = ("mdjpli:", ["json","metadata","pretty","lineage","provenance","id="])
    Usage = """[options] (-i|--id <file id>|<namespace>:<name>)
            -m|--metadata             - include file metadata
            -d|--datasets             - include datasets the file is in

            -j|--json                 - as JSON
            -p|--pretty               - pretty-print information
            
            -l|--provenance           - include provenance information
    """

    
    def __call__(self, command, client, opts, args):
        if not args and "-i" not in opts and "--id" not in opts:
            raise InvalidArguments("Either <namespace:name> or -i|--id <file id> must be specified")

        as_json = "--json" in opts or "-j" in opts
        pretty = "-p" in opts or "--pretty" in opts

        include_provenance = "-l" in opts or "--provenance" in opts
        include_meta = "--metadata" in opts or "-m" in opts
        include_datasets = "--datasets" in opts or "-d" in opts

        did = fid = None
    
        if args:
            did = args[0]
        else:
            fid = opts.get("-i") or opts.get("--id")
            
        if not did and not fid:
            raise InvalidArguments("Eirher DID or file id must be specified")

        try:
            data = client.get_file(did=did, fid=fid, 
                        with_provenance=include_provenance, with_metadata=include_meta,
                        with_datasets=include_datasets)
        except MCError as e:
            print(e)
            sys.exit(1)

        if data is None:
            print("file not found", file=sys.stderr)
            sys.exit(1)
        
        if include_provenance:
            parents = data.get("parents", [])
            if parents:
                parents = client.get_files(parents)
                parents = data["parents"] = [{k:f[k] for k in ("fid", "namespace", "name")} for f in parents]
        
            children = data.get("children", [])
            if children:
                children = client.get_files(children)
                children = data["children"] = [{k:f[k] for k in ("fid", "namespace", "name")} for f in children]

        if as_json:
            print(json.dumps(data, indent=4, sort_keys=True))
        elif pretty:
            pprint.pprint(data)
        else:
            for k, v in sorted(data.items()):
                if k == "checksums":
                    print("checksums:")
                    for typ, cksum in sorted(v.items()):
                        print("    %-10s: %s" % (typ, cksum))
                elif k == "created_timestamp":
                    t = datetime.fromtimestamp(v, timezone.utc)
                    print("%-20s:\t%s" % (k, t))
                elif k not in ("metadata", "parents", "children", "datasets"):
                    print("%-20s:\t%s" % (k, v))

            if "metadata" in data:
                print("metadata:")
                for name, value in sorted(data["metadata"].items()):
                    print("    %-20s: %s" % (name, value))
            if include_provenance:
                if parents:
                    print("parents:")
                    for f in parents:
                        print("   %(namespace)s:%(name)s (%(fid)s)" % f)
                if children:
                    print("children:")
                    for f in children:
                        print("   %(namespace)s:%(name)s (%(fid)s)" % f)
            if "datasets" in data:
                print("datasets:")
                for item in sorted(data["datasets"], key=lambda ds: (ds["namespace"], ds["name"])):
                    print("    %(namespace)s:%(name)s" % item)
                    
class UpdateMetaCommand(CLICommand):
    
    Opts = ("i:n:N:rs", ["namespace=", "names=", "ids=", "sample", "replace","sample"])
    Usage = """[options] (@<JSON file with metadata>|'<JSON expression>')

            -r|--replace          - replace metadata, otherwise update

            list files by DIDs or namespace/names
            -N|--namespace <default namespace>           - default namespace for files
            -n|--names <file namespace>:<file name>[,...]
            -n|--names -          - read the list from stdin
            -n|--names @<file>    - read the list from file

            list files by file id
            -i|--ids <file id>[,...] 
            -i|--ids -            - read the list from stdin
            -i|--ids @<file>      - read the list from file

            read file list from JSON file
            -j|--json <json file> - from a file
            -j|--json -           - from stdin 
            -s|--sample           - print JOSN file list sample
    """

    UpdateSample = json.dumps(
        [
            {        
                "did":"test:file1.dat"
            },
            {        
                "namespace":"test",
                "name":"file2.dat"
            },
            {        
                "fid":"54634"
            }
        ],
        indent=4, sort_keys=True)
        
    
    def __call__(self, command, client, opts, args):
        
        if "--sample" in opts or "-s" in opts:
            print(self.UpdateSample)
            sys.exit(0)
        
        if len(args) != 1:
            raise InvalidArguments("Invalid arguments")

        mode = "replace" if ("-r" in opts or "--replace" in opts) else "update"
        namespace = opts.get("-N") or opts.get("--namespace")
    
        file_list = read_file_list(opts)

        try:
            response = client.update_file_meta(meta, files=file_list, mode=mode, namespace=namespace)
        except MCError as e:
            print(e)
            sys.exit(1)

class UpdateCommand(CLICommand):
    
    MinArgs = 1
    Opts = "jvdrs:f:k:p:c:m: replace sample verbose json dry-run file-description= size= checksums= parents= children= metadata="
    Usage = """[options] (<file namespace>:<file name>|<file id>)

            -d|--dry-run
            -v|--verbose
            -r|--replace                        - Replace metadata, checksums, parents and children
                                                  otherwise update metadata, checksums, add parents and children.
                                                  Applies to -k, -p, -c, -m, -f options
            -j|--json                           - print updated file attributes as JSON. Otherwise - as Python pprint

            -u|--updates <JSON file>            - JSON file with file attributes to be updated as a dictionary.
                                                  The following keys are accepted: 
                                                      size: int, 
                                                      checksums: dict, 
                                                      metadata: dict,
                                                      parents: list of strings,
                                                      children: list of strings

            -s|--size <size>                    - file size
            -k|--checksums <type>:<value>[,...] - checksums
            -m|--metadata <JSON metadata file>  - metadata
            -m|--metadata '<JSON dictionary>'   - inline metadata
            -m|--metadata -                     - read metadata dictionary as JSON from stdin
            -p|--parents <parent>[,...]         - parents can be specified with their file ids or DIDs.
                                                  if the item contains colon ':', it is interpreted as DID
            -p|--parents -                      - use '-' with -r to remove all parents
            -c|--children <child>[,...]         - children can be specified with their file ids or DIDs.
                                                  if the item contains colon ':', it is interpreted as DID
            -c|--children -                     - use '-' with -r to remove all choldren
            
            If -u is used together with some individual attributes options, the attributes from the -u file will
            be updated with those coming from the individual attribute options first.
    """

    def __call__(self, command, client, opts, args):
        
        if "--sample" in opts:
            print(self.SampleFile)
            return

        dry_run = "-d" in opts or "--dry-run" in opts
        replce = "-r" in opts or "--replace" in opts
        verbose = "-v" in opts or "--verbose" in opts

        update_args = {}
        updates_file = opts.get("-u", opts.get("--updates"))
        if updates_file:
            update_args = json.load(open(updates_file, "r"))

        update_args["replace"] = "-r" in opts or "--replace" in opts

        if ':' in args[0]:
            update_args["did"] = args[0]
        else:
            update_args["fid"] = args[0]

        size = opts.get("-s", opts.get("--size"))
        if size is not None:
            size = int(size)
            if size < 0:
                raise InvalidArguments("File size must be non-negative integer")
            update_args["size"] = size

        metadata_file = opts.get("-m") or opts.get("--metadata")
        if metadata_file:
            metadata_update = None
            try:
                metadata_update = json.loads(metadata_file)
            except:
                metadata_update = json.load(open(metadata_file, "r"))
            if not isinstance(metadata_update, dict):
                raise InvalidArguments("Metadata file not found or metadata is not a dictionary")
            invalid_names = [k for k in metadata_update if '.' not in k]
            if invalid_names:
                print("Invalid metadata key(s):", ", ".join(invalid_names), file=sys.stderr)
                sys.exit(1)
            update_args["metadata"] = metadata_update

        parents_specs = opts.get("-p") or opts.get("--parents")
        if parents_specs is not None:
            parents = []
            if parents_specs != "-":
                for p in parents_specs.split(","):
                    if ':' in p:
                        parents.append({"did":p})
                    else:
                        parents.append({"fid":p})
            update_args["parents"] = parents

        children_specs = opts.get("-c") or opts.get("--children")
        if children_specs is not None:
            children = []
            if children_specs != "-":
                for c in children_specs.split(","):
                    if ':' in c:
                        children.append({"did":c})
                    else:
                        children.append({"fid":c})
            update_args["children"] = children

        checksums = opts.get("-k") or opts.get("--checksums")
        if checksums:
            ckdict = {}
            for item in checksums.split(","):
                name, value = item.split(":", 1)
                ckdict[name] = value
            update_args["checksums"] = ckdict

        if verbose or dry_run:
            if dry_run:
                print("-------- dry run mode --------")
            for k, v in sorted(update_args.items()):
                print("%-20s: %s" % (k, str(v)))
        
        if not dry_run:
            results = client.update_file(**update_args)
            if verbose:
                print()
                if "--json" in opts or "-j" in opts:
                    print(json.dumps(results, sort_keys=True, indent=4))
                else:
                    pprint.pprint(results)
            

class AddCommand(CLICommand):
    
    Opts = ("i:j:n:N:s", ["namespace=", "json=", "names=", "ids=", "sample"])
    Usage = """[options] <dataset namespace>:<dataset name>

            list files by DIDs or namespace/names
            -N|--namespace <default namespace>           - default namespace for files
            -n|--names <file namespace>:<file name>[,...]
            -n|--names -          - read the list from stdin
            -n|--names @<file>    - read the list from file
    
            list files by file id
            -i|--ids <file id>[,...] 
            -i|--ids -            - read the list from stdin
            -i|--ids @<file>      - read the list from file

            read file list from JSON file
            -j|--json <json file>
            -j|--json -           - read JSON file list from stdin 
            -s|--sample           - print JOSN file list sample
    """
    
    Usage = 'Use "metacat dataset add..." instead'
    
    AddSample = json.dumps(
        [
            {        
                "did":"test:file1.dat"
            },
            {        
                "namespace":"test",
                "name":"file2.dat"
            },
            {        
                "fid":"54634"
            }
        ],
        indent=4, sort_keys=True
    )

    
    def __call__(self, command, client, opts, args):
        print('Use "metacat dataset add..." instead')
        sys.exit(1)
        
        if "--sample" in opts or "-s" in opts:
            print(self.AddSample)
            sys.exit(0)

        if len(args) != 1:
            raise InvalidArguments("Invalid arguments")

        file_list = read_file_list(opts)
        dataset = args[-1]
        out = client.add_files(dataset, file_list)

FileCLI = CLI(
    "declare",  DeclareSingleCommand(),
    "declare-many",  DeclareManyCommand(),
    "declare-sample",  DeclareSampleCommand(),
    "add",      AddCommand(),
    "datasets", DatasetsCommand(),
    "update",   UpdateCommand(),
    "update-meta",   UpdateMetaCommand(),
    "retire",   RetireCommand(),
    "name",     NameCommand(),
    "fid",      FileIDCommand(),
    "show",     ShowCommand()
)