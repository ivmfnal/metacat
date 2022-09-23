import sys, getopt, os, json, pprint, time
from metacat.webapi import MetaCatClient, MCWebAPIError, MCInvalidMetadataError
from metacat.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments

def read_file_list(opts):
    if "-i" in opts or "--ids" in opts:
        field = "fid"
        source = opts.get("-i") or opts.get("--ids")
    elif "-n" in opts or "--names" in opts:
        field = "name"
        source = opts.get("-n") or opts.get("--names")
    else:
        raise InvalidArguments("File list must be specified either with --names or --ids")
        
    if source == "-":
        lst = (x.strip() for x in sys.stdin.readlines())
    elif source.startswith("@"):
        lst = (x.strip() for x in open(source[1:], "r").readlines())
    else:
        lst = source.split(",")

    return [{field:x} for x in lst if x]

def parse_namespace_name(spec, default_namespace=None):
    if ":" in spec:
        return tuple(spec.split(":", 1))
    else:
        return default_namespace, spec
        
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
    
    MinArgs = 3
    Opts = ("N:p:m:c:", ["namespace=", "parents=", "metadata=", "checksums="])
    Usage = """[options] [[<file namespace>:]<filename>] [<dataset namespace>:]<dataset name>
    Declare signle file:
        declare [options] <size> [<file namespace>:]<filename> [<dataset namespace>:]<dataset>
            -c|--checksums <type>:<value>[,...] - checksums
            -N|--namespace <default namespace>
            -p|--parents <parent_id>,... 
            -m|--metadata <JSON metadata file>  - if unspecified, file will be declared with empty metadata
    """

    def __call__(self, command, client, opts, args):
        size, file_spec, dataset_spec = args
        default_namespace = opts.get("-N") or opts.get("--namespace")
        file_namespace, file_name = parse_namespace_name(file_spec, default_namespace)
        file_name = file_name or None           # for auto-generation
        if not file_namespace:
            raise InvalidArguments("File namespace not specified")
            sys.exit(1)
        dataset_namespace, dataset_name = parse_namespace_name(dataset_spec, default_namespace)
        if not dataset_namespace:
            raise InvalidArguments("Dataset namespace not specified")
            sys.exit(1)

        try:    size = int(size)
        except: size = -1
        if size < 0:
            raise InvalidArguments("File size must be zero or positive integer")

        parents = opts.get("-p") or opts.get("--parents")
        if parents:
            parents = parents.split(",")

        metadata_file = opts.get("-m") or opts.get("--metadata")
        if metadata_file:
            metadata = json.load(open(metadata_file, "r"))
        else:
            metadata = {}
        assert isinstance(metadata, dict)
        file_data = {
                "namespace":    file_namespace,
                "name":         file_name,
                "metadata":     metadata,
                "size":         size
            }
        if parents:
            file_data["parents"] = parents

        checksums = opts.get("-c") or opts.get("--checksums")
        if checksums:
            ckdict = {}
            for item in checksums.split(","):
                name, value = item.split(":", 1)
                ckdict[name] = value
            file_data["checksums"] = ckdict

        files = [file_data]
    
        try:
            response = client.declare_files(f"{dataset_namespace}:{dataset_name}", files, namespace = default_namespace)    
            print(response)
        except MCInvalidMetadataError as e:
            print(e)
            sys.exit(1)


class DeclareManyCommand(CLICommand):
    
    MinArgs = 2
    Opts = ("N:", ["namespace="])
    Usage = """[options] <file list JSON file> [<dataset namespace>:]<dataset name>
    Declare multiple files:
        declare [-N|--namespace <default namespace>] <json file> [<dataset namespace>:]<dataset>
    """

    def __call__(self, command, client, opts, args):
        json_file, dataset_spec = args
        default_namespace = opts.get("-N") or opts.get("--namespace")

        files = json.load(open(json_file, "r"))       # parse to validate JSON

        dataset_namespace, dataset_name = parse_namespace_name(dataset_spec, default_namespace)

        if dataset_namespace is None:
            raise InvalidArguments("dataset not specified")
            sys.exit(1)

        try:
            response = client.declare_files(f"{dataset_namespace}:{dataset_name}", files, namespace = default_namespace)    
            print(response)
        except MCInvalidMetadataError as e:
            print(e)
            sys.exit(1)


class ShowCommand(CLICommand):

    Opts = ("jmpi:l:Ind", ["json","meta-only","pretty","name-only","lineage","provenance","ids", "id-only"])
    Usage = """[options] (-i <file id>|<namespace>:<name>)
            -m|--meta-only            - print file metadata only
            -n|--name-only            - print file namespace, name only
            -d|--id-only              - print file id only
            
            -j|--json                 - as JSON
            -p|--pretty               - pretty-print information
            
            -l|--lineage|--provenance (p|c)        - parents or children instead of the file itself
            -I|--ids                               - for parents and children, print file ids instead of namespace/names
    """

    def __call__(self, command, client, opts, args):
        if not args and "-i" not in opts:
            raise InvalidArguments("Either -i <file id> or <namespace:name> must be specified")
            

        #print("opts:", opts,"    args:", args)
    
        as_json = "--json" in opts or "-j" in opts
        pretty = "-p" in opts or "--pretty" in opts
        provenance = opts.get("-l") or opts.get("--lineage") or opts.get("--provenance")
        id_only = "--id-only" in opts or "-d" in opts
        provenance_ids = "--ids" in opts or "-I" in opts
        meta_only = "--meta-only" in opts or "-m" in opts
        name_only = "--name-only" in opts or "-n" in opts

        did = fid = None
    
        if args:
            did = args[0]
        else:
            fid = opts["-i"]

        data = client.get_file(did=did, fid=fid, with_provenance=provenance, with_metadata=not (name_only or id_only))
        if id_only:
            print(data["fid"])
        elif name_only:
            print("%(namespace)s:%(name)s" % data)
        elif provenance:
            ids = data["parents"] if provenance == "p" else data["children"]
            if ids:
                lst = [dict(fid=fid) for fid in ids]
                related = client.get_files(lst)
                if as_json:
                    print(json.dumps(related))
                elif pretty:
                    pprint.pprint(related)
                else:
                    for f in related:
                        if provenance_ids:
                            print(f["fid"])
                        else:
                            print("%(namespace)s:%(name)s" % f)
        else:
            if meta_only:
                data = data.get("metadata", {})
            if pretty:
                pprint.pprint(data)
            elif as_json:
                print(json.dumps(data, indent=4, sort_keys=True))
            else:
                for k, v in sorted(data.items()):
                    if k != "metadata":
                        print("%-15s:\t%s" % (k, v))
                if "metadata" in data:
                    print("%-15s:\t" % ("metadata",), end="")
                    pprint.pprint(data["metadata"])
                    
class UpdateCommand(CLICommand):
    
    Opts = ("i:n:N:r", ["namespace=", "names=", "ids=", "sample", "replace"])
    Usage = """[options] (@<JSON file with metadata>|'<JSON expression>')

            -r|--replace          - replace metadata, otherwise update

            list files by name
            -N|--namespace <default namespace>           - default namespace for files
            -n|--names <file namespace>:<file name>[,...]
            -n|--names -          - read the list from stdin
            -n|--names @<file>    - read the list from file

            list files by file id
            -i|--ids <file id>[,...] 
            -i|--ids -            - read the list from stdin
            -i|--ids @<file>      - read the list from file
    """

    UpdateSample = json.dumps([
            {        
                "name":"test:file1.dat",
                "metadata": {
                    "pi": 3.14,
                    "version":"1.0",
                    "format":"raw",
                    "done":True
                },
                "parents":[ "4722545", "43954" ]
            },
            {        
                "name":"test:file1.dat",
                "metadata": {
                    "pi": 3.14,
                    "version":"1.0",
                    "format":"raw",
                    "done":True
                },
                "parents":[ "4722545", "43954" ]
            },
            {        
                "fid":"54634",
                "metadata": {
                    "q": 2.8718,
                    "version":"1.1",
                    "format":"processed",
                    "params": [1,2],
                    "done":False
                }
            }
        ],
        indent=4, sort_keys=True)
        
    def __call__(self, command, client, opts, args):
        
        if "--sample" in opts:
            print(self.UpdateSample)
            sys.exit(0)
        
        mode = "replace" if ("-r" in opts or "--replace" in opts) else "update"
        namespace = opts.get("-N") or opts.get("--namespace")
    
        file_list = read_file_list(opts)
        names = fids = None
        if "-i" in opts or "--ids" in opts:
            fids = [f["fid"] for f in file_list]
        else:
            names = [f["name"] for f in file_list]
        
        meta = args[0]
        if meta.startswith('@'):
            meta = json.load(open(meta[1:], "r"))
        elif meta == "-":
            meta = json.load(sys.stdin)
        else:
            meta = json.loads(meta)

        try:    response = client.update_file_meta(meta, names=names, fids=fids, mode=mode, namespace=namespace)
        except MCInvalidMetadataError as e:
            data = e.json()
            print(data["message"], file=sys.stderr)
            for error in data.get("metadata_errors", []):
                print("  {name} = {value}: {reason}".format(**error), file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(e)
            sys.exit(1)
        print(response)

class AddCommand(CLICommand):
    
    Opts = ("i:j:n:N:", ["namespace=", "json=", "names=", "ids=", "sample"])
    Usage = """[options] <dataset namespace>:<dataset name>

            list files by name
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
    """
    MinArgs = 1
    
    AddSample = json.dumps(
        [
            {        
                "name":"test:file1.dat"
            },
            {        
                "name":"test:file1.dat"
            },
            {        
                "fid":"54634"
            }
        ],
        indent=4, sort_keys=True
    )

    def __call__(self, command, context, opts, args):
        if "--sample" in opts:
            print(json.dumps(_add_smaple, sort_keys=True, indent=4, separators=(',', ': ')))
            sys.exit(0)

        file_list = []

        if "-j" in opts or "--json" in opts:
            file_list = json.load(open(opts.get("-f") or opts.get("--files"), "r"))
        else:
            file_list = read_file_list(opts)

        dataset = args[-1]
        namespace = opts.get("-N") or opts.get("--namespace")
        out = client.add_files(dataset, file_list, namespace=namespace)
        print(out)

FileCLI = CLI(
    "declare",  DeclareSingleCommand(),
    "declare-many",  DeclareManyCommand(),
    "declare-sample",  DeclareSampleCommand(),
    "add",      AddCommand(),
    "update",   UpdateCommand(),
    "show",     ShowCommand()
)