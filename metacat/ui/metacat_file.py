import sys, getopt, os, json, pprint, time
from metacat.webapi import MetaCatClient, MCWebAPIError, MCInvalidMetadataError
from metacat.ui.cli import CLI, CLICommand, InvalidOptions, InvalidArguments
from metacat.util import ObjectSpec
from datetime import timezone, datetime

def undid(did, default_namespace=None):
    if ":" in spec:
        return tuple(spec.split(":", 1))
    else:
        return default_namespace, spec
        
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
    
    MinArgs = 2
    Opts = ("N:p:m:c:das:P:j", ["namespace=", "parents=", "metadata=", "checksums=", "dry-run", "auto-name", "size=", "json"])
    Usage = """[options] [[<file namespace>:]<filename>] [<dataset namespace>:]<dataset name>
    Declare signle file:
        declare [options]    [<file namespace>:]<filename>          [<dataset namespace>:]<dataset>
        declare [options] -a [<file namespace>:]<auto-name pattern> [<dataset namespace>:]<dataset>
            -d|--dry-run                        - dry run: run all the checks but stop short of actual file declaration
            -j|--json                           - print results as JSON
            -s|--size <size>                    - file size
            -c|--checksums <type>:<value>[,...] - checksums
            -N|--namespace <default namespace>
            -p|--parents <parent>[,...]         - parents can be specified with their file ids or DIDs.
                                                  if the item contains colon ':', it is interpreted as DID
            -m|--metadata <JSON metadata file>  - if unspecified, file will be declared with empty metadata
            -a|--auto-name                      - generate file name automatically
    """

    def __call__(self, command, client, opts, args):

        if len(args) != 2:
            raise InvalidArguments("Invalid number of arguments")
        file_spec, dataset_spec = args
        file_namespace = file_name = auto_name_pattern = None
        auto_name = "-a" in opts
        default_namespace = opts.get("-N") or opts.get("--namespace")
        size = int(opts.get("-s", opts.get("--size", 0)))
        if size < 0:
            raise InvalidArguments("File size must be non-negative integer")

        file_namespace, file_name = parse_namespace_name(file_spec, default_namespace)
        if not file_namespace:
            raise InvalidArguments("Namespace not specified")

        if auto_name:
            auto_name_pattern = file_name
            file_name = None

        metadata_file = opts.get("-m") or opts.get("--metadata")
        if metadata_file:
            metadata = json.load(open(metadata_file, "r"))
        else:
            metadata = {}
        if not isinstance(metadata, dict):
            raise InvalidArguments("Metadata must be a dictionary")

        dataset_namespace, dataset_name = parse_namespace_name(dataset_spec, default_namespace)
        if not dataset_namespace:
            raise InvalidArguments("Dataset namespace not specified")
            sys.exit(1)

        try:    size = int(size)
        except: size = -1

        parents = []
        parent_specs = opts.get("-p") or opts.get("--parents")
        if parent_specs:
            for item in parent_specs.split(","):
                if ':' in item:
                    ns, n = parse_namespace_name(item)
                    parents.append({"namespace": ns, "name": n})
                else:
                    parents.append({"fid": item})

        file_data = {
                "namespace":    file_namespace,
                "metadata":     metadata,
                "size":         size
            }

        if file_name:
            file_data["name"] = file_name
        else:
            file_data["auto_name"] = auto_name_pattern

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
            response = client.declare_files(f"{dataset_namespace}:{dataset_name}", files, namespace = default_namespace,
                    dry_run = "-d" in opts)[0]
            if "-j" in opts or "--json" in opts:
                print(json.dumps(response, indent=4, sort_keys=True))
            else:
                print(response["fid"], response["namespace"], response["name"])
        except MCInvalidMetadataError as e:
            print(e)
            sys.exit(1)


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

        dataset_namespace, dataset_name = parse_namespace_name(dataset_spec, default_namespace)

        if dataset_namespace is None:
            raise InvalidArguments("dataset not specified")
            sys.exit(1)

        try:
            response = client.declare_files(f"{dataset_namespace}:{dataset_name}", files, namespace = default_namespace,
                    dry_run = "-d" in opts)
            if "-j" in opts or "--json" in opts:
                print(json.dumps(response, indent=4, sort_keys=True))
            else:
                for f in response:
                    print(f["fid"], f["namespace"], f["name"])
        except MCInvalidMetadataError as e:
            print(e)
            sys.exit(1)


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
        data = client.get_file(did=did, fid=fid, with_provenance=False, with_metadata=False, with_datasets=True)
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
        
        data = client.get_file(did=did, namespace=namespace, name=name, with_provenance=False, with_metadata=False,
                               with_datasets=False)
        if data is None:
            print("File not found", file=sys.stderr)
            sys.exit(1)

        print(data["fid"])

class NameCommand(CLICommand):

    MinArgs = 1
    Opts = "jd json did"
    Usage = """[options] <file id>  - print namespace, name
        -j|--json                   - as JSON {"namespace":..., "name":...}
        -d|--did                    - as DID (namespace:name)
    """

    def __call__(self, command, client, opts, args):
        fid = args[0]
    
        data = client.get_file(fid=fid, with_provenance=False, with_metadata=False, with_datasets=False)
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

        data = client.get_file(did=did, fid=fid, 
                        with_provenance=include_provenance, with_metadata=include_meta,
                        with_datasets=include_datasets)
        if data is None:
            print("file not found", file=sys.stderr)
            sys.exit(1)
            
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
            if "parents" in data:
                print("parents:")
                for fid in data["parents"]:
                    print("   ", fid)
            if "children" in data:
                print("children:")
                for fid in data["children"]:
                    print("   ", fid)
            if "datasets" in data:
                print("datasets:")
                for item in sorted(data["datasets"], key=lambda ds: (ds["namespace"], ds["name"])):
                    print("    %(namespace)s:%(name)s" % item)
                    
class UpdateCommand(CLICommand):
    
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
            -j|--json <json file>
            -j|--json -           - read JSON file list from stdin 
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

        try:    response = client.update_file_meta(meta, files=file_list, mode=mode, namespace=namespace)
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
    "name",     NameCommand(),
    "fid",      FileIDCommand(),
    "show",     ShowCommand()
)