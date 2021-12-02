import sys, getopt, os, json, pprint, time
from metacat.util import to_bytes, to_str, TokenLib
from metacat.webapi import MetaCatClient, MCWebAPIError, MCInvalidMetadataError

Usage = """
Show file info:

    show <options> <namespace>:<name>
    show <options> -i <file id>
        -m|--meta-only            - metadata only
        -j|--json                 - as JSON
        -p|--pretty               - pretty-print information
        -l|--lineage|--provenance (p|c)        - parents or children instead of the file itself
        -n|--name-only            - print namespace:name only
        -I|--ids-only             - for parents and children, print file IDs only
        
    
Declare single file:

    declare [options] [<file namespace>:]<filename> [<dataset namespace>:]<dataset>
        -N|--namespace <default namespace>
        -p|--parents <parent_id>,... 
        -m|--metadata <JSON metadata file>  - if unspecified, file will be declared with empty metadata

Declare multiple files:
    declare [-N|--namespace <default namespace>] -j|--json <json file> [<dataset namespace>:]<dataset>
    declare --sample                                   - print JSON file sample
    
Update file metadata:

    update <options> @<JSON file with metadata>
    update <options> '<JSON expression>'

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

        
Add file(s) to dataset:

    add <options> <dataset namespace>:<dataset name>

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

def read_file_list(opts):
    if "-i" in opts or "--ids" in opts:
        field = "fid"
        source = opts.get("-i") or opts.get("--ids")
    elif "-n" in opts or "--names" in opts:
        field = "name"
        source = opts.get("-n") or opts.get("--names")
    else:
        print("File list must be specified either with --names or --ids")
        print()
        print(Usage)
        sys.exit(2)
        
    if source == "-":
        lst = (x.strip() for x in sys.stdin.readlines())
    elif source.startswith("@"):
        lst = (x.strip() for x in open(source[1:], "r").readlines())
    else:
        lst = source.split(",")

    return [{field:x} for x in lst if x]

_declare_smaple = [
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
        "parents":[ "4722545", "43954" ]
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
        "size": 1734,
        "parents":[ "4723345", "4322954" ]
    }
]

def parse_namespace_name(spec, default_namespace=None):
    if ":" in spec:
        return tuple(spec.split(":", 1))
    else:
        return default_namespace, spec

def do_declare(client, args):
    opts, args = getopt.getopt(args, "N:j:p:m:", ["json=", "namespace=", "sample", "parents=", "metadata="])
    opts = dict(opts)
    default_namespace = opts.get("-N") or opts.get("--namespace")

    if "--sample" in opts:
        print(json.dumps(_declare_smaple, sort_keys=True, indent=4, separators=(',', ': ')))
        sys.exit(0)
    
    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)
        
    if "-j" in opts or "--json" in opts:
        json_file = opts.get("-j") or opts.get("--json")
        files = json.load(open(json_file, "r"))       # parse to validate JSON
        dataset_namespace, dataset_name = parse_namespace_name(args[0], default_namespace)
        if dataset_namespace is None:
            print("Dataset name not specified")
            sys.exit(1)
    else:
        parents = opts.get("-p") or opts.get("--parents")
        if parents:
            parents = parents.split(",")
        file_spec, dataset_spec = args
        file_namespace, file_name = parse_namespace_name(file_spec, default_namespace)
        if not file_namespace:
            print("File namespace not specified")
            sys.exit(1)
        dataset_namespace, dataset_name = parse_namespace_name(dataset_spec, default_namespace)
        if not dataset_namespace:
            print("Dataset namespace not specified")
            sys.exit(1)

        metadata_file = opts.get("-m") or opts.get("--metadata")
        if metadata_file:
            metadata = json.load(open(metadata_file, "r"))
        else:
            metadata = {}
        assert isinstance(metadata, dict)
        file_data = {
                "namespace":    file_namespace,
                "name":         file_name,
                "metadata":     metadata
            }
        if parents:
            file_data["parents"] = parents
        files = [file_data]
    
    try:
        response = client.declare_files(f"{dataset_namespace}:{dataset_name}", files, namespace = default_namespace)    
        print(response)
    except MCInvalidMetadataError as e:
        print(e)
        sys.exit(1)
                


def do_show(client, args):
    opts, args = getopt.getopt(args, "jmpi:l:In", ["json","meta-only","pretty","names-only","lineage","provenance","ids"])
    opts = dict(opts)
    
    if not args:
        if not "-i" in opts:
            print(Usage)
            sys.exit(2)
    else:
        if "-i" in opts:
            print(Usage)
            sys.exit(2)
            

    #print("opts:", opts,"    args:", args)
    
    as_json = "--json" in opts or "-j" in opts
    pretty = "-p" in opts or "--pretty" in opts
    provenance = opts.get("-l") or opts.get("--lineage") or opts.get("--provenance")
    names_only = "--names-only" in opts or "-n" in opts
    ids_only = "--ids-only" in opts or "-I" in opts
    meta_only = "--meta-only" in opts or "-m" in opts
    
    name = fid = None
    
    if args:
        name = args[0]
    else:
        fid = opts["-i"]

    data = client.get_file(name=name, fid=fid, with_provenance=True)
    if provenance:
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
                    if ids_only:
                        print(f["fid"])
                    else:
                        print("%(namespace)s:%(name)s" % f)
    else:
        if meta_only:
            data = data.get("metadata", {})
        if pretty:
            pprint.pprint(data)
        elif as_json:
            print(json.dumps(data))
        else:
            for k, v in sorted(data.items()):
                if k != "metadata":
                    print("%-15s:\t%s" % (k, v))
            if "metadata" in data:
                print("%-15s:\t" % ("metadata",), end="")
                pprint.pprint(data["metadata"])

_update_smaple = [
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
]

def do_update(client, args):
    opts, args = getopt.getopt(args, "i:n:N:r", ["namespace=", "names=", "ids=", "sample", "replace"])
    opts = dict(opts)

    if "--sample" in opts:
        print(json.dumps(_update_smaple, sort_keys=True, indent=4, separators=(',', ': ')))
        sys.exit(0)
        
    mode = "replace" if ("-r" in opts or "--replace" in opts) else "update"
    namespace = opts.get("-N") or opts.get("--namespace")
    
    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)
    
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

_add_smaple = [
    {        
        "name":"test:file1.dat"
    },
    {        
        "name":"test:file1.dat"
    },
    {        
        "fid":"54634"
    }
]

def do_add(client, args):
    opts, args = getopt.getopt(args, "i:j:n:N:", ["namespace=", "json=", "names=", "ids=", "sample"])
    opts = dict(opts)

    if "--sample" in opts:
        print(json.dumps(_add_smaple, sort_keys=True, indent=4, separators=(',', ': ')))
        sys.exit(0)

    if not args or args[0] == "help":
        print(Usage)
        sys.exit(2)

    file_list = []

    if "-j" in opts or "--json" in opts:
        file_list = json.load(open(opts.get("-f") or opts.get("--files"), "r"))
    else:
        file_list = read_file_list(opts)

    dataset = args[-1]
    namespace = opts.get("-N") or opts.get("--namespace")
    out = client.add_files(dataset, file_list, namespace=namespace)
    print(out)

def do_file(server_url, args):
    if not args:
        print(Usage)
        sys.exit(2)
        
    command = args[0]
    client = MetaCatClient(server_url)
    try:
        method = {
            "declare":      do_declare,
            "add":          do_add,
            "update":       do_update,
            "show":         do_show
        }[command]
    except KeyError:
        print("Unknown file subcommand: ", command)
        sys.exit(2)
        
    return method(client, args[1:])
