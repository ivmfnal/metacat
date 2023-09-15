import sys, getopt, os, json, fnmatch, pprint
from datetime import datetime, timezone
from textwrap import indent
#from urllib.request import urlopen, Request
from metacat.util import to_bytes, to_str, epoch, ObjectSpec
from metacat.webapi import MetaCatClient, MCError

from metacat.ui.cli import CLI, CLICommand, InvalidArguments
from .common import load_text, load_json, load_file_list

class ListDatasetFilesCommand(CLICommand):
    
    Opts = "mjr with-metadata include-retired-files"
    Usage = """[<options>] <dataset namespace>:<dataset name>          -- list dataset files
        -m|--with-metadata              - include file metadata
        -r|--include-retired-files      - include retired files
        -j                              - as JSON
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        dataset_did = args[0]
        with_meta = "-m" in opts or "--with-metadata" in opts
        files = client.get_dataset_files(dataset_did,
                    with_metadata = with_meta,
                    include_retired_files = "-r" in opts or "--include-retired-files" in opts)
        if "-j" in opts:
            first = True
            print("[")
            for f in files:
                if not first:   print(",")
                print(json.dumps(f, indent=2, sort_keys=True), end="")
                first = False
            print("\n]")
        else:
            for f in files:
                print(f"%s:%s" % (f["namespace"], f["name"]))

class ListDatasetsCommand(CLICommand):
    
    Opts = ("lc", ["--long", "--file-counts"])
    Usage = """[<options>] [<namespace pattern>:<name pattern>]        -- list datasets
            -l|--long               - detailed output
                -c|--file-counts    - if detailed output, include exact file counts -- can take long time !
            """
    
    def __call__(self, command, client, opts, args):
        if args:
            patterns = args[0]
        else:
            patterns = "*:*"
        
        if not ':' in patterns:
            raise InvalidArguments()

        ns_pattern, name_pattern = patterns.split(':', 1)
            
        verbose = "-l" in opts or "--long" in opts
        exact_counts = verbose and ("-c" in opts or "--file-counts" in opts)
        output = list(client.list_datasets(ns_pattern, name_pattern, with_counts=exact_counts))
        output = sorted(output, key=lambda ds:(ds["namespace"], ds["name"]))
    
        verbose_format = "%-16s %-23s %10s %s"
        header_format = "%-16s %-23s %-10s %s"
        divider = " ".join(("-"*16, "-"*23, "-"*10, "-"*60))
        columns = ("creator", "created", "files", "namespace:name")
            
        if verbose:
            print(header_format % columns)
            print(divider)
    
        for item in output:
            match = False
            namespace, name = item["namespace"], item["name"]
            if fnmatch.fnmatch(name, name_pattern) and fnmatch.fnmatch(namespace, ns_pattern):
                if verbose:
                    ct = item.get("created_timestamp")
                    if not ct:
                        ct = ""
                    else:
                        ct = datetime.fromtimestamp(ct, timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                    file_count = item.get("file_count")
                    if file_count is None:
                        file_count = "?"
                    else:
                        file_count = str(file_count)
                    print(verbose_format % (
                        item.get("creator") or "",
                        ct,
                        file_count,
                        namespace + ":" + name
                    ))
                else:
                    print("%s:%s" % (namespace, name))
                    

class ShowDatasetCommand(CLICommand):
    
    Opts = ("pj", ["pprint=","json"])
    Usage = """[<options>] <namespace>:<name>
            -j|--json       - print as JSON
            -p|--pprint     - Python pprint
    """
    MinArgs = 1

    def __call__(self, command, client, opts, args):
        info = client.get_dataset(args[0])
        if info is None:
            print("Dataset not found")
            sys.exit(1)
        if "-p" in opts or "--pprint" in opts:
            pprint.pprint(info)
        elif "-j" in opts or "--json" in opts:
            print(json.dumps(info, indent=4, sort_keys=True))
        else:
            print("Namespace:            ", info["namespace"])
            print("Name:                 ", info["name"])
            print("Description:          ", info.get("description") or "")
            print("Creator:              ", info.get("creator") or "")
            ct = info.get("created_timestamp") or ""
            if ct:
                ct = datetime.fromtimestamp(ct, timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            print("Create timestamp:     ", ct)
            ut = info.get("updated_timestamp") or ""
            if ut:
                ut = datetime.fromtimestamp(ut, timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            print("Updated by:           ", info.get("updated_by") or "")
            print("Update timestamp:     ", ut)
            print("Estimated file count: ", info.get("file_count"), "")
            print("Restricted:           ", "frozen" if info.get("frozen", False) else (
                                            "monotonic" if info.get("monotonic", False) else "no"
                                            )
            )
            print("Metadata:")
            if info.get("metadata"):
                print(indent(json.dumps(info["metadata"], indent=4, sort_keys=True), "  "))
            print("Constraints:")
            for name, constraint in sorted(info.get("file_meta_requirements", {}).items()):
                line = "  %-40s %10s" % (name, "required" if constraint.get("required", False) else "")
                if "values" in constraint:
                    line += " %s" % (tuple(constraint["values"]),)
                rng = None
                if "min" in constraint:
                    rng = [repr(constraint["min"]), ""]
                if "max" in constraint:
                    if rng is None: rng = ["", ""]
                    rng[1] = repr(constraint["max"])
                if rng is not None:
                    line += " [%s - %s]" % tuple(rng)
                if "pattern" in constraint:
                    line += " ~ '%s'" % (constraint["pattern"])
                print(line)
                    

class AddSubsetCommand(CLICommand):

    Usage = """<parent dataset namespace>:<parent name> <child dataset namespace>:<child name> [<child dataset namespace>:<child name> ...]
    """
    MinArgs = 2

    def __call__(self, command, client, opts, args):
        parent, children = args[0], args[1:]
        for child in children:
            client.add_child_dataset(parent, child)

class CreateDatasetCommand(CLICommand):

    Opts = ("m:q:jf:", ["flags=", "metadata=", "query=", "json"])
    Usage = """[<options>] <namespace>:<name> [<description>]          -- create dataset
        -f|--flags (monotonic|frozen)               - optional, dataset flags
        -m|--metadata '<JSON expression>'
        -m|--metadata <JSON file>
        -m|--metadata -                             - read metadata as JSON from stdin
        -q|--query '<MQL file query>'               - run the query and add files to the dataset
        -q|--query <file_with_query>                - run the query and add files to the dataset
        -q|--query -                                - read the query from stdin
        -j|--json                                   - print dataset information as JSON
        """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        dataset_spec, desc = args[0], args[1:]
        if desc:
            desc = " ".join(desc)
        else:
            desc = ""
        flags = opts.get("-f") or opts.get("--flags")
        monotonic = flags == "monotonic"
        frozen = flags == "frozen"
        metadata = load_json(opts.get("-m") or opts.get("--metadata")) or {}
        files_query = load_text(opts.get("-q") or opts.get("--query")) or None
        try:
            out = client.create_dataset(dataset_spec, monotonic = monotonic, frozen = frozen, description=desc, metadata = metadata,
                files_query = files_query
            )
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            if "-j" in opts or "--json" in opts:
                print(json.dumps(out, indent=4, sort_keys=True))
            else:
                nfiles = out.get("file_count")
                print(f"Dataset {dataset_spec} cteated", f"with {nfiles} files" if nfiles is not None else "")

class UpdateDatasetCommand(CLICommand):

    Opts = ("f:m:rj", ["replace", "flags=", "metadata=", "json"])
    Usage = """<options> <namespace>:<name> [<description>]            -- modify dataset info

            update dataset metadata and flags

            -f|--flags (monotonic|frozen|-)             - optional, dataset flags, use '-' to remove restrictions
            -m|--metadata <JSON file with metadata> 
            -m|--metadata '<JSON expression>'
            -m|--metadata -                             - read metadata from stdin
            -r|--replace                                - replace metadata, otherwise update
            -j|--json                                   - print updated dataset information as JSON
    """
    MinArgs = 1

    def __call__(self, command, client, opts, args):
        mode = "replace" if ("-r" in opts or "--replace" in opts) else "update"
        flags = opts.get("-f") or opts.get("--flags")
        if flags not in ("-", "monotonic", "frozen", None):
            raise InvalidArguments(f"Invalid value for dataset flags: {flags}")
        
        monotonic = frozen = None
        if flags == "monotonic":        monotonic = True
        elif flags == "frozen":         frozen = True
        elif flags == "-":              monotonic = frozen = False

        metadata = load_json(opts.get("-m") or opts.get("--metadata")) or {}

        dataset = args[0]
        desc = None
        if args[1:]:
            desc = " ".join(args[1:])

        try:
            response = client.update_dataset(dataset, metadata=metadata, 
                frozen=frozen, monotonic=monotonic, 
                mode=mode, description=desc)
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            if "-j" in opts or "--json" in opts:
                print(json.dumps(response, indent=4, sort_keys=True))

class AddFilesCommand(CLICommand):
    
    Opts = ("i:d:N:sq:f:j:", ["json=", "dids=", "ids=", "sample", "query=", "files=", "names="])
    Usage = """[options] <dataset namespace>:<dataset name>            -- add files to a dataset

            add files by DIDs or namespace/names or MQL query

            -f|--files (<did>|<file id>)[,...]          - dids and fids can be mixed
            -f|--files <file with DIDs or file ids>     - one did or fid per line
            -f|--files <json file>                      - list of dictionaries:
                                                            { "fid": ...} or
                                                            { "namespace": ..., "name":... } or
                                                            { "did":... } or
            -f|--files -                                - read file list from stdin

            add files selected by a query
            -q|--query "<MQL query>"
            -q|--query <file>                           - read query from the file
            -q|--query -                                - read query from stdin
    """
    
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
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):

        if "--sample" in opts or "-s" in opts:
            print(self.AddSample)
            sys.exit(0)

        if len(args) != 1:
            raise InvalidArguments("Invalid arguments")

        # backward compatibility
        if "-f" not in opts and "--files" not in opts:
            opts["-f"] = opts.get("-d") or opts.get("--names") or \
                        opts.get("-i") or opts.get("--ids") or \
                        opts.get("-j") or opts.get("--json")

        if opts.get("-d") or opts.get("--names") or \
                        opts.get("-i") or opts.get("--ids") or \
                        opts.get("-j") or opts.get("--json"):
            print("", file=sys.stderr)
            print("Options -j, --json, -d, --names, -i, --ids are deprecated.\nPlease use -f|--files instead", file=sys.stderr)
            print("", file=sys.stderr)

        default_namespace = opts.get("-N")
        files = query = None
        
        file_list = opts.get("-f") or opts.get("--files")
        if file_list:
            files = load_file_list(file_list)
        else:
            query = load_text(opts.get("-q") or opts.get("--query"))
            
        if (query is None) == (files is None):
            raise InvalidArguments("Eitther file list or a query must be specified, but not both")

        dataset = args[0]
        try:
            nadded = client.add_files(dataset, file_list=files, query=query)
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            print("Added", nadded, "files")

class RemoveFilesCommand(CLICommand):
    
    Opts = ("N:q:f:s", ["query=", "files=", "sample"])
    Usage = """[options] <dataset namespace>:<dataset name>            -- remove files from a dataset

            remove files by file ids, DIDs or namespace/names
            -f|--files (<did>|<file id>)[,...]          - dids and fids can be mixed
            -f|--files <file with DIDs or file ids>     - one did or fid per line
            -f|--files <json file>                      - list of dictionaries:
                                                            { "fid": ...} or
                                                            { "namespace": ..., "name":... } or
                                                            { "did":... } or
            -f|--files -                                - read file list from stdin

            remove files selected by a query
            -q|--query "<MQL query>"
            -q|--query <file>                           - read query from the file
            -q|--query -                                - read query from stdin
    """
    
    RemoveSample = json.dumps(
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
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):

        if "--sample" in opts or "-s" in opts:
            print(self.RemoveSample)
            sys.exit(0)

        if len(args) != 1:
            raise InvalidArguments("Invalid arguments")

        # backward compatibility
        if "-f" not in opts and "--files" not in opts:
            opts["-f"] = opts.get("-d") or opts.get("--names") or \
                        opts.get("-i") or opts.get("--ids") or \
                        opts.get("-j") or opts.get("--json")

        if opts.get("-d") or opts.get("--names") or \
                        opts.get("-i") or opts.get("--ids") or \
                        opts.get("-j") or opts.get("--json"):
            print("", file=sys.stderr)
            print("Options -j, --json, -d, --names, -i, --ids are deprecated.\nPlease use -f|--files instead", file=sys.stderr)
            print("", file=sys.stderr)

        default_namespace = opts.get("-N")
        files = query = None
        
        file_list = opts.get("-f") or opts.get("--files")
        if file_list:
            files = load_file_list(file_list)
        else:
            query = load_text(opts.get("-q") or opts.get("--query"))
            
        if (query is None) == (files is None):
            raise InvalidArguments("Eitther file list or a query must be specified, but not both")

        dataset = args[0]
        try:
            nremoved = client.remove_files(dataset, file_list=files, query=query)
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            print("Added", nremoved, "files")

class RemoveDatasetCommand(CLICommand):
    
    Usage = """<dataset namespace>:<dataset name>                      -- demove a dataset
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        dataset = args[0]
        try:
            nremoved = client.remove_dataset(dataset)
        except MCError as e:
            print(e)
            sys.exit(1)

DatasetCLI = CLI(
    "create",       CreateDatasetCommand(),
    "show",         ShowDatasetCommand(),
    "files",        ListDatasetFilesCommand(),
    "list",         ListDatasetsCommand(),
    "add-subset",   AddSubsetCommand(),
    "add-files",    AddFilesCommand(),
    "remove-files", RemoveFilesCommand(),
    "update",       UpdateDatasetCommand(),
    "remove",       RemoveDatasetCommand()
)
    
 
