import sys, getopt, os, json, fnmatch, pprint, datetime
#from urllib.request import urlopen, Request
from metacat.util import to_bytes, to_str, epoch
from metacat.webapi import MetaCatClient, MCError

from metacat.ui.cli import CLI, CLICommand, InvalidArguments

Usage = """
Usage: 
    metacat dataset <command> [<options>] ...
    
    Commands and options:
    
        list [<options>] [[<namespace pattern>:]<name pattern>]
            -l|--long           - detailed output
            -c|--file-counts    - include file counts if detailed output
            
        create [<options>] <namespace>:<name> [<description>]
            -M|--monotonic
            -F|--frozen
            -m|--metadata '<JSON expression>'
            -m|--metadata @<JSON file>
            
        add <parent dataset namespace>:<parent name> <child dataset namespace>:<child name> [<child dataset namespace>:<child name> ...]

        remove <parent namespace>:<parent name> <child namespace>:<child name> 
        
        show [<options>] <namespace>:<name>
            -j|--json       - print as JSON
            -p|--pprint     - Python pprint
        
        update <options> <namespace>:<name> [<description>]
            -M|--monotonic (yes|no) - set/reset monotonic flag
            -F|--frozen (yes|no)    - set/reset monotonic flag
            -r|--replace            - replace metadata, otherwise update
            -m|--metadata @<JSON file with metadata> 
            -m|--metadata '<JSON expression>' 
"""

class ListDatasetFilesCommand(CLICommand):
    
    Opts = "mj with-metadata"
    Usage = """[<options>] <dataset namespace>:<dataset name>           -- list dataset files
        -m|--with-metadata      - include file metadata
        -j                      - as JSON
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        dataset_did = args[0]
        with_meta = "-m" in opts or "--with-metadata" in opts
        files = client.get_dataset_files(dataset_did, with_metadata = with_meta)
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
    Usage = """ [<options>] [<namespace pattern>:<name pattern>]      -- list datasets
            -l|--long           - detailed output
            -c|--file-counts    - include file counts if detailed output
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
        include_counts = verbose and ("-c" in opts or "--file-counts" in opts)
        output = list(client.list_datasets(ns_pattern, name_pattern, with_counts=include_counts))
        output = sorted(output, key=lambda ds:(ds["namespace"], ds["name"]))
    
        verbose_format = "%-16s %-19s %10s %5s/%-5s %s"
        header_format = "%-16s %-19s %-10s %-11s %s"
    
        if verbose:
            print(header_format % (
                "creator", "created", "files", "subsets", "namespace:name"
            ))
            print("-"*16, "-"*19, "-"*10, "-"*11, "-"*60)
    
        for item in output:
            match = False
            namespace, name = item["namespace"], item["name"]
            for p in patterns:
                pns = None
                if ":" in p:
                    pns, pn = p.split(":", 1)
                else:
                    pn = p
                if fnmatch.fnmatch(name, pn) and (pns is None or fnmatch.fnmatch(namespace, pns)):
                    match = True
                    break
            if match:
                if verbose:
                    ct = item.get("created_timestamp")
                    if not ct:
                        ct = ""
                    else:
                        ct = datetime.datetime.fromtimestamp(ct).strftime("%Y-%m-%d %H:%M:%S")
                    file_count = item.get("file_count")
                    if file_count is None:
                        file_count = "?"
                    else:
                        file_count = str(file_count)
                    child_count = item.get("child_count", "?")
                    subset_count = item.get("subset_count", "?")
                    print(verbose_format % (
                        item.get("creator") or "",
                        ct,
                        file_count, child_count, subset_count,
                        namespace + ":" + name
                    ))
                else:
                    print("%s:%s" % (namespace, name))
                    

class ShowDatasetCommand(CLICommand):
    
    Opts = ("pj", ["pprint=","json"])
    Usage = """[<options>] <namespace>:<name>                           -- print dataset info
            -j|--json       - print as JSON
            -p|--pprint     - Python pprint
    """
    MinArgs = 1

    def __call__(self, command, client, opts, args):
        info = client.get_dataset(args[0])
        if "-p" in opts or "--pprint" in opts:
            pprint.pprint(info)
        elif "-j" in opts or "--json" in opts:
            print(json.dumps(info, indent=4, sort_keys=True))
        else:
            for k, v in sorted(info.items()):
                if k == "created_timestamp":
                    v = "" if not v else datetime.datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S")
                elif k == "children" or k == "parents":
                    n = len(v)
                    if n <= 5:
                        v = " ".join(v)
                    else:
                        v = "(%d) " % (n,) + " ".join(v[:5]) + " ..."
                elif k == "metadata":
                    v = json.dumps(v or {})
                print("%-25s: %s" % (k, v))
                    

class AddSubsetCommand(CLICommand):

    Usage = """<parent dataset namespace>:<parent name> <child dataset namespace>:<child name> [<child dataset namespace>:<child name> ...]
    """
    MinArgs = 2

    def __call__(self, command, client, opts, args):
        parent, children = args[0], args[1:]
        for child in children:
            client.add_child_dataset(parent, child)

def load_metadata(opts):
    # return None if no -j in options
    meta = None
    if "-m" in opts or "--metadata" in opts:
        arg = opts.get("-m") or opts.get("--metadata")
        if arg.startswith('@'):
            meta = json.load(open(arg[1:], "r"))
        else:
            meta = json.loads(arg)
    return meta
    
class CreateDatasetCommand(CLICommand):

    Opts = ("MFm:f:d:r:", ["monotonic", "frozen", "metadata=", "dataset-query=", "file-query=", "meta-requirements="])
    Usage = """[<options>] <namespace>:<name> [<description>]           -- create dataset
        -M|--monotonic
        -F|--frozen
        -m|--metadata '<JSON expression>'
        -m|--metadata @<JSON file>
        -f|--file-query '<MQL file query>'          - run the query and add files to the dataset
        -f|--file-query @<file_with_query>          - run the query and add files to the dataset
        -r|--meta-requirements '<JSON expression>'  - add metadata requirements
        -r|--meta-requirements @<JSON file>         - add metadata requirements
        """
    MinArgs = 1
    
    def get_text(self, opts, opt_keys):
        opt_value = None
        for key in opt_keys:
            opt_value = opts.get(key)
            if opt_value:   break
        if opt_value:
            if opt_value.startswith('@'):
                opt_value = open(opt_value[1:], "r").read()
        return opt_value

    def __call__(self, command, client, opts, args):
        dataset_spec, desc = args[0], args[1:]
        if desc:
            desc = " ".join(desc)
        else:
            desc = ""    
        monotonic = "-M" in opts or "--monotonic" in opts
        frozen = "-F" in opts or "--frozen" in opts
        metadata = load_metadata(opts) or {}
        files_query = self.get_text(opts, ["-f", "--file-query"])
        #subsets_query = self.get_text(opts, ["-d", "--datasets-query"])
        try:
            out = client.create_dataset(dataset_spec, monotonic = monotonic, frozen = frozen, description=desc, metadata = metadata,
                files_query = files_query
            )
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            print(out)

class UpdateDatasetCommand(CLICommand):

    Opts = ("MFm:r", ["replace", "monotonic", "frozen", "metadata="])
    Usage = """<options> <namespace>:<name> [<description>]             -- modify dataset info
            -M|--monotonic (yes|no) - set/reset monotonic flag
            -F|--frozen (yes|no)    - set/reset monotonic flag
            -r|--replace            - replace metadata, otherwise update
            -m|--metadata @<JSON file with metadata> 
            -m|--metadata '<JSON expression>' 
    """
    MinArgs = 1

    def __call__(self, command, client, opts, args):
        mode = "replace" if ("-r" in opts or "--replace" in opts) else "update"

        if not args or args[0] == "help":
            print(Usage)
            sys.exit(2)
        
        metadata = load_metadata(opts)

        dataset = args[0]
        monotonic = frozen = None
        if "-M" in opts or "--monotonic" in opts:    
            monotonic = opts.get("-M") or opts.get("--monotonic")
            if not monotonic in ("yes", "no"):
                print("Invalid value for -M or --monotonic option:", monotonic, ". Valid values are 'yes' and 'no'")
                sys.exit(2)
            monotonic = monotonic == "yes"
        if "-F" in opts or "--frozen" in opts:    
            frozen = opts.get("-F") or opts.get("--frozen")
            if not frozen in ("yes", "no"):
                print("Invalid value for -F or --frozen option:", frozen, ". Valid values are 'yes' and 'no'")
                sys.exit(2)
            frozen = frozen == "yes"
        desc = None
        if args[1:]:
            desc = " ".join(args[1:])

        try:
            response = client.update_dataset(dataset, metadata=metadata, frozen=frozen, monotonic=monotonic, mode=mode, description=desc)
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            print(response)

class AddFilesCommand(CLICommand):
    
    Opts = ("i:j:d:N:sq:", ["namespace=", "json=", "dids=", "ids=", "sample", "query="])
    Usage = """[options] <dataset namespace>:<dataset name>

            list files by DIDs or namespace/names
            -N|--namespace <default namespace>           - default namespace for files
            -d|--names <file namespace>:<file name>[,...]
            -d|--names -            - read the list from stdin
            -d|--names @<file>      - read the list from file

            list files by file id
            -i|--ids <file id>[,...]
            -i|--ids -              - read the list from stdin
            -i|--ids @<file>        - read the list from file

            read file list from JSON file
            -j|--json <json file>
            -j|--json -             - read JSON file list from stdin
            -s|--sample             - print JOSN file list sample

            add files matching a query
            -q|--query "<MQL query>"
            -q|--query @<file>      - read query from the file
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

    def get_text(self, opts, opt_keys):
        opt_value = None
        for key in opt_keys:
            opt_value = opts.get(key)
            if opt_value:   break
        if opt_value:
            if opt_value.startswith('@'):
                opt_value = open(opt_value[1:], "r").read()
            if opt_value == "-":
                opt_value = sys.stdin.read()
        return opt_value

    def get_list(self, opts, opt_keys):
        opt_value = self.get_text(opts, opt_keys)
        out = []
        if opt_value:
            for line in opt_value.split():
                for word in line.split(","):
                    word = word.strip()
                    if word:
                        out.append(word)
        return out

    def __call__(self, command, client, opts, args):

        if "--sample" in opts or "-s" in opts:
            print(self.AddSample)
            sys.exit(0)

        if len(args) != 1:
            raise InvalidArguments("Invalid arguments")

        default_namespace = opts.get("-N")
        files = query = None
        if "-i" in opts or "--ids" in opts:
            files = [{"fid": fid} for fid in self.get_list(opts, ["-i", "--ids"])]
        elif "-d" in opts or "--dids" in opts:
            files = [{"did": did} for did in self.get_list(opts, ["-d", "--dids"])]
        elif "-j" in opts or "--json" in opts:
            json_file = opts.get("-j") or opts.get("--json")
            files = json.loads(self.get_text(opts, ["-j", "--json"]))
        else:
            query = opts.get("-q") or opts("--query")
            
        if (query is None) == (files is None):
            raise InvalidArguments("Eitther file list or a query must be specified, but not both")

        dataset = args[-1]
        try:
            out = client.add_files(dataset, file_list=files, query=query)
        except MCError as e:
            print(e)
            sys.exit(1)
        else:
            print("Added", len(out), "files")

DatasetCLI = CLI(
    "create",       CreateDatasetCommand(),
    "show",         ShowDatasetCommand(),
    "files",        ListDatasetFilesCommand(),
    "list",         ListDatasetsCommand(),
    "add-subset",   AddSubsetCommand(),
    "add-files",    AddFilesCommand(),
    "update",       UpdateDatasetCommand()
)
    
 
