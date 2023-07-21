#
# not used
#

from metacat.webapi import MCError
from metacat.util import ObjectSpec
import sys, json, os.path

def catch_mc_errors(method):
    def decorated(*params, **args):
        try:
            return method(*params, **args)
        except MCError as e:
            print(e, file=sys.stderr)
            sys.exit(1)
    return decorated

def load_text(arg):
    text = arg
    if arg:
        file_path = None
        if arg.startswith('@'):             # accept "@<file>" for backward compatibility
            file_path = arg[1:]
        elif os.path.isfile(arg):
            file_path = arg
        if file_path:
            text = open(file_path, "r").read()
        elif arg == "-":
            text = sys.stdin.read()
    return text

def load_json(arg):
    data = None
    text = load_text(arg)
    if text:
        data = json.loads(text)
    return data

def parse_file_spec(spec):
    if isinstance(spec, dict) and "did" in spec:
        ns, n = spec["did"].split(':', 1)
        return {"namespace":ns, "name":n}

def load_file_list(arg):
    text = load_text(arg)
    data = []
    try:
        data = json.loads(text)
    except:
        #print(f"load_file_list: received text: [{text}]")
        for line in text.split("\n"):
            line = line.strip()
            if line:
                for item in line.split(","):
                    item = item.strip()
                    if item:
                        data.append(item)
    #print("load_file_list: data:", data)
    return [ObjectSpec(item).as_dict() for item in data]
        
