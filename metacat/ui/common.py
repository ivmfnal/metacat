#
# not used
#

from metacat.webapi import MCError
import sys

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
        if arg.startswith('@'):
            file_path = arg[1:]
        elif os.path.isfile(arg):
            file_path = arg
        if file_path == "-":
            text = sys.stdin.read()
        elif file_path:
            text = open(file_path, "r").read()
    return text

def load_json(arg):
    meta = None
    text = load_text(arg)
    if text:
        meta = json.loads(text)
    return meta

