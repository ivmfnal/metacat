import sys, importlib, os.path, getopt
from pythreader import ShellCommand

Usage = """
python copy_modules.py [options] <module> [...] <out_dir>"
options:
    -q                          - quiet, do not print status information
    -c                          - create output directory if does not exist. Raise an error otherwise.
"""

def copy_module(mod_name, out_dir):
    mod = importlib.import_module(mod_name)
    mod_file = mod.__file__
    mod_dir, mod_file_name = os.path.split(mod.__file__)
    if mod_file_name.startswith("__init__"):
        cmd = f"/bin/cp -R {mod_dir} {out_dir}"
    else:
        cmd = f"/bin/cp {mod_file} {out_dir}"
    status, out, error = ShellCommand.execute(cmd)
    if status != 0:
        raise RuntimeError(f"Error copying module {mod_name}: {out}\n{error}")

opts, args = getopt.getopt(sys.argv[1:], "qch?")
opts = dict(opts)

if not args or "-?" in opts or "-h" in opts:
    print(Usage)
    sys.exit(2)

modules, out_dir = args[:-1], args[-1]

if not os.path.isdir(out_dir):
    if "-c" in opts:
        os.makedirs(out_dir, mode=0o744)
    else:
        print(f"Output directory {out_dir} does not exist. Use -c to create", file=sys.stderr)
        sys.exit(1)

for mod in modules:
    try:
        copy_module(mod, out_dir)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    if "-q" not in opts:
        print("copied:", mod)
