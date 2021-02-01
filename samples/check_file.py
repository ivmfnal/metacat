#
# check if a file exists in MetaCat
#

Usage = """
python check_file.py [-s <server_url>] <namespace>:<name>
python check_file.py [-s <server_url>] <file_id>
"""

import sys, getopt, os
from metacat.webapi import MetaCatClient

opts, args = getopt.getopt(sys.argv[1:], "s:")
opts = dict(opts)

if not args:
    print(Usage, file=sys.stderr)
    sys.exit(2)

server_url = opts.get("-s", os.environ.get("METACAT_SERVER_URL"))
if not server_url:
    print("Server URL must be specified either using -s option or environment variable METACAT_SERVER_URL", file=sys.stderr)
    sys.exit(2)

client = MetaCatClient(server_url)
nmissing = 0
for spec in args:
    if ':' in spec:
        f = client.get_file_info(name=spec)
    else:
        f = client.get_file_info(fid=spec)
    if f is None:
        print(spec)
        nmissing += 1

if nmissing:
    sys.exit(1)


