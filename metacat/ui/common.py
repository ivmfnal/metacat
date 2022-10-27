#
# not used
#

from metacat.webapi MCError
import sys

def catch_mc_errors(method):
    def decorated(*params, **args):
        try:
            return method(*params, **args)
        except MCError as e:
            print(e, file=sys.stderr)
            sys.exit(1)
    return decorated