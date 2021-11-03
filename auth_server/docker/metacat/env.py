from webpie import WPApp
import sys

def env(request, relpath):
	out = [f"Python version: {sys.version}\n"]
	return out + [f"{k}:\t{v}\n" for k, v in request.environ.items()], "text/plain"

application = WPApp(env)
