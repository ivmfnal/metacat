from webpie import WPApp
import sys

def env(request, relpath):
	out = [f"Python version: {sys.version}\n"]
	return out + [f"{k}:\t{v}\n" for k, v in request.environ.items()], "text/plain"

application = WPApp(env)

if __name__ == "__main__":
    application.run_server(443, certfile="/tmp/config/cert.pem", keyfile="/tmp/config/key.pem",
        ca_file="/tmp/config/ca_bundle.pem", verify="required", allow_proxies=True)
