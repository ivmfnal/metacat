import os

def application(env, start_response):
        start_response("200 OK", [("Content-Type","text/plain")])
        out = (
                ["Request environmet:\n"] + 
                ["%s=%s\n" % (k, v) for k, v in sorted(env.items())] +
                ["\nOS environment:\n"] + 
                ["%s=%s\n" % (k, v) for k, v in sorted(os.environ.items())]
        )
        return [x.encode("utf-8") for x in out]
	
