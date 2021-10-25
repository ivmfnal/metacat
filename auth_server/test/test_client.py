import requests, sys, getopt

opts, args = getopt.getopt(sys.argv[1:], "k:c:")

opts = dict(opts)
cert = opts["-c"]
key = opts.get("-k", cert)
url = args[0]

r = requests.get(url, cert=(cert, key), verify=False)

print (r)
for k, v in r.headers.items():
    print(k,'=', v)
print(r.text)


