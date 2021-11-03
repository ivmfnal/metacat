import jwt, getopt, yaml, sys

opts, args = getopt.getopt(sys.argv[1:], "c:")
opts = dict(opts)

config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
cmd = args[0]


if cmd == "create":
    msg = args[1]
    payload={"msg":msg}
    private_key = config["private_key"]
    #print("private key:", private_key)
    algorithm = config.get("algorithm", "RS256")
    token = jwt.encode(payload, private_key, algorithm=algorithm)
    #print(jwt.get_unverified_header(token))
    print(token)
elif cmd == "read":
    token = sys.stdin.read().strip()
    #print(jwt.get_unverified_header(token))
    public_key = config["public_key"]
    algorithm = config.get("algorithm", "RS256")
    payload = jwt.decode(token, key=public_key, algorithms=[algorithm])
    print(payload["msg"])

