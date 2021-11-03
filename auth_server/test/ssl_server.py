import ssl, sys, socket, pprint

port, keyfile, certfile, ca_file = sys.argv[1:]
SSLContext = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
print("SSLContext verify flags:", SSLContext.verify_flags)
SSLContext.load_cert_chain(certfile, keyfile)	#, password=password)
if ca_file != "-":
    SSLContext.load_verify_locations(cafile=ca_file)
SSLContext.verify_mode = ssl.CERT_OPTIONAL
"""
{
        "none":ssl.CERT_NONE,
        "optional":ssl.CERT_OPTIONAL,
        "required":ssl.CERT_REQUIRED
    }[verify]
"""
SSLContext.load_default_certs()


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(("", int(port)))
sock.listen(10)
while True:
    s, addr = sock.accept()
    ssl_socket = SSLContext.wrap_socket(s, server_side=True)
    pprint.pprint(ssl_socket.getpeercert())
    ssl_socket.send("OK")
    ssl_socket.close()
    
