from metacat.webapi import MetaCatClient

client = MetaCatClient("https://metacat.fnal.gov:9443/dune_meta_demo/app")

p1 = client.async_query("files from dune:all skip 100 limit 10")
p2 = client.async_query("files from dune:all skip 105 limit 10")


print("query 1: ---")
for f in p1.wait():
    print(f)
    
print("query 2: ---")
for f in p2.wait():
    print(f)
    
