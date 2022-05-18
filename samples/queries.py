from metacat.webapi import MetaCatClient
import pprint

client = MetaCatClient("https://metacat.fnal.gov:9443/dune_meta_demo/app")

print("query 1: ---")
for f in client.query("files from dune:all skip 100 limit 10"):
    pprint.pprint(f)

print("query 2: ---")
for f in client.query("files from dune:all where creator='php13tkw' skip 105 limit 10", 
            with_metadata=True):
    pprint.pprint(f)
