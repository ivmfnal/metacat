import os

class RucioReplicas(MetaCatFilter):
    
    def __init__(self, config):
        self.RucioConfig = config.get("rucio_config")
    
    def filter(self, inputs, *params, rse_expression=None, **ignore):
        from rucio.client.replicaclient import ReplicaClient
        if self.RucioConfig is not None:
            os.environ["RUCIO_CONFIG"] = self.RucioConfig
        client = ReplicaClient()
        for chunk in inputs[0].clunked():
            chunk_files = { f.did(): f }
            if rse_expression is None:
                # no RSEs selected - include all files
                for f in chunk_files.values():
                    f.Metadata["rucio.rses"] = []
                replicas = client.list_replicas(list(chunk_files.keys()), all_states=False, ignore_availability=False)
                for r in replicas:
                    did = "%(scope)s:%(name)s" % r
                    f = chunk_files[did]
                    f.Metadata["rucio.rses"] = list(r["rses"])
                for f in chunk_files.values():
                    yield f
            else:
                # include only files with replicas in specific RSEs
                replicas = client.list_replicas(list(chunk_files.keys()), all_states=False, ignore_availability=False,
                    rse_expression=rse_expression)
                for r in replicas:
                    did = "%(scope)s:%(name)s" % r
                    f = chunk_files[did]
                    f.Metadata["rucio.rses"] = list(r["rses"])
                    yield f
                
                


