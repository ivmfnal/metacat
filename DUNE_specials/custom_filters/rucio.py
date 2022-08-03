import os
from metacat.filters import MetaCatFilter
from metacat.util import Tracer

class RucioReplicas(MetaCatFilter):
    
    def __init__(self, config):
        self.RucioConfig = config.get("rucio_config")
    
    def filter(self, inputs, *params, rse_expression=None, **ignore):
        T = Tracer()
        from rucio.client.replicaclient import ReplicaClient
        if self.RucioConfig is not None:
            os.environ["RUCIO_CONFIG"] = self.RucioConfig
        client = ReplicaClient()
        out = []
        with T["cnunk_loop"]:
            for chunk in inputs[0].chunked():
                chunk_files = { f.did(): f for f in chunk }
                dids = [{"scope":f.Namespace, "name":f.Name} for f in chunk]
                if rse_expression is None:
                    # no RSEs selected - include all files
                    for f in chunk_files.values():
                        f.Metadata["rucio.rses"] = []
                    with T["list_replicas()"]:
                        replicas = client.list_replicas(dids, all_states=False, ignore_availability=False)
                    with T["update_meta"]:
                        for r in replicas:
                            with T["update_meta_single"]:
                                    did = "%(scope)s:%(name)s" % r
                                    f = chunk_files[did]
                                    f.Metadata["rucio.rses"] = list(r["rses"])
                    with T["yielding"]:
                        for f in chunk_files.values():
                            out.append(f)
                else:
                    # include only files with replicas in specific RSEs
                    with T["list_replicas()"]:
                        replicas = client.list_replicas(dids, all_states=False, ignore_availability=False,
                            rse_expression=rse_expression)
                    for r in replicas:
                        did = "%(scope)s:%(name)s" % r
                        f = chunk_files[did]
                        f.Metadata["rucio.rses"] = list(r["rses"])
                        out.append(f)
        print("trace: ", T.formatStats())
        return out
                


