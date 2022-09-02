import os
from metacat.filters import MetaCatFilter

class RucioReplicas(MetaCatFilter):

    def __init__(self, config):
        self.RucioConfig = config.get("rucio_config")

    def filter(self, inputs, *params, **ignore):

        from rucio.client.replicaclient import ReplicaClient
        if self.RucioConfig is not None:
            os.environ["RUCIO_CONFIG"] = self.RucioConfig
        client = ReplicaClient()

        for chunk in inputs[0].chunked():
            chunk_files = {f.did(): f for f in chunk}
            dids = [{"scope":f.Namespace, "name":f.Name} for f in chunk]
            for f in chunk_files.values():
                f.Metadata["rucio.rses"] = []

            replicas = client.list_replicas(dids, all_states=False, ignore_availability=False,
                resolve_archives=False)

            for r in replicas:
                did = "%(scope)s:%(name)s" % r
                f = chunk_files[did]
                f.Metadata["rucio.rses"] = list(r["rses"].keys())

            yield from chunk_files.values()
