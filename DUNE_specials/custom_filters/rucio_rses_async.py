import os
from pythreader import TaskQueue, Task
from metacat.filters import MetaCatFilter


class RucioReplicas(MetaCatFilter):
    """
    Inputs: single file set
    
    Parameters: none
    
    Output: The same file set with added file attribute rucio.rse = [<rse name>, ...] - list of RSEs where replicas of the file can be found
    
    Configuration:
        rucio_config:   path to Rucio client configuration file. If unspecified, standard Rucio config file lookup procedure will be used.
    """

    def __init__(self, config):
        MetaCatFilter.__init__(self, config)
        self.RucioConfig = config.get("rucio_config")
        self.TaskQueue = TaskQueue(10)

    def list_replicas(self, client, chunk):
        chunk = list(chunk)
        dids = [{"scope":f.Namespace, "name":f.Name} for f in chunk]
        replicas = client.list_replicas(dids, all_states=False, ignore_availability=False, resolve_archives=False)
        rses_by_did = {"%(scope)s:%(name)s" % r : list(r["rses"].keys()) for r in replicas}
        for f in chunk:
            f.Metadata["rucio.rses"] = rses_by_did.get(f.did(), [])
        return chunk

    def filter(self, inputs, *params, **ignore):

        from rucio.client.replicaclient import ReplicaClient
        if self.RucioConfig is not None:
            os.environ["RUCIO_CONFIG"] = self.RucioConfig
        client = ReplicaClient()

        promises = [self.TaskQueue.add(self.list_replicas, client, chunk).promise for chunk in inputs[0].chunked(chunk_size=10000)]
        for promise in promises:
            chunk = promise.wait()
            yield from chunk

def create_filters(config):
    return {
        "rucio_replicas":      RucioReplicas(config)
    }
