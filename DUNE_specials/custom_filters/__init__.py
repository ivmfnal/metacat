def create_filters(config):
    from .runsdb import RunsDB
    from .rucio import RucioReplicas
    
    filters = {
        "dune_runsdb":      RunsDB(config.get("runsdb", {})),
        "rucio_replicas":   RucioReplicas(config.get("rucio", {}))
    }
    return filters
