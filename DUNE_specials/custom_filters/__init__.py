def create_filters(config):
    from .runsdb import RunsDB
    
    filters = {
        "dune_runsdb":  RunsDB(config["runsdb"])
    }
    return filters
    