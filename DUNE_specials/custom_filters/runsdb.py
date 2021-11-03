from metacat.filters import MetaCatFilter
from wsdbtools import ConnectionPool


class RunsDB(MetaCatFilter):
    
    def __init__(self, config):
        self.Config = config
        self.Connection = self.Config["connection"]
        self.ConnPool = ConnectionPool(postgres=self.Connection, max_idle_connections=1)
        self.TableName = self.Config["table"]
        self.IncludeColumns = self.Config["columns"]
        self.MetaPrefix = self.Config.get("meta_prefix", "runs_history")
    
    def filter(self, inputs, params, **ignore):
        daq_inter_version = None
        if params:
            daq_inter_version, = params
        db = self.ConnPool.connect()
        cursor = db.cursor()
        assert len(inputs) == 1

        filter = "" if not daq_inter_version else f"and daqinterface_commit='{daq_inter_version}'"

        for chunk in inputs[0].chunked():
            by_run = {f.metadata()["core.runs"][0]:f for f in chunk}
            run_nums = list(by_run.keys)
            c.execute(f"""
                select runnum, daqinterface_commit, {self.IncludeColumns}
                    from {self.TableName}
                    where runnum = any(%s) {filter}
            """, (run_nums,))
            tup = c.fetchone()
            while tup:
                (runnum, daqinterface_commit), rest = tup[:2], tup[2:]
                rest = {f"{self.MetaPrefix}.{c}":v for (c, v) in zip(self.IncludeColumns, rest)}
                f = by_run[runnum]
                f.Metadata.update(rest)
                f.Metadata[f"{self.MetaPrefix}.runnum"] = runnum
                f.Metadata[f"{self.MetaPrefix}.daqinterface_commit"] = daqinterface_commit
                yield f
                tup = c.fetchone()
