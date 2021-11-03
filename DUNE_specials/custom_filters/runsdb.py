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
    
    def filter(self, inputs, *params, daqinterface_commit=None, mode=None, **ignore):
        db = self.ConnPool.connect()
        cursor = db.cursor()

        assert len(inputs) == 1

        filter = "" 
        if daqinterface_commit:
            filter += f" and daqinterface_commit='{daqinterface_commit}' "
        if mode:
            filter += f" and mode='{mode}' "

        colnames = ("," + ",".join(self.IncludeColumns)) if self.IncludeColumns else ""

        for chunk in inputs[0].chunked():
            by_run = {f.metadata()["core.runs"][0]:f for f in chunk}
            run_nums = list(by_run.keys())
            cursor.execute(f"""
                select runnum {colnames}
                    from {self.TableName}
                    where runnum = any(%s) {filter}
            """, (run_nums,))
            tup = cursor.fetchone()
            while tup:
                runnum, rest = tup[0], tup[1:]
                f = by_run[runnum]
                for c, v in zip(self.IncludeColumns, rest):
                    f.Metadata[f"{self.MetaPrefix}.{c}"] = v
                yield f
                tup = cursor.fetchone()
