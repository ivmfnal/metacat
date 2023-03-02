from metacat.filters import MetaCatFilter
from wsdbtools import ConnectionPool
import re


class RunsDB(MetaCatFilter):
    """
    Inputs: single file set
    
    Positional parameters: none
    
    Keyword parameters:
        daqinterface_commit: string, daqinterface_commit value for the corresponding run
        mode: string, mode for the corresponding run
    
    Description: Uses core.runs metadata values for each file to get information from Runs database. 
        Selects only runs with given "daqinterface_commit" and "mode" column values.
        For each run record found in the runs database, adds <category>.<column name> value to the file metadata.
        If the file corresponds to multiple run, which run's data will be used is currently undefined.
    
    Configuration:
        connection:     Postgres connection string to use to connect to the Runs database ("host=... port=... dbname=... ...")
        table:          table name
        columns:        list of columns to add values from
        meta_prefix:    metadata parameter category to use to add metadata values
    """
    
    def __init__(self, config):
        self.Config = config
        show_config = config.copy()
        show_config["connection"] = self.hide(show_config["connection"], "user", "password")
        MetaCatFilter.__init__(self, show_config)
        self.Connection = self.Config["connection"]
        self.ConnPool = ConnectionPool(postgres=self.Connection, max_idle_connections=1)
        self.TableName = self.Config["table"]
        self.IncludeColumns = self.Config["columns"]
        self.MetaPrefix = self.Config.get("meta_prefix", "runs_history")
        
    def hide(self, conn, *fields):
        for f in fields:
             conn = re.sub(f"\s+{f}\s*=\s*\S+", f" {f}=(hidden)", conn, re.I)
        return conn
    
    def filter(self, inputs, daqinterface_commit=None, mode=None, **ignore):
        db = self.ConnPool.connect()
        cursor = db.cursor()

        assert len(inputs) == 1

        colnames = ("," + ",".join(self.IncludeColumns)) if self.IncludeColumns else ""

        for chunk in inputs[0].chunked():
            by_run = {}
            for f in chunk:
                if "core.runs" in f.Metadata:
                    for runnum in f.Metadata["core.runs"]:
                        by_run.setdefault(runnum,[]).append(f)
            run_nums = list(by_run.keys())
            cursor.execute(f"""
                select runnum {colnames}
                    from {self.TableName}
                    where runnum = any(%s)
                        and (%s is null or daqinterface_commit=%s)
                        and (%s is null or mode=%s)
            """, (run_nums, daqinterface_commit, daqinterface_commit, mode, mode))
            tup = cursor.fetchone()
            while tup:
                runnum, rest = tup[0], tup[1:]
                for f in by_run[runnum]:
                    for column, value in zip(self.IncludeColumns, rest):
                        f.Metadata[f"{self.MetaPrefix}.{column}"] = value
                    yield f
                tup = cursor.fetchone()

def create_filters(config):
    return {
        "dune_runsdb":      RunsDB(config)
    }
