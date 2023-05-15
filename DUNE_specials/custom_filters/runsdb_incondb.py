from metacat.filters import MetaCatFilter
from wsdbtools import ConnectionPool
import re
from condb import ConDB

class RunsDBinConDB(MetaCatFilter):
    """
    Inputs:

    Positional parameters: none

    Keyword parameters:

    Description:

    Configuration:
        host =
        port =
        dbname =
        user =
        password =
    """

    def __init__ (self, config):
        self.Config = config
        show_config = config.copy()
        show_config["connection"] = self.hide(show_config["connection"], "user", "password")
        MetaCatFilter.__init__(self, show_config)
        self.Connection = self.Config["connection"]
        self.ConnPool = ConnectionPool(postgres=self.Connection, max_idle_connections=1)
        self.FolderName = self.Config["folder"]
        self.MetaPrefix = self.Config.get("meta_prefix", "runs_history")
        
        #
        # get column names
        #
        
        db = ConDB(self.ConnPool)
        folder = db.openFolder(self.FolderName)
        folder_columns = folder.data_column_types()
        self.ColumnTypes = folder.data_column_types()

    def hide(self, conn, *fields):
        for f in fields:
             conn = re.sub(f"\s+{f}\s*=\s*\S+", f" {f}=(hidden)", conn, re.I)
        return conn
        
    def file_run_number(self, metadata):
        file_runs = metadata.get("core.runs")
        if file_runs:
            return file_runs[0]
        else:
            return None

    def filter(self, inputs, **ignore):
        
        # Conect to db via condb python API
        db = ConDB(self.ConnPool)
        folder = db.openFolder(self.FolderName)
        
        data_by_run = {}        # cache data by run number across chunks

        # Get files from metacat input
        file_set = inputs[0]
        for chunk in file_set.chunked():
            need_run_nums = set()

            for f in chunk:
                runnum = self.file_run_number(f.Metadata)
                if runnum is not None and runnum not in data_by_run:
                    need_run_nums.add(runnum)

            if need_run_nums:
                # Get run_hist data
                data_runhist = folder.getData(0, channel_range=(min(need_run_nums), max(need_run_nums)+1))
                for row in data_runhist:
                    runnum, data = row[0], row[4:]
                    if runnum not in data_by_run:
                        data_by_run[runnum] = data
        
            # Insert run hist data to Metacat
            for f in chunk:
                runnum = self.file_run_number(f.Metadata)
                if runnum is not None and runnum in data_by_run:
                    for (col, typ), value in zip(self.ColumnTypes, data_by_run[runnum]):
                        if typ.startswith("timestamp") and value is not None:
                            value = value.timestamp()
                        f.Metadata[f"{self.MetaPrefix}.{col}"] = value

            yield from chunk
 

def create_filters(config):
    return {
        "dune_runshistdb": RunsDBinConDB(config)
    }
