import json, requests, time, psycopg2
from pythreader import TaskQueue
from metacat.db import DBUser, DBNamespace, DBDataset, DBFile
from metacat.logs import Logged, init as init_logs
from wsdbtools import ConnectionWithTransactions

class MetaCatDaemon(Logged):
    
    def __init__(self, config):
        Logged.__init__(self, "MetaCatDaemon")

        ssl_config = config.get("ssl", {})
        self.CertFile = ssl_config.get("cert")
        self.KeyFile = ssl_config.get("key", self.CertFile)
        
        daemon_config = config["daemon"]
        self.FerryURL = daemon_config["ferry_url"]
        if self.FerryURL.lower().startswith("https:") and not (self.CertFile and self.KeyFile):
            raise ValueError("X.509 cert and key files are not in the conficuration")
        
        self.FerryUpdateInterval = daemon_config.get("ferry_update_interval", 1*3600)
        self.CountsUpdateInterval = daemon_config.get("counts_update_interval", 1*3600)
        self.VO = daemon_config["vo"]

        db_config = config["database"]
        self.DBConnect = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s" % db_config
        if "password" in db_config:
            self.DBConnect += " password=%(password)s" % db_config
        self.Schema = db_config.get("schema")
        self.ConnPool = 

        self.Queue = TaskQueue(5, delegate=self)
        self.Queue.append(self.ferry_update, interval=self.FerryUpdateInterval, after=time.time())
        self.Queue.append(self.update_dataset_file_counts, interval=self.CountsUpdateInterval, after=0) #self.CountsUpdateInterval//3)
        self.Queue.append(self.update_namespace_file_counts, interval=self.CountsUpdateInterval, after=0) #2*self.CountsUpdateInterval//3)
        self.debug("tasks enqueued")
        
    def db(self):
        db = psycopg2.connect(self.DBConnect)
        if self.Schema:
            db.cursor().execute(f"set search_path to {self.Schema}")
        return ConnectionWithTransactions(db)
        
    def update_dataset_file_counts(self):
        db = self.db()
        counts = DBDataset.file_count_by_dataset(db)
        for ds in DBDataset.list(db):
            ds.FileCount = counts.get((ds.Namespace, ds.Name), 0)
            ds.save()
        db.close()
        self.log("Dataset file counts updated")

    def update_namespace_file_counts(self):
        db = self.db()
        counts = DBFile.file_count_by_namespace(db)
        for ns in DBNamespace.list(db):
            ns.FileCount = counts.get(ns.Name, 0)
            ns.save()
        db.close()
        self.log("Namespace file counts updated")

    def ferry_update(self):
        self.debug("ferry_update...")
        url = f"{self.FerryURL}/getAffiliationMembersRoles?unitname={self.VO}"

        self.debug("ferry URL:", url)
        response = requests.get(url, verify=False, cert=(self.CertFile, self.KeyFile))
        data = response.json()
        self.debug("data received")

        status = data["ferry_status"]
        if status != "success":
            print("\nFerry error:")
            for line in data["ferry_error"]:
                print(line)
            sys.exit(1)

        ferry_users = {item["username"]: item for item in data["ferry_output"][self.VO]}
        self.log("Loaded", len(ferry_users), "users from Ferry")

        db = self.db()
        db_users = {u.Username: u for u in DBUser.list(db)}
        
        ncreated = nupdated = 0
        updated = []
        created = []
        for username, ferry_user in ferry_users.items():
            db_user = db_users.get(username)
            if db_user is None:
                new_user = DBUser(db, username, ferry_user.get("fullname", ""), None, "", None, ferry_user.get("uuid"))
                new_user.save()
                ncreated += ncreated
                created.append(username)
            else:
                uuid = ferry_user.get("uuid")
                name = ferry_user.get("fullname")
                do_update = False
                if uuid and uuid != db_user.AUID:
                    db_user.AUID = uuid
                    do_update = True
                if name and name != db_user.Name:
                    db_user.Name = name
                    do_update = True
                if do_update:
                    db_user.save()
                    nupdated += 1
                    updated.append(username)

        self.log("created:", len(created), "" if not created else ",".join(created))
        self.log("updated:", len(updated), "" if not updated else ",".join(updated))
        db.close()

Usage = """
daemon.py -c <config.yaml> [-d] [-l <log path>]
"""
        
def main():
    import sys, getopt, yaml, time

    opts, args = getopt.getopt(sys.argv[1:], "l:c:dh?", ["help"])
    opts = dict(opts)
    
    if "-c" not in opts or "-?" in opts or "-h" in opts or "--help" in opts:
        print(Usage)
        sys.exit(2)
    
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    log_file = opts.get("-l", "-")
    init_logs(log_file, error_out=log_file, debug_out=log_file, debug_enabled="-d" in opts)
    
    daemon = MetaCatDaemon(config)
    while True:
        time.sleep(10)
        
if __name__ == "__main__":
    main()
