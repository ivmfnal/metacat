import json, requests
from pythreader import TaskQueue
from metacat.db import DBUser
from metacat.logs import Logged, init as init_logs

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
        
        self.FerryUpdateInterval = daemon_config.get("ferry_update_interval", 3600)
        self.VO = daemon_config["vo"]

        db_config = config["database"]
        self.DBConnect = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s" % db_config
        if "password" in db_config:
            self.DBConnect += " password=%(password)s" % db_config

        self.Queue = TaskQueue(5, delegate=self)
        self.Queue.append(self.ferry_update, interval=self.FerryUpdateInterval)
        
    def ferry_update(self):
        url = f"{self.FerryURL}/getAffiliationMembersRoles?unitname={self.VO}"
    
        response = requests.get(url, verify=False, cert=(self.CertFile, self.KeyFile))
        data = response.json()

        status = data["ferry_status"]
        if status != "success":
            print("\nFerry error:")
            for line in data["ferry_error"]:
                print(line)
            sys.exit(1)

        ferry_users = {item["username"]: item for item in data["ferry_output"][self.VO]}
        self.log("Loaded", len(ferry_users), "users from Ferry")

        db = psycopg2.connect(self.DBConnect)
        db_users = {u.Username: u for u in DBUser.list(db)}
        
        ncreated = nupdated = 0
        updated = []
        created = []
        for username, ferry_user in ferry_users:
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

Usage = """
daemon.py -c <config.yaml> [-l <log path>]
"""
        
def main():
    import sys, getopt, yaml, time

    opts, args = getopt.getopt(sys.argv[1:], "l:c:")
    opts = dict(opts)
    
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    log_file = opts.get("-l", "-")
    init_logs(log_file, error_out=log_file)
    
    daemon = MetaCatDaemon(config)
    while True:
        time.sleep(10)
        
if __name__ == "__main__":
    main()