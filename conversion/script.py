import getopt, yaml, sys, traceback, os, signal, time
from textwrap import indent
from pythreader import SubprocessAsync, Task, Primitive, synchronized, TaskQueue

if sys.version_info[:2] < (3,11):
    print("Pytbon version 3.11 or later is required", file=sys.stderr)
    sys.exit(1)

Usage = """
python convery.py <script.yaml>
"""

LogLock = Primitive()

def log(*parts, **kv):
    with LogLock:
        print("%s:" % (time.ctime(),), *parts, **kv)
        
class BaseTask(Task):

    def __init__(self, config, env):
        self.Title = config.get("title")
        Task.__init__(self, name=self.Title)
        self.Killed = False
        self.Env = env.copy()
        self.Env.update(config.get("env", {}))
        self.Exception = None
    
    def run(self):
        raise NotImplementedError()
        return True             # success/failure
        
    def kill(self):
        raise NotImplementedError()
        
    def killed(self):
        self.Killed = True
        
    def exception(self, exc_type, exc_value, tb):
        self.Exception = (exc_type, exc_value, tb)
        
    def print_status(self, indent=""):
        raise NotImplementedError()

    @property
    def is_killed(self):
        return self.Killed

class Command(BaseTask, QTask):
    
    def __init__(self, config, env={}):
        Task.__init__(self)
        Group.__init__(self, config, env)
        self.Command = config["command"]
        self.Process = None
        self.Out = None
        self.Err = None
        self.Retcode = None

    def __str__(self):
        process = self.Process
        pid = process.pid if process is not None else ""
        return f"Task {self.Title} ({pid}: {self.Command})"
        
    def run(self):
        retcode = None
        try:
            log(f"Starting task {self.Title} ...")
            env = os.environ.copy()
            env.update(self.Env)
            self.Process = SubprocessAsync(self.Command, shell=True, env=env, process_group=0).start()
            out, err = self.Process.wait()
            self.Out = out
            self.Err = err
            self.Retcode = self.Process.returncode
            self.Process = None
            if self.is_killed: self.Retcode = "killed"
        except:
            self.Retcode = "exception"
            self.Exception = sys.exc_info()
        return not retcode and not self.Exception and not self.Killed
        
    def print_status(self, add_timestamp=True, indent=""):
        headline = time.ctime(time.time())+': ' if add_timestamp else "") + self.Title
        print(indent + headline)
        status = "succeeded" if retcode == 0 else f"failed with exit code {self.Retcode}"
        print(indent + "  Status:", status)
        print(indent + "  Elapsed time:", self.pretty_time(command.Ended - command.Started))
        if self.Exception:
            print(indent + "  Exception:")
            for line in traceback.format_exception(*self.Exception):
                print(indent + "  " + line)
        out = out.strip()
        err = err.strip()
        if out:
            print("")
            print(indent + "  -- stdout: ------")
            print(indent(out, indent + "  "))
            print(indent + "  ------------------")
        if err:
            print("")
            print(indent + "  -- stderr: -------")
            print(indent(err, indent + "  "))
            print(indent + "  ------------------")

    @synchronized
    def kill(self):
        if not self.Killed and self.Process is not None:
            self.Process.killpg()
            #self.Process.kill(signal.SIGHUP)
            self.killed()

class ParallelTask(Primitive, Group):
    
    def __init__(self, config, env={}):
        Primitive.__init__(self)
        Group.__init__(self, config)
        self.Env = env.copy()
        self.Env.update(config.get("env", {}))
        self.Queue = TaskQueue(config.get("multiplicity", 5), delegate=self)
        self.Groups = [Command(task, env=env) for task in config["groups"]]
        self.Failed = False
        
    @synchronized
    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        log(f"Task {task.Title} exception: ---")
        traceback.print_exc(exc_type, exc_value, tb)
        self.Failed = True
        self.shutdown()
    
    @synchronized
    def taskEnded(self, queue, command, result):
        retcode, out, err = result
        self.print_command_results(command, retcode, out, err)
        if retcode:
            self.Failed = True
            if retcode != "killed":
                self.shutdown()
    
    @staticmethod
    def pretty_time(t):
        fs = t - int(t/60)*60
        t = int(t)
        h = t//3600
        m = (t % 3600) // 60
        s = int(t % 60)
        if t > 3600:
            return(f"{h}h {m}m")
        elif t > 60:
            return(f"{m}m {s}s")
        else:
            return("%.2fs" % (fs,))
            
    @synchronized
    def print_command_results(self, command, retcode, out, err):
        status = "succeeded" if retcode == 0 else f"failed with exit code {retcode}"
        log("End of task", command.Title)
        print("  Status:", status)
        print("  Elapsed time:", self.pretty_time(command.Ended - command.Started))
        out = out.strip()
        err = err.strip()
        if out:
            print("\n  -- stdout: ------")
            print(indent(out, "  "))
            print("  ------------------")
        if err:
            print("\n  -- stderr: -------")
            print(indent(err, "  "))
            print("  ------------------")
        print("")

    @synchronized
    def shutdown(self):
        log("Shutting down parallel step:", self.Title)
        self.Queue.hold()
        for task in self.Queue.waitingTasks():
            print("Cancelling:", task.Title)
            self.Queue.cancel(task)
        for task in self.Queue.activeTasks():
            #print("active task:", task)
            if not task.Killed:
                print("Killing:", task)
                task.kill()

    def run(self):
        t0 = time.time()
        log(f"STEP {self.Title} ...")
        for command in self.Commands:
            self.Queue.append(command)
        self.Queue.join()
        t1 = time.time()
        log("End of STEP:", self.Title)
        print("  Status:", "failed" if self.Failed else "succeeded")
        print("  Elapsed time:", self.pretty_time(t1 - t0))
        print("\n")
        return not self.Failed

class Script(object):

    def __init__(self, config):
        self.Env = {}
        for name, value in config.get("env", {}).items():
            mode = ""
            if name.endswith("(append)"):
                name = name[:-len("(append)")]
                mode = "append"
            elif name.endswith("(prepend)"):
                name = name[:-len("(prepend)")]
                mode = "prepend"
            v = os.environ.get(name)
            if v and mode == "append":
                v = v + ":" + value
            elif v and mode == "prepend":
                v = value + ":" + v
            else:
                v = value
            self.Env[name] = v
        self.Steps = [Step(step, env=self.Env) for step in config["steps"]]

    def run(self):
        for step in self.Steps:
            if not step.run():
                return False
        else:
            return True

opts, args = getopt.getopt(sys.argv[1:], "h?", ["--help"])
opts = dict(opts)
if len(args) != 1 or "-?" in opts or "-h" in opts or "--help" in opts:
    print(Usage)
    sys.exit(2)
    
config = yaml.load(open(args[0], "r"), Loader=yaml.SafeLoader)
script = Script(config)
ok = script.run()
if ok:
    print("\nScript succeded")
    sys.exit(0)
else:
    print("\nScript failed")
    sys.exit(1)
