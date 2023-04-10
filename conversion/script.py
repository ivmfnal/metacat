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

    def __init__(self, config, external_env):
        self.Title = config.get("title")
        Task.__init__(self, name=self.Title)
        self.Killed = False
        self.Env = external_env.copy()
        self.Env.update(self.parse_env(config))
        self.Status = None
        self.Level = level
    
    @staticmethod
    def from_config(config, external_env):
        if config.get("type") == "parallel":
            return ParallelGroup.from_config(config, external_env)
        elif config.get("command"):
            return Command.from_config(config, external_env)
        else:
            return SequentialGroup.from_config(config, external_env)
    
    def run(self):
        raise NotImplementedError()
        return status             # "ok" or "error" or "killed" or "cancelled"
        
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
        
    def parse_env(self, config):
        env = {}
        for name, value in config.get("env", {}).items():
            v = os.environ.get(name, "")
            while "$" + name in value:
                value = value.replace("$" + name, v) 
            env[name] = value
        return env
        
    def indent(self, text, extra_indent = ""):
        return indent(text, ("  " * self.Level) + extra_indent)
        
    def log(self, *parts, **kv):
        print("%s:" % (time.ctime(),), *parts, **kv)

class Command(BaseTask):
    
    def __init__(self, config, external_env={}):
        BaseTask.__init__(self, config, external_env)
        self.Command = config["command"]
        self.Process = None
        self.Out = None
        self.Err = None
        self.Retcode = None
        
    @classmethod
    def from_config(cls, config, external_env):
        cmd = cls(config, external_env)
        cmd.Title = cmd.Title or cmd.Command

    def __str__(self):
        process = self.Process
        pid = process.pid if process is not None else ""
        return f"Command {self.Title}"
        
    def run(self):
        log("STARTED: command", self.Title)
        self.Process = SubprocessAsync(self.Command, shell=True, env=env, process_group=0).start()
        out, err = self.Process.wait()
        self.Out = out
        self.Err = err
        self.Retcode = self.Process.returncode
        status = "ok"
        if self.is_killed:
            status = "killed"
        elif self.Retcode:
            status = "error"
        self.Status = status

        log("ENDED: command", self.Title)
        print("  Status:", self.Status)
        print("  Elapsed time:", self.pretty_time(command.Ended - command.Started))
        if self.Exception:
            print(indent + "  Exception:")
            for line in traceback.format_exception(*self.Exception):
                print(indent + "  " + line)
        out = out.strip()
        err = err.strip()
        if out:
            print("")
            print("  -- stdout: ------")
            print(indent(out, "  "))
            print("  ------------------")
        if err:
            print("")
            print("  -- stderr: -------")
            print(indent(err, "  "))
            print("  ------------------")

        return self.Status
        
    def print_status(self, add_timestamp=True, indent=""):
        headline = time.ctime(time.time())+': ' if add_timestamp else "") + self.Title
        print(indent + headline)
        print(indent + "  Status:", self.Status)
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
            log("KILLING: command", self.Title)
            self.Process.killpg()
            self.killed()

class ParallelGroup(BaseTask):
    
    def __init__(self, config, external_env={}, tasks=[]):
        BaseTask.__init__(self, config, external_env)
        self.Queue = TaskQueue(config.get("multiplicity", 5), delegate=self)
        self.Steps = tasks
        
    @classmethod
    def from_config(cls, config, external_env={}):
        group = cls(config, external_env)
        steps = [BaseTask.from_config(cfg, external_env=group.Env) for cfg in config.get("steps", [])]
        group.Steps = steps

    @synchronized
    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        log(f"EXCEPTION in {task.Title}:")
        traceback.print_exc(exc_type, exc_value, tb)
        print("")
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
    def print_status(self, command, retcode, out, err):
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
        log("STOPPING: parallel group", self.Title)
        self.Queue.hold()
        for task in self.Queue.waitingTasks():
            print("  Cancelling:", task.Title)
            self.Queue.cancel(task)
        for task in self.Queue.activeTasks():
            #print("active task:", task)
            if not task.Killed:
                print("  Killing:", task)
                task.kill()
        self.killed()

    def run(self):
        log("STARTED: parallel group", self.Title)
        t0 = time.time()
        for task in self.Tasks:
            self.Queue.append(task)
        self.Queue.join()
        t1 = time.time()
        log("ENDED parallel group:", self.Title)
        print("  Status:", self.Status)
        print("  Elapsed time:", self.pretty_time(t1 - t0))
        print("\n")
        return self.Status

class SequentialGroup(BaseTask):
    
    def __init__(self, config, external_env, steps = []):
        BaseTask.__init__(self, config, env)
        self.Steps = tasks

    @classmethod
    def from_config(cls, config, external_env={}):
        group = cls(config, external_env)
        steps = [BaseTask.from_config(cfg, external_env=group.Env) for cfg in config.get("steps", [])]
        group.Steps = steps

    def run(self):
        log("STARTED: sequential group", self.Title)
        t0 = time.time()
        for task in tasks:
            if self.Status is None:
                status = task.run()
                if status != "ok":
                    self.Status = "killed"
            else:
                task.cancel()
        if self.Status is None:
            self.Status = "ok"
        log("ENDED parallel group:", self.Title)
        print("  Status:", self.Status)
        print("  Elapsed time:", self.pretty_time(t1 - t0))
        print("\n")


class Script(BaseTask):

    def __init__(self, config):
        self.Env = self.parse_env(config)
        
        self.Steps = [Step(step, env=self.Env) for step in config["script"]]

    def run(self):
        t0 = time.time()
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
