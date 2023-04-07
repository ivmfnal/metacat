import getopt, yaml, sys, traceback, os, signal
from pythreader import SubprocessAsync, Task, Primitive, synchronized, TaskQueue

Usage = """
python convery.py <script.yaml>
"""

class Command(Task):
    
    def __init__(self, config, env={}):
        Task.__init__(self)
        self.Title = config.get("title")
        self.Command = config["command"]
        self.Process = None
        self.Env = env
        self.Killed = False
        
    def __str__(self):
        return f"Task {self.Title} ({self.Command})"
        
    def run(self):
        env = os.environ.copy()
        env.update(self.Env)
        self.Process = SubprocessAsync(self.Command, shell=True, env=env).start()
        out, err = self.Process.wait()
        retcode = self.Process.returncode
        self.Process = None
        if self.Killed: retcode = "killed"
        return retcode, out, err

    @synchronized
    def kill(self):
        if not self.Killed:
            process.signal(signal.SIGHUP)
            self.Killed = True

class CommandTask(Task):
    
    def __init__(self, config):
        self.Title = config.get("title")
        self.Command = Command(config)
        
    def run(self):
        return self.Command.run()
        
    def kill(self):
        self.Command.kill()
        
class Step(Primitive):
    
    def __init__(self, config, env={}):
        Primitive.__init__(self)
        self.Env = env
        self.Title = config.get("title")
        if "command" in config:
            self.Commands = [Command(config, env=env)]
            self.Queue = None
        else:
            self.Queue = TaskQueue(config.get("multiplicity", 5), delegate=self)
            self.Commands = [Command(task, env=env) for task in config["tasks"]]
        self.Failed = False
        
    @synchronized
    def taskFailed(self, queue, command, exc_type, exc_value, tb):
        print(f"--- task {command.Title} exception: ---")
        traceback.print_exc(exc_type, exc_value, tb)
        self.Failed = True
        self.shutdown()
    
    @synchronized
    def taskEnded(self, queue, command, result):
        retcode, out, err = result
        self.print_command_results(command, retcode, out, err)
        if retcode:
            self.Failed = True
            self.shutdown()

    @synchronized
    def print_command_results(self, command, retcode, out, err):
        status = "succeeded" if retcode == 0 else f"failed with exit code {retcode}"
        if out or err:
            print(f"--- task {command.Title}:")
            if out:
                print(out)
            if err:
                print("--- stderr -------")
                print(err)
            print(f"--- end of task {command.Title}: {status}")
        else:
            print(f"--- task {command.Title}: {status}")
        print("")

    @synchronized
    def shutdown(self):
        print("\nShutting down parallel step:", self.Title)
        self.Queue.hold()
        for task in self.Queue.waitingTasks():
            print("Cancelling:", task.Title)
            self.Queue.cancel(task)
        for task in self.Queue.activeTasks():
            print("active task:", task)
            if not task.Killed:
                print("Killing:", task.Title)
                task.kill()

    def run(self):
        print(f"====== STEP {self.Title} ...")
        if len(self.Commands) == 1:
            command = self.Commands[0]
            retcode, out, err = command.run()
            self.print_command_results(command, retcode, out, err)
            self.Failed = retcode != 0
        else:
            for command in self.Commands:
                self.Queue.append(command)
            self.Queue.join()
        print(f"====== end of STEP {self.Title}: %s" % ("failed" if self.Failed else "succeeded",))
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
