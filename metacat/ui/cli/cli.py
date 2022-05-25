import getopt, textwrap, sys

class UnknownCommand(Exception):
    def __init__(self, command, argv):
        self.Command = command
        self.Argv = argv
        
    def __str__(self):
        return f"Uknown command: {self.Command}\n" + \
            f"    command line: {self.Argv}"

class EmptyCommandLine(Exception):
    pass
    
class InvalidArguments(Exception):
    pass
    
class InvalidOptions(Exception):
    pass
    
def format_paragraph(indent, text):
    if "\n" in text:
        first_line, rest = text.split("\n", 1)
        return [first_line.strip()] + [indent + l for l in textwrap.dedent(rest.rstrip()).split("\n")]
    else:
        return [text.strip()]
    
class CLIInterpreter(object):

    Opts = ("", [])
    Usage = ""
    Usage0 = ""
    Defaults = {}
    MinArgs = 0
    Hidden = False

    def get_options(self):
        tup = self.Opts
        out = ("", [])
        if isinstance(tup, str):
            words = tup.split()
            if words:
                if words[0] == "--":
                    out = ("", words[1:])
                else:
                    out = (words[0], words[1:])
        elif isinstance(tup, list):
            out = ("", tup)
        else:
            assert isinstance(tup, tuple) and len(tup) == 2
            out = tup
        return out

    def make_opts_dict(self, opts):
        opts_dict = {}
        for opt, val in opts:
            existing = opts_dict.get(opt)
            if existing is None:
                opts_dict[opt] = val
            elif isinstance(existing, list):
                existing.append(val)
            else:
                opts_dict[opt] = [existing, val]
        out = self.Defaults.copy()
        out.update(opts_dict)
        return out

    def getopt(self, argv):
        short_opts, long_opts = self.get_options()
        try:
            opts, args = getopt.getopt(argv, short_opts, long_opts)
        except getopt.GetoptError:
            raise InvalidOptions()
        if len(args) < self.MinArgs:
            raise InvalidArguments()
        return self.make_opts_dict(opts), args
    
    # overridable
    def _run(self, command, context, argv, usage_on_empty = True, usage_on_unknown = True):
        return None
        
    
class CLICommand(CLIInterpreter):

    def _run(self, command, context, argv, usage_on_error = True):

        if argv and argv[0] == "-?":
            print(self.usage(command), file=sys.stderr)
            return
            
        if argv and argv[0] in ("help", "--help"):
            print(self.help(command), file=sys.stderr)
            return

        try:
            opts, args = self.getopt(argv)
            return self(command, context, opts, args)
        except (InvalidOptions, InvalidArguments):
            if usage_on_error:
                cmd = "" if not command else f"for {command}"
                print(f"Invalid arguments or options for {cmd}\n", file=sys.stderr)
                print(self.help(command), file=sys.stderr)
                return
            else:
                raise

    def help(self, command="", indent=""):
        try:
            usage = self.Usage
        except AttributeError:
            usage = ""
            
        if command: command = command + " "
        return indent + command + "\n".join(format_paragraph(indent + "  ", usage))

    def usage(self, word=""):
        usage = (self.Usage0 or self.Usage.split("\n", 1)[0]).strip()
        if usage.startswith(word + " "):
            usage = usage[len(word)+1:]
        return usage

class CLI(CLIInterpreter):
    
    def __init__(self, *args, hidden=False, usage="", opts=""):
        self.Hidden = hidden
        self.UsageParagraph = usage
        self.Opts = opts
        self.Words = []          
        self.Interpreters = {}

        i = 0
        while i < len(args):
            w, c = args[i], args[i+1]
            self.Words.append(w)
            self.Interpreters[w] = c
            i += 2
        
    def commands(self):
        return self.Words
            
    # overridable
    def update_context(self, context, opts, args):
        return context
    
    def _run(self, pre_command, context, argv, usage_on_error = True):
        
        #print(self,"._run: pre_command:", pre_command)

        if argv and argv[0] == "-?":
            print(self.usage(pre_command), file=sys.stderr)
            return
            
        if argv and argv[0] in ("help", "--help"):
            print(self.help(pre_command), file=sys.stderr)
            return

        try:
            opts, args = self.getopt(argv)
        except (InvalidOptions, InvalidArguments):
            if usage_on_error:
                cmd = "" if not pre_command else f"for {pre_command}"
                print(f"Invalid arguments or options {cmd}\n", file=sys.stderr)
                print(self.usage(pre_command), file=sys.stderr)
                return
            else:
                raise

        if not args:
            if usage_on_error:
                print(self.usage(pre_command), file=sys.stderr)
                return
            else:
                raise EmptyCommandLine()
            

        #print(f"{self.__class__.__name__}._run(): argv:", argv, "  args:", args)

        context = self.update_context(context, opts, args)
        word, rest = args[0], args[1:]
        
        if word in ("help", "--help"):
            print(self.help(word), file=sys.stderr)
            return
        
        interp = self.Interpreters.get(word)
        if interp is None:
            print(f"Unknown command {pre_command} {word}\n", file=sys.stderr)
            if usage_on_error:
                indent = "" if not pre_command else "  "
                print("Usage:" if not pre_command else f"Usage for {pre_command}:")
                print(self.usage(pre_command, indent=indent),
                      file=sys.stderr)
                return
            else:
                raise UnknownCommand(word, args)
        
        if pre_command: pre_command = pre_command + " "
        #print(interp, ".run(): pre:", pre_command, "   word:", word)
        return interp._run(pre_command + word, context, rest, usage_on_error = usage_on_error)
        
    def run(self, argv, context=None, usage_on_error = True, argv0=None):
        argv0 = argv0 or argv[0]
        command, argv = argv0, argv[1:]
        self._run(command, context, argv, usage_on_error)
        
    def format_usage_paragraph(self, indent=""):
        return "\n".join(format_paragraph(indent, self.UsageParagraph))
        
    def usage_headline(self):
        return self.UsageParagraph.split("\n", 1)[0].strip()

    def usage(self, pre_command="", as_list=False, long=True, end="", indent=""):

        out = []
        if pre_command:
            out.append(pre_command + " " + self.usage_headline())
            indent = "  " + indent

        maxcmd = max(len(w) for w in self.Interpreters.keys())
        maxcmd = max(maxcmd, 4)     # for "help"
        fmt = f"%-{maxcmd}s %s"

        for w in self.Words:
            interp = self.Interpreters[w]
            if not interp.Hidden:
                if isinstance(interp, CLI):
                    down_usage = ",".join(interp.Words)
                else:
                    down_usage = interp.usage()
                out.append(indent + (fmt % (w, down_usage)))
        out.append(indent + (fmt % ("help", "-- print help")))
        #print(self, f": usage:{out}")
        if as_list:
            return out
        else:
            return "\n".join(out) + end
            
    def help(self, pre_command="", indent=""):

        out = []
        if pre_command: pre_command = pre_command + " "
        formatted_usage = self.format_usage_paragraph(indent + "  ")
        if formatted_usage: 
            formatted_usage = formatted_usage + "\n"
        else:
            formatted_usage = "<command> [<command options, agruments> ...]\n"
        out.append(indent + pre_command + formatted_usage)
        out.append("Commands:")
        indent += "  "

        fmt = "%s %s"
        if self.Interpreters:
            maxcmd = max(len(w) for w in self.Interpreters.keys())
            maxcmd = max(maxcmd, 4)     # for "help"
            fmt = f"%-{maxcmd}s %s"

            for word in self.Words:
                interp = self.Interpreters[word]
                if not interp.Hidden:
                    if isinstance(interp, CLI):
                        out.append(indent + (fmt % (word, ",".join(interp.commands()))))
                    elif isinstance(interp, CLICommand):
                        # assume CLICommand subclass
                        #usage = interp.usage(" "*(maxcmd-len(word)), indent + " "*(maxcmd+1))
                        #usage = interp.usage("", indent + " "*(maxcmd+1))
                        cmd_usage = interp.usage(word)
                        out.append(indent + (fmt % (word, cmd_usage)))
                    else:
                        raise ValueError("Unrecognized type of the interpreter: %s %s" % (type(interp), interp))
        out.append(indent + (fmt % ("help", "-- print help")))
        #print(self, f": usage:{out}")
        return "\n".join(out)
        
    def print_usage(self, headline="Usage:", head_paragraph = "", file=None):
        if file is None: file = sys.stderr
        head_paragraph = textwrap.dedent(head_paragraph).strip()
        if headline:
            print(headline, file=file)
        if head_paragraph:
            print(head_paragraph, file=file)
        usage = self.usage(headline=None)
        print(self.usage(headline=None), file=file)
        
