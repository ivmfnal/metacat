import traceback, sys, time
from datetime import datetime
from .log_file import LogFile, LogStream, log_writer

DefaultLogger = None

class LogChannel(object):
    
    def __init__(self, output, label=None, enabled=True, timestamps=True):
        assert output is not None
        self.Timestamps = timestamps
        self.Writer = log_writer(output)
        self.Label = label
        self.Enabled = enabled
        
    def enable(self, enabled=True):
        self.Enabled = enabled

    def log(self, who, *message, sep=" ", t=None, label=None):
        #print("LogChannel.log(): who:", who)
        if self.Enabled:
            message = sep.join([str(p) for p in message])
            label = label or self.Label
            if label is not None:
                message = f"[{label}] {message}"
            if who:
                message = f"{who}: {message}"
            if not self.Timestamps: t = False
            self.Writer.log(message, t=t)

class AbstractLogger(object):

    def log(self, *message, sep=" ", who=None, t=None, channel="log"):
        raise NotImplementedError()


class Logger(AbstractLogger):

    def __init__(self, log_path, error_path=None, debug_path=None, debug=True, append=True):
        self.Debug = debug
        writer = log_writer(log_path, append=append)
        
        # default channels
        self.Channels = {       
            "log":      LogChannel(writer),
            "error":    LogChannel(writer if error_path is None else log_writer(error_path, append=append), label="ERROR")
        }
        if debug:
            self.Channels["debug"] = LogChannel(writer if debug_path is None else log_writer(debug_path, append=append), label="DEBUG")

    def add_channel(self, name, path=None, print_label=False, timestamps=True, **params):
        if path:    
            channel = LogChannel(log_out if path is None else log_writer(path, **params), 
                label = name if print_label else None,
                timestamps = timestamps
                )
        else:
            channel = self.Channels["log"]
        self.Channels[name] = channel

    def log(self, *message, sep=" ", who=None, t=None, channel="log"):
        #print("Logger.log(", message, sep, who, t, channel, ")")
        assert who is not None, "Message originator (who) must be specified"
        channel = self.Channels.get(channel)
        if channel is not None:
            channel.log(who, *message, sep=sep, t=t)

    def error(self, *message, sep=" ", who=None, t=None):
        assert who is not None, "Message originator (who) must be specified"
        self.log(*message, channel="error", sep=sep, who=who, t=t)

    def debug(self, *message, sep=" ", who=None, t=None):
        assert who is not None, "Message originator (who) must be specified"
        if self.Debug:
            self.log(*message, channel="debug", sep=sep, who=who, t=t)

class Logged(AbstractLogger):

    def __init__(self, name=None, debug=True, logger=None,
            log_channel="log", error_channel="error", debug_channel="debug"):
        assert logger is None or isinstance(logger, AbstractLogger), "logger must be either None or a Logger or a Logged"
        self.Logger = logger
        self.LogName = name or self.__class__.__name__
        self.Debug = debug
        self.LogChannel = log_channel
        self.ErrorChannel = error_channel
        self.DebugChannel = debug_channel

    def log(self, *message, sep=" ", who=None, t=None, channel=None):
        #print("Logged.log(", message, sep, who, t, channel, ")")
        channel = channel or self.LogChannel
        who = who or self.LogName
        logger = self.Logger or DefaultLogger
        if logger is not None and (channel != self.DebugChannel or self.Debug):
           logger.log(*message, sep=sep, who=who, t=t, channel=channel)
    
    def error(self, *message, sep=" ", who=None, t=None):
        self.log(*message, sep=sep, who=who or self.LogName, t=t, channel=self.ErrorChannel)

    def debug(self, *message, sep=" ", who=None, t=None):
        self.log(*message, sep=sep, who=who or self.LogName, t=t, channel=self.DebugChannel)


def init(log_output, error_out=None, debug_out=None, debug_enabled=False):
    global DefaultLogger
    DefaultLogger = Logger(log_output, error_out, debug_out, debug_enabled)
    return DefaultLogger
    
