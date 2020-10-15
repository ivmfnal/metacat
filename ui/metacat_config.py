import yaml

class MetaCatConfig(object):

    def __init__(self, cfg_file):
        if isinstance(cfg_file, str):
            cfg_file = open(cfg_file, "r")
            
        self.Config = yaml.load(cfg_file, Loader=yaml.SafeLoader)
    
    def __getitem__(self, name):
        return self.Config[name]
