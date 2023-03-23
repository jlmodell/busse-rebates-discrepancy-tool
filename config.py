import os

import yaml


class Config:
    def __init__(self):
        self.config = self.load_config()
        self.keys = self.load_keys()

    def load_config(self):
        with open(os.path.join(os.getcwd(), "config.yaml")) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def load_keys(self):
        with open(os.path.join(os.getcwd(), "config.yaml")) as f:
            return list(yaml.load(f, Loader=yaml.FullLoader).keys())

    def get(self, key):
        value = self.config.get(key, None)
        if value is None:
            raise KeyError(f"Key '{key}' not found in config.yaml")
        return value

    def list(self):
        if len(self.keys) == 0:
            self.keys = self.load_keys()
        return self.keys


c = Config()
