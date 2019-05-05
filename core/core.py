import os
import json
from box import Box


class Source:
    def __init__(self, conf):
        self.conf = conf


class HandlersFactory:

    _handlers = {}

    @classmethod
    def get_handler(cls, name):
        try:
            return cls._handlers[name]
        except KeyError:
            raise ValueError(name)

    @classmethod
    def register(cls, name, handler):
        cls._handlers[name] = handler


class Downloader(Source):
    def __init__(self, conf, handler=None):
        self.action = None
        self.directory = os.getenv("TEMP_DIR")
        os.makedirs(self.directory, exist_ok=True)
        super(Downloader, self).__init__(conf)
        if handler:
            self.action = handler()

    def download(self):
        if self.action:
            return self.action.download(self, self.path(self.conf, self.directory))

    @staticmethod
    def path(conf, directory):
        action = HandlersFactory.get_handler(Downloader.handler_name(conf))
        return action().path(conf, directory)

    @staticmethod
    def handler_name(conf):
        name = "download_" + Box(json.loads(conf)).storage.location_type
        return name
