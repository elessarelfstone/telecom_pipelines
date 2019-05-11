import json
import os

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
        super(Downloader, self).__init__(conf)
        if handler:
            self.action = handler()

    def download(self):
        if self.action:
            path = self.path(self.conf)
            return self.action.download(self, path)

    @staticmethod
    def path(conf):
        action = HandlersFactory.get_handler(Downloader.handler_name(conf))
        return action().path(conf)

    @staticmethod
    def handler_name(conf):
        name = "download_" + Box(json.loads(conf)).storage.location_type
        return name


class Extractor(Source):

    def __init__(self, conf, file, directory, handler=None):
        self.action = None
        self.file = file
        self.directory = directory
        super(Extractor, self).__init__(conf)
        if handler:
            self.action = handler()

    def extract(self):
        if self.action:
            path = self.path(self.conf, self.directory)
            return self.action.extract(self, path)

    @staticmethod
    def path(conf, directory):
        action = HandlersFactory.get_handler(Extractor.handler_name(conf))
        return action().path(conf, directory)

    @staticmethod
    def handler_name(conf):
        suffix = ''
        name = "extract_" + Box(json.loads(conf)).storage.entity
        if 'list' in Box(json.loads(conf)).storage.location_type:
            suffix = 's'
        return name + suffix
