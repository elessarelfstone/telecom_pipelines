import json

from box import Box


class Source:
    def __init__(self, srconf):
        self.srconf = srconf


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
    def __init__(self, srconf, handler=None):
        self.action = None
        super(Downloader, self).__init__(srconf)
        if handler:
            self.action = handler()

    def download(self):
        if self.action:
            path = self.path(self.srconf)
            return self.action.download(self, path)

    @staticmethod
    def path(srconf):
        action = HandlersFactory.get_handler(Downloader.handler_name(srconf))
        return action().path(srconf)

    @staticmethod
    def handler_name(srconf):
        name = "download_" + Box(json.loads(srconf)).storage.location_type
        return name


class Extractor(Source):

    def __init__(self, srconf, fpath, dpath, handler=None):
        self.action = None
        self.fpath = fpath
        self.dpath = dpath
        super(Extractor, self).__init__(srconf)
        if handler:
            self.action = handler()

    def extract(self):
        if self.action:
            path = self.path(self.srconf, self.dpath)
            return self.action.extract(self, path)

    @staticmethod
    def path(srconf, dpath):
        action = HandlersFactory.get_handler(Extractor.handler_name(srconf))
        return action().path(srconf, dpath)

    @staticmethod
    def handler_name(srconf):
        suffix = ''
        name = "extract_" + Box(json.loads(srconf)).storage.entity
        if 'list' in Box(json.loads(srconf)).storage.location_type:
            suffix = 's'
        return name + suffix


class XLSParser(Source):
    def __init__(self, srconf, jobconf, xlspath, dpath, handler=None):
        self.action = None
        self.jobconf = jobconf
        self.xlspath = xlspath
        self.dpath = dpath
        super(XLSParser, self).__init__(srconf)
        if handler:
            self.action = handler()

    def parse(self):
        if self.action:
            fpath = self.path(self.srconf, self.jobconf, self.dpath)
            return self.action.parse(self, fpath)

    @staticmethod
    def path(srconf, jobconf, dpath):
        action = HandlersFactory.get_handler(XLSParser.handler_name(jobconf))
        return action().path(srconf, jobconf, dpath)

    @staticmethod
    def handler_name(jobconf):
        name = "xlsparse_to_" + Box(json.loads(jobconf)).data_format
        return name


class Parser(Source):
    def __init__(self, srconf, jobconf, dpath, handler=None):
        self.action = None
        self.jobconf = jobconf
        self.dpath = dpath
        super(Parser, self).__init__(srconf)
        if handler:
            self.action = handler()

    def parse(self):
        if self.action:
            fpath = self.path(self.srconf, self.jobconf, self.dpath)
            return self.action.parse(self, fpath)

    @staticmethod
    def path(srconf, jobconf, dpath):
        action = HandlersFactory.get_handler(Parser.handler_name(srconf, jobconf))
        return action().path(srconf, jobconf, dpath)

    @staticmethod
    def handler_name(srconf, jobconf):
        web = Box(json.loads(srconf)).storage.store
        location_type = Box(json.loads(srconf)).storage.location_type
        entity = Box(json.loads(srconf)).storage.entity
        type = Box(json.loads(srconf)).storage.type
        data_format = Box(json.loads(jobconf)).data_format
        name = f"{web}_{location_type}_{entity}_{type}_parse_to_{data_format}"
        return name




