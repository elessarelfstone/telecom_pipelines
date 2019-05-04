import os
import json



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



