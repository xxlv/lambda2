# -*- coding: utf-8 -*-
import hashlib


class Lambda2Hash():
    def __init__(self):
        pass

    @staticmethod
    def make_event_hash(server, name):
        server = str(server)
        name = str(name)
        return "LAMBDA2_FUNC_%s@%s" % (str(hashlib.md5((server + name).encode("utf-8")).hexdigest()), name)
