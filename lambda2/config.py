# -* coding: utf-8 -*-

import yaml


class Config():
    @staticmethod
    def load_config():
        return yaml.load(open('./conf/server.yml'))
