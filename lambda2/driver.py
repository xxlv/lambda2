# -*- coding: utf-8 -*-
import redis
from lambda2 import config as myConfig

class Driver():
    @staticmethod
    def get_driver():
        config = myConfig.Config.load_config()
        Redis = redis.StrictRedis(
            **{'host': config['host'], 'password': config['password'], 'port': config['port'], 'db': config['db']})
        return Redis

