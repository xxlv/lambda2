# -*- coding: utf-8 -*-


# 结果集
class ResultPool():
    def __init__(self):
        self.result_pool = {}

    # def get(self, id):
    #     if id in self.result_pool:
    #         return self.result_pool[id]
    #     else:
    #         return None

    def update(self, channel_func_id, result):
        self.result_pool[channel_func_id] = result
