# -*- coding: utf-8 -*-

class FunctionPool():
    def __init__(self):
        self.function_pool = {}

    def push(self, func_id, func_body):
        self.function_pool[func_id] = func_body

    def load(self, func_id):
        return self.function_pool[func_id]
