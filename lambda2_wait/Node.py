import hashlib
import redis
import socket
import json

# Redis = redis.StrictRedis(**{'host': '127.0.0.1', 'password': 'helloworld', 'port': '6379', 'db': 0})
#
# # 节点端
# class Node():
#     def __init__(self):
#         # TODO load from config
#         self.driver = Redis
#
#     def accept(self):
#         pass
#
#     def publish(self, event, data):
#         self.driver.publish(event, data)
#
