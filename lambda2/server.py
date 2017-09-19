# -* coding: utf-8 -*-

import socket
import json
import time
import random

from lambda2 import subscriber
from lambda2 import discover


class Server():
    def __init__(self, ip='127.0.0.1'):
        port = random.randrange(30000, 40000)
        address = (ip, port)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(address)
        s.listen(5)

        self.subscriber = subscriber.Subscriber()

        self.port = port
        self.ip = ip
        self.server = s

    def compute(data_o):
        event_id = data_o['event_id']
        func_body = data_o['func_body']
        func_id = data_o['func_id']
        # TODO
        # Check expression
        # Check cache
        func_body_str = "".join(func_body[0])
        exec(func_body_str)
        data = eval("{}()".format(func_id))

        return data

    def run(self):
        while True:
            print("当前绑定端口   " + str(self.port) + "  正在等待计算资源包...")

            discover.Discover.mount({"ip": "127.0.0.1", "port": self.port})
            ss, addr = self.server.accept()
            data = ss.recvmsg(10086)[0].decode('utf-8')
            data_o = json.loads(data)
            event_id = data_o['event_id']
            data = self.compute(data_o)

            print("计算完毕，正在发布 " + event_id)
            self.subscriber.publish(event_id, data)
            print("Publish {}".format(event_id))
            time.sleep(2)
