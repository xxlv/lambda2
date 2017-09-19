# -*- coding: utf-8 -*-

import socket
import json
from lambda2 import lambda_server_adapter
from lambda2 import lambda2_hash


class ScoketAdapter(lambda_server_adapter.LambdaServerAdapter):
    # Http适配器 可以是tcp适配器？ 多进程适配器等
    def make_server(self, server):

        if server is not None and "ip" in server and "port" in server:
            try:
                address = (server['ip'], int(server['port']))
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(address)
            except ConnectionRefusedError:

                s = None

        else:
            s = None

        return s

    def send(self, server, func_id, func_body):

        # make server
        s = self.make_server(server)
        if s is not None:
            event_id = lambda2_hash.Lambda2Hash.make_event_hash(server, func_id)
            data = {
                "event_id": event_id,
                "func_id": func_id,
                "func_body": func_body
            }

            s.send(str.encode(json.dumps(data)))
        else:
            event_id = None

        return event_id
