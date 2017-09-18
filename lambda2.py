#!/usr/local/bin/python
import hashlib
import redis
import socket
import json

Redis = redis.StrictRedis(**{'host': '127.0.0.1', 'password': 'helloworld', 'port': '6379', 'db': 0})


# 节点端
class Node():
    def __init__(self):
        self.driver = Redis

    def accept(self):
        pass

    def publish(self, event, data):
        self.driver.publish(event, data)


# -------------------------------------------------------------------------------------------
# 抽象适配器
class LambdaServerAdapter():
    def send(self, server, name):
        pass


# Http适配器 可以是tcp适配器？ 多进程适配器等
class ScoketAdapter(LambdaServerAdapter):
    def send(self, server, func_id, func_body):
        address = ('127.0.0.1', 31503)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(address)
        event_id = Lambda2Hash.make_event_hash(server, func_id)
        data = {
            "event_id": event_id,
            "func_id": func_id,
            "func_body": func_body
        }

        s.send(str.encode(json.dumps(data)))
        return event_id

        # def _make_event_id(self,server,name):
        #     return "LAMBDA2-%s" % str(hashlib.md5((server+name).encode("utf-8")).hexdigest())


class Lambda2Hash():
    def __init__(self):
        pass

    @staticmethod
    def make_event_hash(server, name):
        server = str(server)
        name = str(name)
        return "LAMBDA2-%s@%s" % (str(hashlib.md5((server + name).encode("utf-8")).hexdigest()), name)


# API
class ContentLessApi():
    def __init__(self, api):
        self.api = api

    def send_to(self, server, func_id, func_body):
        return self.api.send(server, func_id, func_body)


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


# 函数
class FunctionPool():
    def __init__(self):
        self.function_pool = {}

    def push(self, func_id, func_body):
        self.function_pool[func_id] = func_body

    def load(self, func_id):
        return self.function_pool[func_id]


# Lambda2 Main
class Lambad2():
    def __init__(self):
        # 计算节点集群
        self.init()

    def init(self):

        self.driver = Redis
        # 计算适配器
        self.lambad_adapter_api = ContentLessApi(ScoketAdapter())
        # 计算节点池
        self.node_pool = []
        # 过程
        self.lambda_functions_pool = FunctionPool()
        # 聚合
        self.lambda_functions_handle = FunctionPool()
        # 结果
        self.result_pool = ResultPool()
        # 函数节点map
        self.function_node = {}
        # 函数事件map
        self.function_sub_map = {}

        self.subscribe = None

    def run(self):

        for func_id, func_body in self.lambda_functions_pool.function_pool.items():
            self.lambad_adapter_api.send_to(self.node_pool[self._hash(func_id)], func_id, func_body)

        return self._compute()


    def add_node(self, node):
        self.node_pool.append(node)

    def add_nodes(self, nodes):
        self.node_pool.extend(nodes)

    def remove_node(self, node):
        pass

    # 申明此次聚合计算的依赖
    def depend_on(self, *event):
        self._update_function_hash_sub_event_map(*event)
        return self._sub(*event)

    # 计算结果聚合处理
    def _compute(self):
        self._update_result_pool()
        return self._handle()


    # 聚合处理
    def _handle(self):
        handle_pools = self.lambda_functions_handle.function_pool

        for func_id, func_body in handle_pools.items():
            # TODO  (a="1",b="2")
            data = self._exec_local_function(func_id, "".join(func_body[0]), (self.result_pool.result_pool))
            return data

    # 本机运行函数,此次运算时简单的聚合，原子操作，本地计算即可，当然也可以配置一个server计算
    def _exec_local_function(self, func_id, func_body_str, params):

        params = self._recovery_to_local_func_map(params)
        # 重建函数
        exec(func_body_str)
        # 调用函数
        if params is not None:
            data = eval("{}({})".format(func_id, params))
        else:
            data = eval("{}()".format(func_id))
        return data

    def _recovery_to_local_func_map(self, result_map):

        new_map = {}
        for k, v in result_map.items():
            for k2, v2 in self.function_sub_map.items():
                if str(k.decode('utf-8')) == str(v2):
                    new_map[k2] = v

        return new_map


        # 更新计算结果池

    def _update_result_pool(self):

        event_map_size = len(self.function_sub_map)
        if self.subscribe is None:
            print("subscribe not found")
            return
        for m in self.subscribe.listen():
            if m['type'] == "message":
                self.result_pool.update(m['channel'], m['data'].decode('utf-8'))
                event_map_size = event_map_size - 1
                if event_map_size == 0:
                    break;

    # 更新函数的订阅事件map
    def _update_function_hash_sub_event_map(self, *events):
        for func_id in events:
            self.function_sub_map[func_id] = Lambda2Hash.make_event_hash(self.node_pool[self._hash(func_id)], func_id)

    # 订阅事件
    def _sub(self, *events):
        sub_events = []
        for event in events:
            sub_events.append(self.function_sub_map.get(event))
        sub = self.driver.pubsub()
        sub.subscribe(sub_events)
        self.subscribe = sub
        return self.subscribe

    #
    # 函数映射到计算节点
    def _hash(self, key):
        # 给定一个函数的identity,输出一个server id
        node_len = len(self.node_pool)
        if node_len == 0:
            node_len = 1

        md5_key = hashlib.md5(key.encode('utf-8')).hexdigest()
        key_sum = 1
        for c in md5_key:
            key_sum = key_sum + ord(c)

        mode = key_sum % node_len
        return mode
