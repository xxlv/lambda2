#!/usr/local/bin/python
import hashlib
import redis
import socket
import json
import yaml


class Discover():
    @staticmethod
    def get_id():
        return "LAMBAD2_DISCOVER_EVENT"

    @staticmethod
    def mount(server):
        Subscriber().publish(Discover.get_id(), server)


class Driver():
    @staticmethod
    def get_driver():
        config = Config.load_config()
        Redis = redis.StrictRedis(
            **{'host': config['host'], 'password': config['password'], 'port': config['port'], 'db': config['db']})
        return Redis


# 节点端
class Subscriber():
    def __init__(self):
        self.driver = Driver.get_driver()

    def accept(self):
        pass

    def publish(self, event, data):
        return self.driver.publish(event, data)


class Config():
    @staticmethod
    def load_config():
        return yaml.load(open('./conf/server.yml'))


class LambdaServerAdapter():
    def send(self, server, name, func_body):
        pass


# Http适配器 可以是tcp适配器？ 多进程适配器等
class ScoketAdapter(LambdaServerAdapter):
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
            event_id = Lambda2Hash.make_event_hash(server, func_id)
            data = {
                "event_id": event_id,
                "func_id": func_id,
                "func_body": func_body
            }

            s.send(str.encode(json.dumps(data)))
        else:
            event_id = None

        return event_id


class Lambda2Hash():
    def __init__(self):
        pass

    @staticmethod
    def make_event_hash(server, name):
        server = str(server)
        name = str(name)
        return "LAMBDA2_FUNC_%s@%s" % (str(hashlib.md5((server + name).encode("utf-8")).hexdigest()), name)


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
        self.node_index = 0
        # 计算节点集群
        self.init()

    def init(self):

        self.driver = Driver.get_driver()

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
            self.lambad_adapter_api.send_to(self.get_node(func_id), func_id, func_body)

        return self._compute()

    def get_node(self, key):
        ring = HashRing(self.node_pool)
        node = ring.get_node(key)
        # 检查node 是否可用
        if not self.check_node(node):
            if node in self.node_pool:
                self.node_pool.remove(node)

        return node

    def check_node(self, node):
        return self.lambad_adapter_api.api.make_server(node)

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

    def _check_msg_is_node_return(self, channel):
        return channel.decode('utf-8')[0:12] == 'LAMBDA2_FUNC'

    def _update_result_pool(self):
        event_map_size = len(self.function_sub_map)
        if self.subscribe is not None:
            # 阻塞等待
            for m in self.subscribe.listen():
                if m['type'] == "message":
                    if m['channel'].decode('utf-8') == Discover.get_id():
                        # 动态追加节点
                        node = m['data'].decode('utf-8')
                        node = dict(eval(node))
                        self.add_node(node)

                    # 判断协议标志
                    if self._check_msg_is_node_return(m['channel']):
                        self.result_pool.update(m['channel'], m['data'].decode('utf-8'))
                        event_map_size = event_map_size - 1
                        if event_map_size <= 0:
                            break

    # 更新函数的订阅事件map
    def _update_function_hash_sub_event_map(self, *events):
        for func_id in events:
            node = self.get_node(func_id)
            self.function_sub_map[func_id] = Lambda2Hash.make_event_hash(node, func_id)

    # 订阅事件
    def _sub(self, *events):
        sub_events = []
        for event in events:
            sub_events.append(self.function_sub_map.get(event))

        sub_events.append(Discover.get_id())
        sub = self.driver.pubsub()
        sub.subscribe(sub_events)
        self.subscribe = sub
        return self.subscribe


class HashRing(object):
    def __init__(self, nodes=None, replicas=3):
        """Manages a hash ring.

        `nodes` is a list of objects that have a proper __str__ representation.
        `replicas` indicates how many virtual points should be used pr. node,
        replicas are required to improve the distribution.
        """
        self.replicas = replicas

        self.ring = dict()
        self._sorted_keys = []

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """Adds a `node` to the hash ring (including a number of replicas).
        """
        for i in range(0, self.replicas):
            key = self.gen_key('%s:%s' % (node, i))
            self.ring[key] = node
            self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def remove_node(self, node):
        """Removes `node` from the hash ring and its replicas.
        """
        for i in range(0, self.replicas):
            key = self.gen_key('%s:%s' % (node, i))
            del self.ring[key]
            self._sorted_keys.remove(key)

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        return self.get_node_pos(string_key)[0]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None, None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        for i in range(0, len(nodes)):
            node = nodes[i]
            if key <= node:
                return self.ring[node], i

        return self.ring[nodes[0]], 0

    def get_nodes(self, string_key):
        """Given a string key it returns the nodes as a generator that can hold the key.

        The generator is never ending and iterates through the ring
        starting at the correct position.
        """
        if not self.ring:
            yield None, None

        node, pos = self.get_node_pos(string_key)
        for key in self._sorted_keys[pos:]:
            yield self.ring[key]

        while True:
            for key in self._sorted_keys:
                yield self.ring[key]

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.

        md5 is currently used because it mixes well.
        """
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return int(m.hexdigest(), 16)
