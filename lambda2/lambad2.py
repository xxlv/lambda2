# -*- coding: utf-8 -*-

from lambda2 import content_less_api
from lambda2 import function_pool
from lambda2 import result_pool
from lambda2 import driver
from lambda2 import scoket_adapter
from lambda2 import hash_ring
from lambda2 import discover
from lambda2 import lambda2_hash

import inspect
import logging
import os
import sys
try:
    import colorlog
except ImportError:
    pass

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    format      = '%(asctime)s - %(levelname)-8s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    if 'colorlog' in sys.modules and os.isatty(2):
        cformat = '%(log_color)s' + format
        f = colorlog.ColoredFormatter(cformat, date_format,
              log_colors = { 'DEBUG'   : 'reset',       'INFO' : 'reset',
                             'WARNING' : 'bold_yellow', 'ERROR': 'bold_red',
                             'CRITICAL': 'bold_red' })
    else:
        f = logging.Formatter(format, date_format)
    ch = logging.StreamHandler()
    ch.setFormatter(f)
    root.addHandler(ch)

setup_logging()
log = logging.getLogger(__name__)


# Lambda2 Main
class Lambad2():
    def __init__(self):
        self.node_index = 0
        # 计算节点集群
        self.init()

    def init(self):

        self.driver = driver.Driver.get_driver()

        # 计算适配器
        self.lambad_adapter_api = content_less_api.ContentLessApi(scoket_adapter.ScoketAdapter())
        # 计算节点池
        self.node_pool = []
        # 过程
        self.lambda_functions_pool = function_pool.FunctionPool()
        # 聚合
        self.lambda_functions_handle = function_pool.FunctionPool()
        # 结果
        self.result_pool = result_pool.ResultPool()
        # 函数节点map
        self.function_node = {}
        # 函数事件map
        self.function_sub_map = {}

        self.subscribe = None

    def run(self):

        logging.debug("正在执行Run")
        for func_id, func_body in self.lambda_functions_pool.function_pool.items():
            node = self.get_node(func_id)

            logging.debug("正在将计算资源{} 发送到节点 {}:{}".format(func_id,node['ip'],node['port']))
            self.lambad_adapter_api.send_to(node, func_id, func_body)

        return self._compute()

    def get_node(self, key):

        ring = hash_ring.HashRing(self.node_pool)
        node = ring.get_node(key)

        logging.debug("正在为 key {} 分配节点 {}".format(key,node))

        # 检查node 是否可用
        if not self.check_node(node):
            if node in self.node_pool:
                self.node_pool.remove(node)
                logging.warning("正在将节点{} 从节点池中移除".format(node))
            else:
                logging.warning("当前节点{} 不存在".format(node))

        return node

    def make_src(self, func_src):
        return inspect.getsourcelines(func_src)

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

    def _check_msg_is_node_return(self, channel):
        return channel.decode('utf-8')[0:12] == 'LAMBDA2_FUNC'

    def _update_result_pool(self):
        event_map_size = len(self.function_sub_map)
        if self.subscribe is not None:
            # 阻塞等待
            for m in self.subscribe.listen():
                if m['type'] == "message":
                    if m['channel'].decode('utf-8') == discover.Discover.get_id():
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
            self.function_sub_map[func_id] = lambda2_hash.Lambda2Hash.make_event_hash(node, func_id)

    # 订阅事件
    def _sub(self, *events):
        sub_events = []
        for event in events:
            sub_events.append(self.function_sub_map.get(event))

        sub_events.append(discover.Discover.get_id())
        sub = self.driver.pubsub()
        sub.subscribe(sub_events)
        self.subscribe = sub
        return self.subscribe
