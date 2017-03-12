#!/usr/local/bin/python
import hashlib
import redis
import socket
import json

Redis=redis.StrictRedis(**{'host':'127.0.0.1','password':'helloworld','port':'6379','db':0})

# 节点端
class Node():
    def __init__(self):
        self.driver=Redis

    def accept(self):
        pass

    def publish(self,event,data):
        self.driver.publish(event,data)

# -------------------------------------------------------------------------------------------
# 抽象适配器
class LambdaServerAdapter():
    def send(self,server,name):
        pass

# Http适配器 可以是tcp适配器？ 多进程适配器等
class ScoketAdapter(LambdaServerAdapter):
    def send(self,server,func_id,func_body):
        address = ('127.0.0.1', 31502)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(address)
        event_id=self._make_event_id(server,func_id)
        data={
            "event_id":event_id,
            "func_id":func_id,
            "func_body":func_body
        }

        s.send(str.encode(json.dumps(data)))
        return event_id

    def _make_event_id(self,server,name):
        return "LAMBDA2-%s" % str(hashlib.md5((server+name).encode("utf-8")).hexdigest())


# API
class ContentLessApi():

    def __init__(self,api):
        self.api=api

    def send_to(self,server,func_id,func_body):
        return self.api.send(server,func_id,func_body)


# 结果集
class ResultPool():

    def __init__(self):
        self.result_pool={}

    # pull result from server
    def pull(self,func):
        return self.result_pool.get(func,None)


# 函数
class FunctionPool():
    def __init__(self):
        self.function_pool={}

    def push(self,func_id,func_body):
        self.function_pool[func_id]=func_body

    def pull(self,func_id):
        pass

# Lambda2 Main
class Lambad2():

    def __init__(self):
        # 计算节点集群
        self.node_pool=[]
        # 过程
        self.lambda_functions_pool=FunctionPool()
        # 结果
        self.lambda_result_pool=ResultPool()

        # 函数节点map
        self.function_node={}
        # 计算适配器
        self.lambad_adapter_api=ContentLessApi(ScoketAdapter())
        # 函数事件map
        self.function_sub_map={}
        self.driver=Redis


    def init(self):
        pass

    def run(self):
        print("Start compute")
        for func_id,func_body in self.lambda_functions_pool.function_pool.items():
            # 当计算完毕后，怎么通知到这里？？？
            self.function_sub_map[func_id]=self.lambad_adapter_api.send_to(self.node_pool[self._hash(func_id)],func_id,func_body)


    def sub(self,*events):
        sub_events=[]
        for event in events:
            sub_events.append(self.function_sub_map.get(event))

        sub=self.driver.pubsub()
        sub.subscribe(sub_events)
        return sub


    def add_node(self,node):
        self.node_pool.append(node)

    def add_nodes(self,nodes):
        self.node_pool.extend(nodes)

    def remove_node(self,node):
        pass

    # def lambda_functions_pool(self):
    #     return self.function_pool

    # def lambda_result_pool(self):
    #     return self.redult_pool
    def _hash(self,key):
        # 给定一个函数的identity,输出一个server id
        node_len=len(self.node_pool)
        if node_len==0:
            node_len=1

        md5_key=hashlib.md5(key.encode('utf-8')).hexdigest()
        key_sum=1
        for c in md5_key:
            key_sum=key_sum+ord(c)

        mode=key_sum%node_len
        return mode
