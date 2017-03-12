#!/usr/local/bin/python

# 接受 函数
# 执行函数
# 执行回调
# 可以这样做，首先，将计算描述发送给计算节点，并且发送一个唯一的事件编号。计算节点在处理完毕后，将发布该事件，结果即使内容。
# 客户端可以订阅这些事件。

import lambda2
import socket
import json
from multiprocessing import Process


address = ('127.0.0.1', 31502)
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.bind(address)
node=lambda2.Node()
node.accept()
s.listen(5)


def compute(data_o):
    event_id=data_o['event_id']
    func_body=data_o['func_body']
    func_id=data_o['func_id']
    # TODO
    # Check expression
    # Check cache
    func_body_str="".join(func_body[0])
    exec(func_body_str)
    data=eval("{}()".format(func_id))
    return data



while True:

    print("waiting")
    ss,addr=s.accept()
    # TODO 按照协议 一直读取直到协议终止
    data=ss.recvmsg(10086)[0].decode('utf-8')
    data_o=json.loads(data)
    event_id=data_o['event_id']

    # func_body=data_o['func_body']
    # func_id=data_o['func_id']
    # # TODO
    # # Check expression
    # # Check cache
    # func_body_str="".join(func_body[0])
    # exec(func_body_str)
    # data=eval("{}()".format(func_id))
    p = Process(target=compute, args=(data_o,))
    p.start()
    p.join()


    node.publish(event_id,compute(data_o))

    print("Publish {}".format(event_id))
