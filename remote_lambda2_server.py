#!/usr/local/bin/python
# 接受 函数
# 执行函数
# 执行回调
# 可以这样做，首先，将计算描述发送给计算节点，并且发送一个唯一的事件编号。计算节点在处理完毕后，将发布该事件，结果即使内容。
# 客户端可以订阅这些事件。

import lambda2
import socket
import json
import time
import random

port = random.randrange(30000, 40000)
address = ('127.0.0.1', port)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(address)
subscriber = lambda2.Subscriber()

s.listen(5)


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


while True:
    # 发布服务事件
    print("当前绑定端口   " + str(port) + "  正在监听...")

    lambda2.Discover.mount({"ip": "127.0.0.1", "port": port})
    # subscriber.publish("asdada", "asdasd")
    #
    ss, addr = s.accept()
    # TODO 按照协议 一直读取直到协议终止
    data = ss.recvmsg(10086)[0].decode('utf-8')
    data_o = json.loads(data)

    event_id = data_o['event_id']

    # 检查even 是否合法
    # print(event_id)
    # func_body=data_o['func_body']
    # func_id=data_o['func_id']
    # # TODO
    # # Check expression
    # # Check cache
    # func_body_str="".join(func_body[0])
    # exec(func_body_str)
    # data=eval("{}()".format(func_id))
    # p = Process(target=compute, args=(data_o,))
    # p.start()
    # p.join()
    # compute(data_o)

    data = compute(data_o)

    print("计算完毕，正在发布 " + event_id)
    subscriber.publish(event_id, data)
    print("Publish {}".format(event_id))
    time.sleep(2)
