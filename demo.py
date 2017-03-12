#!/usr/local/bin/python
import inspect
import time
import lambda2

def a():
    t=0
    for i in range(101010):
        print("a compute {}".format(str(i)))
        t+=i
    return t

def b():
    t=0
    for i in range(101010):
        print("b compute {}".format(str(i)))
        t+=i
    return t


def lamdba2_sum(a,b):
    # 核心计算
    a=lambda2.need(a)
    b=lambda2.need(b)
    return a+b


# curr=time.time()
# result_a=a()
# result_b=b()
# curr2=time.time()
# print(curr2-curr)
# TODO 这个文件里面不要出现计算，分解计算到节点上 直接获取结果即可
# TODO 缓存 每一个计算的HASH 都可以被缓存 ，每次计算都一样

lambda2=lambda2.Lambad2()
#
#
# # new ways init servers
#
lambda2.init()
#
lambda2.add_nodes(["server1","server2"])
#
# # hash map to server
# TODO 增加语言适配器
lambda2.lambda_functions_pool.push("a",inspect.getsourcelines(a))
lambda2.lambda_functions_pool.push("b",inspect.getsourcelines(b))
#
# # how to compute ?
lambda2.run()
#
# result_a=lambda2.pull('func_a')
# result_b=lambda2.pull('func_a')
# #
# Redis=redis.StrictRedis(**{'host':'127.0.0.1','password':'helloworld','port':'6379','db':0})
# events=["test.server.php56.on_error",'production.server.biz-production.on_error','vs.on_publish']
# sub=Redis.pubsub()
# sub.subscribe(events)
sub=lambda2.sub("a","b")

c=2
result=0
# TODO Hide this
for m in sub.listen():
    if m['type']=="message":
        print("----------------------------->")
        # if c==0:
        #     break
        # c=c-1

        t=int(m['data'].decode('utf-8'))
        print("Found value from remote  {}".format(str(t)))
        # 这里其实也是计算
        # 对计算进行依赖申明即可
        # result=result+t



print(result)
