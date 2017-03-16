#!/usr/local/bin/python
import inspect
import time
import lambda2

def a():
    t=0
    for i in range(100):
        print("a compute {}".format(str(i)))
        t+=i
    return t

def b():
    t=0
    for i in range(100):
        print("b compute {}".format(str(i)))
        t+=i
    return t


def lamdba2_sum(r):
    # TODO
    return r['a']+r['b']

# curr=time.time()
# result_a=a()
# result_b=b()
# curr2=time.time()
# print(curr2-curr)
# TODO 这个文件里面不要出现计算，分解计算到节点上 直接获取结果即可
# TODO 缓存 每一个计算的HASH 都可以被缓存 ，每次计算都一样

lambda2=lambda2.Lambad2()
lambda2.init()
lambda2.add_nodes(["server1","server2"])
# # hash map to server
# TODO 增加语言适配器
lambda2.lambda_functions_pool.push("a",inspect.getsourcelines(a))
lambda2.lambda_functions_pool.push("b",inspect.getsourcelines(b))
lambda2.lambda_functions_handle.push("lamdba2_sum",inspect.getsourcelines(lamdba2_sum))


# # how to compute ?
# result_a=lambda2.pull('func_a')
# result_b=lambda2.pull('func_a')
lambda2.depend("a","b")
lambda2.run()
