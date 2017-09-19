#!/usr/local/bin/python

import inspect

from lambda2 import lambad2


def a():
    t = 0
    for i in range(1000):
        t += i
    return t


def b():
    t = 0
    for i in range(1000):
        t += i
    return t


def c():
    t = 0
    for i in range(1000):
        t += i
    return t


def lamdba2_sum(r):
    # print(r)
    a = int(r.get('a'))
    b = int(r.get('b'))
    if a is not None and b is not None:
        return (a + b)

    else:
        print("计算节点出错，无法获取正确的数据")
        return 0


lambda2 = lambad2.Lambad2()
lambda2.init()
lambda2.add_nodes([{"ip": "127.0.0.1", "port": "37207"}])
lambda2.add_nodes([{"ip": "127.0.0.1", "port": "37165"}])
lambda2.add_nodes([{"ip": "127.0.0.1", "port": "38716"}])

lambda2.lambda_functions_pool.push("a", lambda2.make_src(a))
lambda2.lambda_functions_pool.push("b", lambda2.make_src(b))
lambda2.lambda_functions_pool.push("c", lambda2.make_src(c))

# 增加聚合处理
lambda2.lambda_functions_handle.push("lamdba2_sum", lambda2.make_src(lamdba2_sum))

# 声明此次计算的依赖
# TODO 自动发现依赖即可
lambda2.depend_on("a", "b", "c")

# 执行计算
r = lambda2.run()

print(r)
