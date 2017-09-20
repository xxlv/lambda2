#!/usr/local/bin/python
# -* coding: utf-8 -*-

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

l = lambad2.Lambad2()
l.init()

l.add_nodes([{"ip": "127.0.0.1", "port": "36707"}])
# l.add_nodes([{"ip": "127.0.0.1", "port": "31859"}])

l.lambda_functions_pool.push("a", l.make_src(a))
l.lambda_functions_pool.push("b", l.make_src(b))
l.lambda_functions_pool.push("c", l.make_src(c))

# 增加聚合处理
l.lambda_functions_handle.push("lamdba2_sum", l.make_src(lamdba2_sum))


# 声明此次计算的依赖
# TODO 自动发现依赖即可
l.depend_on("a", "b", "c")

# 执行计算
r = l.run()

print(r)
