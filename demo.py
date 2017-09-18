#!/usr/local/bin/python

import inspect
import lambda2


def a():
    t = 0
    for i in range(100):
        print("a compute {}".format(str(i)))
        t += i
    return t


def b():
    t = 0
    for i in range(100):
        print("b compute {}".format(str(i)))
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


# curr=time.time()
# result_a=a()
# result_b=b()
# curr2=time.time()
# print(curr2-curr)
# TODO 这个文件里面不要出现计算，分解计算到节点上 直接获取结果即可
# TODO 缓存 每一个计算的HASH 都可以被缓存 ，每次计算都一样

lambda2 = lambda2.Lambad2()
lambda2.init()
lambda2.add_nodes(["a"])
# # hash map to server
# TODO 增加语言适配器

# 将可计算单元推送到函数池中
lambda2.lambda_functions_pool.push("a", inspect.getsourcelines(a))

lambda2.lambda_functions_pool.push("b", inspect.getsourcelines(b))

# 增加聚合处理
lambda2.lambda_functions_handle.push("lamdba2_sum", inspect.getsourcelines(lamdba2_sum))

# 声明此次计算的依赖
# TODO 自动发现依赖即可
lambda2.depend_on("a", "b")

# 执行计算
r = lambda2.run()

print(r)
