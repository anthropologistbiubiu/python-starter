# 元祖的遍历


# 生成器表达式
my_tuple = tuple(x**2 for x in range(5))
print(my_tuple)

# 遍历元祖数据
for item in my_tuple:
    print(item,' ',end='')

# 索引遍历
for index,item in enumerate(my_tuple):
    print(index,item)
