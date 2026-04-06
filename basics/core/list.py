# 列表推导式子
my_list = [x**2 for x in range (10)]
print(my_list)

# 遍历列表
for item in my_list:
    print(item,end=' ')
print()

# 遍历索引和数据
for index,item in enumerate(my_list):
   print(index,item)
print()

# 列表解析式
my_list1 = [(x,y) for x in range (5) for y in range (5)]
print(my_list1)
