
# 字典推导式
squares = {x: x*x for x in range(1, 6)}
#  结果：{1: 1, 2: 4, 3: 9, 4: 16, 5: 25}
print(squares)

# 遍历字典的key
for key in squares:
    print(key,end=' ')
print()
# 遍历字典的value
for value in squares.values():
    print(value,end=' ')
print()
# 遍历字典的value
for key,value in squares.items():
    print(key,value,';',end='')
print()

# 遍历字典的 key,value


