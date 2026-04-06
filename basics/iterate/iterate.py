
# 总结一下python 常见的for 循环


for item in 'beijing':
    print(item,end='')
print()

for item in ('a','b','c'):
    print(item)

for item in [1,2,3]:
    print(item)

for item in {'sun','wei','ming'}:
    print(item)

for item in {'name':'sunweiming','age':18}:
    print(item)

# 文件对象的迭代
with open('test.txt', 'r') as file:
    for line in file:
        print(line,end='')

