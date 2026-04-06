# deque 用法

from collections import deque

# 创建双端队列
my_queue = deque([1,3,4])
print(my_queue,type(my_queue))

# 添加元素
my_queue.append(8)
# 弹出元素
print(my_queue.pop())
# 从左端弹出元素
print(my_queue.popleft())
# 从右侧扩展
my_queue.extend([2,8,9])
# 从左侧扩展
my_queue.extendleft(['sun','wei','ming'])
# 队列旋转
print('before rotate',my_queue)
my_queue.rotate(1)
print('after rotate',my_queue)
print(my_queue)
# 移除元素
print(my_queue)
my_queue.remove(2)
print(my_queue)
# 求长度
print(len(my_queue))
# 访问队列
print(my_queue[0])
# 清空队列
print(my_queue.clear())

