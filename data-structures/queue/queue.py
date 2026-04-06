from collections import deque

# deque拥有众多的方法，因为deque是一个双向链表，所以在头和尾都可以操作，方法有如下：
# deque.append(x) 在队列右边（尾部）插入x
# deque.appendleft(x) 在队列左边（头部）插入x
# deque.pop() 在队列左边（尾部）出栈
# deque.popleft() 在队列右边（头部）出栈
# deque.clear() 清空队列，初始化队列长度为1
# deque.copy() 创建一个浅拷贝的队列
# deque.count(x) 计算队列中x元素的个数
# deque.extend(iterable) 将可迭代对象扩展到队列右边
# deque.extendleft(iterable)将可迭代对象扩展到队列左边
# deque.index(x) 返回元素x在队列中的位置，没有找到抛出异常ValueError
# deque.insert(i, x) 插入元素x到指定位置x。如果位置超出范围，抛出IndexError异常
# deque.remove(x) 删除第一个找到的元素x，没有元素抛出ValueError异常
# deque.reverse() 逆序
# deque.rotate(n) 将队列向右移动n个元素，如果n为负数则向左移动n个元素
# deque.maxlen() 返回队列长度，为空返回None。


def main():
    my_queue = deque([1,2,3])
    while  len(my_queue) > 0:
        print(my_queue.pop())
    my_queue.append(4)
    my_queue.appendleft(8)
    print('after append {}'.format(my_queue))
    my_queue.reverse()
    print("after reverse {}".format(my_queue))
    print('after remove {}'.format(my_queue))
    my_queue.clear()
    print('after clear {}'.format(my_queue))
    my_copy_queue = my_queue.copy()
    print('after copy {}'.format(my_copy_queue))

if __name__ == '__main__':
    main()