# 写一个不带头节点的单链表的增删改查操作
class Node:
    def __init__(self, data=None):
        self.data = data
        self.next = None


class LinkList:
    def __init__(self):
        self.head = None
        self.capacity = 0

    # 插入节点
    def append(self, data):
        new_node = Node(data)
        if self.head is None:
            self.head = new_node
            return
        last_node = self.head
        while last_node.next:
            last_node = last_node.next
        last_node.next = new_node  # 尾插法
        self.capacity += 1

    def delete(self, data):
        cur_node = self.head
        if cur_node is None:
            return
        # 单独处理头节点
        if cur_node.data == data:
            self.head = self.head.next
            self.capacity += 1
        # 处理非头节点
        while cur_node.next and cur_node.next.data != data:
            cur_node = cur_node.next
        if cur_node.next is None:
            return
        cur_node.next = cur_node.next.next
        self.capacity += 1

    def update(self, index, new_data):
        cur_node = self.head
        if cur_node is None:
            return None
        if index < 0 or index > self.capacity:
            return None
        i = 0
        while i < index:
            cur_node = cur_node.next
            i += 1
        cur_node.data = new_data

    def index(self, index):
        cur_node = self.head
        if cur_node is None:
            return None
        if index >= self.capacity or index < 0:
            return None
        i = 0
        while i < index:
            cur_node = cur_node.next
            i += 1
        return cur_node.data

    def reverse(self):  # 反转链表
        if self.head is None:
            return
        cur_node = self.head
        pre = None
        while cur_node is not None:
            tem_node = cur_node.next
            cur_node.next = pre
            pre = cur_node
            cur_node = tem_node
        self.head = pre

    def sort(self):
        left = self.head
        if self.head is None or self.capacity <= 1:
            return
        while left.next:
            left = left.next
        right = left
        left = self.head
        while right != self.head:
            while left.next != right:
                if left.data > left.next.data:
                    left.data,left.next.data = left.next.data,left.data
                left = left.next
            if left.data > left.next.data:
                left.data, left.next.data = left.next.data, left.data
            right = left
            left = self.head

    def didsplay(self):
        cur = self.head
        while cur:
            if cur.next == None:
                print(cur.data)
                return
            print(cur.data, end='->')
            cur = cur.next


mylist = LinkList()
mylist.append(1)
mylist.append(2)
mylist.append(3)
mylist.append(4)
mylist.didsplay()
# mylist.delete(1)
# print("delete 1")
# mylist.didsplay()
# mylist.delete(4)
# print("delete 4")
# mylist.didsplay()
# print("delete 3")
# mylist.delete(3)
# mylist.didsplay()
# print('mylist.index')
# print(mylist.index(0))
# print('mylist.update')
# mylist.update(0,100)
print('mylist.reverse')
mylist.reverse()
mylist.didsplay()
mylist.sort()
print('after mylist.sort')
mylist.didsplay()
mylist.append(0)
mylist.append(9)
mylist.append(8)
mylist.append(23)
mylist.append(13)
mylist.append(27)
mylist.append(7)
mylist.didsplay()
print('after mylist.sort')
mylist.sort()
mylist.didsplay()



