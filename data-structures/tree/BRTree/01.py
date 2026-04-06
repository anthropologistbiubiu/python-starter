
# 完成红黑树的所有增删改查
class Node:
    def __init__(self, key, color, left=None, right=None, parent=None):
        self.key = key
        self.color = color
        self.left = left
        self.right = right
        self.parent = parent

class RedBlackTree:
    def __init__(self):
        self.NIL = Node(None, "BLACK")  # NIL节点，表示空节点
        self.root = self.NIL

    def left_rotate(self, x):
        y = x.right
        x.right = y.left

        if y.left != self.NIL:
            y.left.parent = x

        y.parent = x.parent

        if x.parent == self.NIL:
            self.root = y
        elif x == x.parent.left:
            x.parent.left = y
        else:
            x.parent.right = y

        y.left = x
        x.parent = y

    def right_rotate(self, y):
        x = y.left
        y.left = x.right

        if x.right != self.NIL:
            x.right.parent = y

        x.parent = y.parent

        if y.parent == self.NIL:
            self.root = x
        elif y == y.parent.left:
            y.parent.left = x
        else:
            y.parent.right = x

        x.right = y
        y.parent = x

    def insert(self, key):
        new_node = Node(key, "RED", self.NIL, self.NIL, self.NIL)  # 新插入的节点为红色
        self._insert(new_node)
        self.root.color = "BLACK"  # 根节点始终为黑色

    def _insert(self, z):
        y = self.NIL
        x = self.root

        while x != self.NIL:
            y = x
            if z.key < x.key:
                x = x.left
            else:
                x = x.right

        z.parent = y
        if y == self.NIL:
            self.root = z
        elif z.key < y.key:
            y.left = z
        else:
            y.right = z

        self._insert_fixup(z)

    def _insert_fixup(self, z):
        while z.parent.color == "RED":
            if z.parent == z.parent.parent.left:
                y = z.parent.parent.right
                if y.color == "RED":   # 叔父节点是红色
                    z.parent.color = "BLACK"  # 父节点设为红色
                    y.color = "BLACK"  # 叔父节设为黑色
                    z.parent.parent.color = "RED" # 祖父节点设为红色
                    z = z.parent.parent # 当前节点设为祖父节点 ,检查父节点
                else:                         # 叔父节点为黑色
                    if z == z.parent.right: # 当前节点是父节点的右节点
                        z = z.parent
                        self.left_rotate(z)   # 对新节点的父节点进行左旋
                    z.parent.color = "BLACK"
                    z.parent.parent.color = "RED"
                    self.right_rotate(z.parent.parent) # 右旋，以当前节点的祖父节点
            else:
                y = z.parent.parent.left
                if y.color == "RED":
                    z.parent.color = "BLACK"
                    y.color = "BLACK"
                    z.parent.parent.color = "RED"
                    z = z.parent.parent
                else:
                    if z == z.parent.left:
                        z = z.parent
                        self.right_rotate(z)
                    z.parent.color = "BLACK"
                    z.parent.parent.color = "RED"
                    self.left_rotate(z.parent.parent)

        self.root.color = "BLACK"


def print_tree(node, level=0, prefix="Root: "):
    if node != None:
        print(" " * (level * 4) + prefix + f"{node.key} ({node.color})")
        if node.left != None or node.right != None:
            print_tree(node.left, level + 1, "L--- ")
            print_tree(node.right, level + 1, "R--- ")

# 创建一个红黑树实例
rb_tree = RedBlackTree()

# 插入节点
rb_tree.insert(10)
rb_tree.insert(20)
rb_tree.insert(5)

# 打印红黑树结构
print_tree(rb_tree.root)

