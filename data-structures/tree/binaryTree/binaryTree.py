
class Node:
    def __init__(self,data=None):
        self.data = data
        self.left = None
        self.right = None

class BinaryTree:
    def __init__(self):
        self.root = None
    def insert(self,key):
        self.root = self._insert(self.root,key)
    def _insert(self,root,key):
        if root is None:
            root = Node(key)
        elif root.data > key:
           root.left = self._insert(root.left,key)
        elif root.data < key:
            root.right = self._insert(root.right,key)
        return root
    def preorder_traversal(self):
        if self.root is None:
            print(None)
        self._preorder_traversal(self.root)
    def _preorder_traversal(self,root):
        if root is None:
            return
        print(root.data)
        self._preorder_traversal(root.left)
        self._preorder_traversal(root.right)
    def inorder_traversal(self):
        if self.root is None:
            print(None)
        self._inorder_traversal(self.root)
    def _inorder_traversal(self,root):
        if root is None:
            return
        self._inorder_traversal(root.left)
        print(root.data)
        self._inorder_traversal(root.right)
    def postorder_traversal(self):
        self._postorder_traversal(self.root)
    def _postorder_traversal(self,root):
        if root is None:
            return
        self._postorder_traversal(root.left)
        self._postorder_traversal(root.right)
        print(root.data)

    def update(self):
        pass
    def delete(self):
        pass

mytree = BinaryTree()
mytree.insert(2)
mytree.insert(1)
mytree.insert(4)
mytree.insert(5)
mytree.insert(3)
mytree.insert(18)
mytree.insert(7)
mytree.insert(9)
print('preorder_traversal')
mytree.preorder_traversal()
print('inorder_traversal')
mytree.inorder_traversal()
print('postorder_traversal')
mytree.postorder_traversal()
