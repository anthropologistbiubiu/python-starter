from typing import Optional
from typing import List


class TreeNode:
     def __init__(self, val=0, left=None, right=None):
         self.val = val
         self.left = left
         self.right = right
class Solution:
    def preorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res = []
        self.preOrder(root,res)
        return res
    def preOrder(self,root,res):
        if not root:
            return
        res.append(root.val)
        self.preOrde(root.left,res)
        self.preOrde(root.right,res)






class Solution1:
    def inorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        result = []
        self.inorder(root, result)
        return result

    def inorder(self, node, result):
        if node:
            self.inorder(node.left, result)
            result.append(node.val)
            self.inorder(node.right, result)


solution = Solution1()
print(solution.inorderTraversal([1,3,2]))





