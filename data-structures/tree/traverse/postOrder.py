from typing import Optional
from typing import  List


class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

class Solution:
    def postorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res = []
        self.postOrder(root)
        return res

    def postOrder(self,root:TreeNode,res:List[int]):
        if not root:
            return
        self.postOrder(root.left,res)
        self.postOrder(root.right,res)
        res.append(root.val)
