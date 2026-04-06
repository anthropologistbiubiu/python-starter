from typing import Optional
from typing import List


class TreeNode:
    pass

class Solution:
    def inorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res = []
        self.inOrder(root,res)
        return res
    def inOrder(self,root:TreeNode,res:List[int]):
        if not root:
           return
        self.inOrder(root.left,res)
        res.append(root.val)
        self.inOrder(root.rignt,res)


