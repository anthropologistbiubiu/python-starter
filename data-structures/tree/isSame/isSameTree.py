from typing import Optional

class TreeNode:
     def __init__(self, val=0, left=None, right=None):
         self.val = val
         self.left = left
         self.right = right

class Solution:
    def isSameTree(self, p: Optional[TreeNode], q: Optional[TreeNode]) -> bool:
        if not p and q:
            return False
        elif not q and p:
            return False
        elif not q and not p:
            return True
        elif q.val != p.val:
           return False
        result = self.isSameTree(p.left,q.left) and self.isSameTree(p.right,q.right)
        if not result:
            return result
        return True