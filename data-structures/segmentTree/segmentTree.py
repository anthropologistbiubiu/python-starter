
class SegmentNode:
    def __init__(self,start,end) -> None:
        self.total = 0
        self.start = start
        self.end = end
        self.left = None
        self.right = None


class SegmentNodeTree:
    def __init__(self,nums) -> None:
        self.nums = nums
        self.root = None

    def buildSegmentTree(self):
        self.root = self._buildSegmentTree(0,len(self.nums)-1)

    def _buildSegmentTree(self,start,end):
        if start > end:
            return None

        if start == end:
            node = SegmentNode(start,end)
            node.total = self.nums[end]
            return node

        mid = start + (end - start) // 2
        left_node = self._buildSegmentTree(start,mid)
        right_node = self._buildSegmentTree(mid+1,end)
        # 退栈空间的内存操作
        node = SegmentNode(start,end)
        node.total = left_node.total + right_node.total
        node.left = left_node
        node.right = right_node
        return node

    def range_sum(self,start,end):
        return self._range_sum(self.root,start,end)
    def _range_sum(self,root,start,end):
        if root.start == start and root.end == end:
            return root.total
        mid = root.start + (root.end - root.start) // 2
        if start > mid:
            return self._range_sum(root.right,start,end)
        elif end <= mid:
            print('end',end,mid)
            return self._range_sum(root.left,start,end)
        return self._range_sum(root.left,start,mid) + self._range_sum(root.right,mid+1,end)

    def travels(self):
        self._travels(self.root)
    def _travels(self,root):
        if root == None:
            return
        self._travels(root.left)
        self._travels(root.right)
    def update(self):
        self._update(self.root)
    def _update(self,root):
        pass

def main():
    nums = [1, 3, 5, 7, 9, 11, 13, 15]
    segmentTree1 = SegmentNodeTree(nums)
    segmentTree1.buildSegmentTree()
    segmentTree1.travels()
    print(segmentTree1.range_sum(0,1))

if __name__ == '__main__':
    main()
