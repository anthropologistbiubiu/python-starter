class MinHeap:
    def __init__(self):
        self.heap = []
        self.size = 0

    def push(self, item):
        self.heap.append(item)
        self._heapify_up()
        self.size += 1

    def pop(self):
        if self.size == 0:
            return None
        top_item = self.heap[0]
        self._swap(0, self.size - 1)
        self.size -= 1
        self._heapify_down()
        return top_item

    def _heapify_up(self):
        # 想清楚堆想上调整的过程中的思路
        index = len(self.heap) - 1
        while index >= 0:
            parent_index = (index - 1) // 2
            if parent_index >= 0 and self.heap[parent_index] > self.heap[index]:
                self._swap(parent_index, index)
            else:
                break
            index = parent_index

    def _heapify_down(self):
        cur_index = 0
        while cur_index * 2 + 1 < self.size:
            left_index = cur_index * 2 + 1
            right_index = cur_index * 2 + 2
            if right_index < self.size and self.heap[right_index] < self.heap[left_index]:
                min_index = right_index
            else:
                min_index = left_index
            if self.heap[min_index] < self.heap[cur_index]:
                self._swap(min_index, cur_index)
            cur_index = min_index

    def _swap(self, i, j):
        self.heap[i], self.heap[j] = self.heap[j], self.heap[i]

    def display(self):
        # 如何打印出堆的结构形状
        print(self.heap[:self.size])

    def sort(self, data=None):
        # 堆排序
        if len(data) == 0:
            return None
        for item in data:
            self.push(item)
        print('size',self.size)
        while self.size > 0:
            top = self.pop()
            data[len(data) - self.size - 1] = top

    def print_heap_structure(self):
        depth = 0
        current_level = 0
        elements_on_level = 1

        while current_level < len(self.heap):
            print("  " * (depth), end="")
            for i in range(elements_on_level):
                if current_level + i < len(self.heap):
                    print(f"{self.heap[current_level + i]}", end=" ")
                else:
                    break
            print()
            current_level += elements_on_level
            elements_on_level *= 2
            depth += 1


myheap = MinHeap()
# myheap.push(8)
# myheap.push(3)
# myheap.push(4)
# myheap.push(6)
# myheap.push(1)
# myheap.push(2)
# myheap.pop()
myheap.display()

data = [9, 8, 7, 6, 5, 4, 3, 2, 1]
myheap.sort(data)
print(data)
