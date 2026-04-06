
class BinaryIndexTree:
    def __init__(self, nums):
        self.tree = [0] * (len(nums) + 1)
        self.capacity = len(nums)
        for i in range (self.capacity):
            self.update(i+1, nums[i])

    def update(self, index, item):
        while index <= self.capacity:
            self.tree[index] += item
            index += self.lowbit(index)

    def lowbit(self, x):
        return x & -x

    def query(self, index):
        total = 0
        while index > 0:
            total += self.tree[index]
            index -= self.lowbit(index)
        return total

    def query_rang(self, end, start):
        return self.query(end) - self.query(start - 1)


def main():
    nums = [1, 3, 5, 7, 9, 11, 13, 15]
    bit = BinaryIndexTree(nums)
    print(bit.tree)
    print(bit.query(2))
    print(bit.query_rang(5,1))


if __name__ == '__main__':
    main()