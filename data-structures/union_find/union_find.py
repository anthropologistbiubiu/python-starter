# 并查集
class DisJoinSet:
    def __init__(self,n):
        self.parent = [i for i in range(n)]
        self.rank = [1] * n
    def find(self,i):
        if self.parent[i] == i:
            return i
        return self.find(self.parent[i])
    def union(self,i,j):
        root_i = self.find(i)
        root_j = self.find(j)
        # 需要添加一个防止重复合并带来的bug
        if root_j == root_j:
            return
        if self.rank[root_i] > self.rank[root_j]:
            self.parent[root_j] = root_i
        elif self.rank[root_i] < self.rank[root_j]:
            self.parent[root_i] = root_j
        else:
            self.parent[root_j] = root_i
            self.rank[root_i] += 1
    def display(self):   # 打印不同的集合，打印不同集合的连通性
        # 怎么打印完全所有的集合
        pass



union_set = DisJoinSet(10)
union_set.union(1,2)
union_set.union(2,3)

print(union_set.find(2))
print(union_set.find(3))

union_set.union(4,5)
union_set.union(4,6)

print(union_set.find(5))
print(union_set.find(6))

print(union_set.find(1) == union_set.find(4))

union_set.union(3,6)

print(union_set.parent)
print(union_set.rank)
print(union_set.find(2)==union_set.find(5))

# 集合的查找与合并,打印。
# 如何初始化一个并查集


