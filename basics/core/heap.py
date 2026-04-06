# 内置堆模块的应用
import heapq



my_list = [10,9,8,7,6,5,4,3,2,1]
heapq.heapify(my_list)
# 插入数据
heapq.heappush(my_list,0)
# 取出数据
min_value = heapq.heappop(my_list)
print(min_value)
# 从堆中取出最大元素
n_large = heapq.nlargest(3,my_list)
print(n_large,type(n_large))
# 从堆中取出最小n个元素
n_small = heapq.nsmallest(3,my_list)
print(n_small,type(n_small))
# 合并有序堆
heap1 = [1,3]
heap2 = [2,4]
heap_merged = heapq.merge(heap1,heap2)
print(list(heap_merged))


