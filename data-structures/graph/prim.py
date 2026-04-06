import heapq
# 最小生成树算法

# 给出prime 算法的
# 首先从任意节点开始,把邻节点的权重都加入到一个堆中。
# 然后从相邻节点找权重最小的边加入到最小生成树中
# 直到把所有的节点都加入到生成树中







def prim(graph,start):
    visited = set()
    visited.add(start)
    heap = []
    min_tree = []
    for neighbor ,weight in graph[start].items():
        heapq.heappush(heap,(start,neighbor,weight))
    heapq.heapify(heap)
    while heap:
        cur,end,min_weight = heapq.heappop(heap)
        if end not in visited:
            visited.add(end)
            min_tree.append((cur, end, min_weight))
            for neighbor,weight in graph[end].items():
                heapq.heappush(heap,(end,neighbor,weight))
    return min_tree





graph = {
    'A': {'B': 2, 'C': 1},
    'B': {'A': 2, 'C': 3, 'D': 1},
    'C': {'A': 1, 'B': 3, 'D': 2},
    'D': {'B': 1, 'C': 2}
}

if __name__ == '__main__':
    print(prim(graph,'A'))







