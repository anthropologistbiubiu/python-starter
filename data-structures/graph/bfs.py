# 广度优先遍历
from collections import deque

graph = {
    'A': ['B', 'C'],
    'B': ['A', 'D', 'E'],
    'C': ['A', 'F', 'G'],
    'D': ['B'],
    'E': ['B', 'H'],
    'F': ['C'],
    'G': ['C'],
    'H': ['E']
}
# 解释 BFS 代码：
# visited 是一个集合，用于跟踪已经访问过的节点。
# queue 是一个双端队列，用于存储待访问的节点。
# 将起始节点 start 放入队列。
# 循环直到队列为空：
# 出队并打印队首的节点 node。
# 将 node 添加到 visited 中。
# 将 node 的未访问邻居加入队列。

def bfs(graph,start,queue,visited):
    queue.append(start)
    while len(queue) >0:
        current_node = queue.popleft()
        print(current_node , end=' ')
        visited.add(current_node)
        for item in graph[current_node]:
            if item not in visited:
                queue.append(item)
    print('')


if __name__ == '__main__':
    queue = deque()
    visited = set()
    bfs(graph, 'A', queue,visited)
