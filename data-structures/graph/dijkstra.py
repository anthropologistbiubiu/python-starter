# 示例使用
from collections import deque
import heapq
graph = {
    'A': {'B': 1, 'C': 4},
    'B': {'A': 1, 'D': 2, 'E': 7},
    'C': {'A': 4, 'F': 5, 'G': 2},
    'D': {'B': 2},
    'E': {'B': 7, 'H': 1},
    'F': {'C': 5},
    'G': {'C': 2},
    'H': {'E': 1},
}
graph_with_weights = {
    'A': {'B': 9, 'C': 1},
    'B': {'A': 9, 'D': 1},
    'C': {'A': 1, 'D': 2},
    'D': {'B': 1,'C':2},
}
# 初始化最短距离
# 把邻接的节点开始加入到堆中
# 开始比较已知最短路径与源节点到当前节点的的距离，如果小于则更新源节点到其他节点的最短距离，如果不小于则不需要通过当前节点来更新
# 重复以上步骤

# 如果使用队列来求解最短路径,是否有必要使用set()对这个集合对已经访问过的节点再去访问。
# 其次为什么要用heap 来记录这个访问的过程，而不是队列。
# 是否可以解决有环路的最短路径问题
# 是否可以解决带负权变的最短路径

'''
def dijkstra(graph,start,distances):
    visited = set()
    queue = deque()
    queue.append(start)
    distances[start] = 0
    while queue:
        current_node = queue.popleft()
        for neighbor, weight in graph[current_node].items():
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
                if weight > distances[neighbor]:
                    continue
                elif weight + distances[current_node] < distances[neighbor]:
                    distances[neighbor] = weight + distances[current_node]
'''

# 优化版本
# 贪心的策略是体现在选择中继节点的时候，选择最短的距离，体现贪心的策略，然后遍历完全整个优先级队列。
def dijkstra_with_heap(graph,start):
    distances = {node:float('infinity') for node in graph}
    distances[start] = 0
    priority_queue = [(start,0)]
    while priority_queue:
        current_node, current_distance = heapq.heappop(priority_queue)
        if distances[current_node] < current_distance:
            continue
        for neighbor,weight in graph[current_node].items():
           distance =  current_distance + weight
           if distance < distances[neighbor]:
               distances[neighbor] = distance
               heapq.heappush(priority_queue,(neighbor,distance))
    return distances

if __name__ == '__main__':
    distances ={node:float('infinity') for node in graph_with_weights}
    # dijkstra(graph_with_weights, 'A',distances)
    print(dijkstra_with_heap(graph,'A'))
