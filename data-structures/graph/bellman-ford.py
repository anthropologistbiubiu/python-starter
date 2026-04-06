
graph_with_weights = {
    'A': {'B': 1, 'C': 4},
    'B': {'A': 1, 'D': 2, 'E': 7},
    'C': {'A': 4, 'F': 5, 'G': 2},
    'D': {'B': 2},
    'E': {'B': 7, 'H': 1},
    'F': {'C': 5},
    'G': {'C': 2},
    'H': {'E': 1},
}
# 是否可以解决带有环路的图的最短路径。
# 是否可以解决带有负权重的环的最短路径。
# 示例使用
def bellman_ford(graph,start):
    distances = {node: float('infinity') for node in graph}
    distances[start] = 0
    for _ in range (len(graph)-1):
        for node in graph:
            for neighbor, weight in graph[node].items():
                if distances[neighbor] > distances[node] + weight:
                    distances[neighbor] = distances[node] + weight

    for node in graph:
        for neighbor, weight in graph[node].items():
            if distances[neighbor] > distances[node] + weight:
                return None
    return distances



if __name__ == '__main__':
    start_node = 'A'
    shortest_distances_bf = bellman_ford(graph_with_weights, start_node)
    print(f"Shortest distances from {start_node}: {shortest_distances_bf}")



# 示例使用
'''
0 --(1)--> 1 --(-1)--> 2
 ^            |
 |            v
 4 <---(2)--- 3 --(-3)--> 4
'''
