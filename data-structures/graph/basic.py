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

# 获取图中所有节点
nodes = sorted(set(node for nodes in graph.values() for node in nodes))

# 创建邻接矩阵
adj_matrix = [[0] * len(nodes) for _ in range(len(nodes))]

# 填充邻接矩阵
for node, neighbors in graph.items():
    for neighbor in neighbors:
        adj_matrix[nodes.index(node)][nodes.index(neighbor)] = 1
        adj_matrix[nodes.index(neighbor)][nodes.index(node)] = 1

# 打印邻接矩阵
for row in adj_matrix:
    print(row)