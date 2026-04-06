# 深度优先遍历

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
# 解释 DFS 代码：
# visited 是一个集合，用于跟踪已经访问过的节点。
# 函数 dfs 接受图、起始节点 start 和 visited 集合作为参数。
# 对于当前节点 start，打印它，并将其添加到 visited 中。
# 对于 start 的每个未访问的邻居，递归调用 dfs。
def dfs(graph,start,visited):
    print(start)
    visited.add(start)
    for item in graph[start]:
       if item not in visited:
           dfs(graph,item,visited)


if __name__ == '__main__':
    visited = set()
    dfs(graph, 'A',visited)
    print(visited)
