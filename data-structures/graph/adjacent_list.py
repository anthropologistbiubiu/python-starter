# 创建邻接表
class Graph:
    def __init__(self):
        self.adj_list = {}

    def add_edge(self, u, v):
        if u not in self.adj_list:
            self.adj_list[u] = []
        if v not in self.adj_list:
            self.adj_list[v] = []

        self.adj_list[u].append(v)
        self.adj_list[v].append(u)

    def __str__(self):
        result = ""
        for vertex, neighbors in self.adj_list.items():
            result += f"{vertex}: {', '.join(map(str, neighbors))}\n"
        return result

# Example usage:
graph = Graph()
graph.add_edge(0, 1)
graph.add_edge(1, 2)
graph.add_edge(2, 3)
print(graph)
