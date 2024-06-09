#graph: {'A':['B','C'],'B':['E','A'],'C':['A', 'B', 'E','F'],'E':['B','C'],'F':['C']}
#A class taking list above representing a graph vertices as keys and their adjacent vertices as values
#The class creates adjacency matrix as a two-dimension array and an edge list of tuples   
class Graph:   
    def __init__(self,g):
        self.graph=dict()
        self.vertex_list=[]
        for key in g.keys():
            self.graph[key]=g[key]
            self.vertex_list.append(key)
        self.vertex_list.sort()
        cols=rows=len(self.vertex_list)
        self.adjacency_matrix=[[0 for x in range(rows)] for y in range(cols)]
        self.edge_list=[]
        for key in self.vertex_list:
            for neighbour in self.graph[key]:
                self.edge_list.append((key,neighbour))
        for edge in self.edge_list:
            index_of_first_vertex = self.vertex_list.index(edge[0])
            index_of_second_vertex = self.vertex_list.index(edge[1])
            self.adjacency_matrix[index_of_first_vertex][index_of_second_vertex ]=1
#=-=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=--=-=-=
grapha=Graph({'A':['B','C'],'B':['E','A'],'C':['A', 'B', 'E','F'],'E':['B','C'],'F':['C']})
print(grapha.adjacency_matrix)
print(grapha.edge_list)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
