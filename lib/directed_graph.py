class DirectedGraph:

    def __init__(self):
        self.__nodes = {}
        self.__edges = {}
        self.__reversed_edges = {}

    def add_node(self, key, value):
        if key in self.__nodes:
            raise AssertionError("Key {} duplicated".format(key))
        self.__nodes[key] = value
        if key not in self.__edges:
            self.__edges[key] = []
        if key not in self.__reversed_edges:
            self.__reversed_edges[key] = []

    def add_edge(self, origin, destination):
        if origin not in self.__nodes:
            raise AssertionError("Origin {} not found".format(origin))
        if destination not in self.__nodes:
            raise AssertionError("Destination {} not found".format(destination))

        origins = self.__reversed_edges[destination]
        destinations = self.__edges[origin]

        if destination in destinations:
            raise AssertionError("Origin {} already connected to destination {}".format(origin, destination))
        
        origins.append(origin)
        destinations.append(destination)

    def get_edge(self, node):
        if node not in self.__edges:
            raise KeyError("Key {} not in edges".format(node))
        return self.__edges[node]

    def get_reversed_edge(self, node):
        if node not in self.__reversed_edges:
            raise KeyError("Key {} not in reversed edges".format(node))
        return self.__reversed_edges[node]

    def get_length(self):
        return len(self.__nodes)

    # A graph is connected if after DFS from one node in its edges and reversed_edges reaches all nodes
    def is_connected(self):
        node_keys = list(self.__nodes.keys())

        if len(node_keys) == 0:
            raise AssertionError("There is no nodes in the graph")

        visited = { key : False for key in node_keys }
        
        queue = [node_keys[0]]
        visited[node_keys[0]] = True
        while len(queue) > 0:
            node_key = queue.pop(0)
            for node in self.__edges.get(node_key, []):
                if not visited[node]:
                    visited[node] = True
                    queue.append(node)

        queue = [node_keys[0]]
        while len(queue) > 0:
            node_key = queue.pop(0)
            for node in self.reversed_edges.get(node_key, []):
                if not visited[node]:
                    visited[node] = True
                    queue.append(node)
                    
        return all(visited.values())

    # A graph is connected if after DFS from one node in its edges and reversed_edges reaches all nodes and not checking the direction
    def is_connected_undirected(self):
        node_keys = list(self.__nodes.keys())

        if len(node_keys) == 0:
            raise AssertionError("There is no nodes in the graph")

        visited = { key : False for key in node_keys }
        
        queue = [node_keys[0]]
        visited[node_keys[0]] = True
        while len(queue) > 0:
            node_key = queue.pop(0)
            for node in self.__edges.get(node_key, []) + self.__reversed_edges.get(node_key, []):
                if not visited[node]:
                    visited[node] = True
                    queue.append(node)
                    
        return all(visited.values())

    # A graph has cycle if after DFS from any node it can return back to the same node it started, need to check all nodes
    def has_cycle(self):
        node_keys = list(self.__nodes.keys())

        for node_to_check in node_keys:
            queue = [node_to_check]
            visited = []
            while len(queue) > 0:
                node_key = queue.pop(0)
                for node in self.__edges.get(node_key, []):
                    visited.append(node)
                    if node_to_check in visited:
                        return True
                    else: 
                        queue.append(node)

    def draw(self):
        node_keys = sorted(list(self.__nodes.keys()))
        scale = 2 if len(node_keys) <= 9 else 1

        # Max length of al the node keys + 1
        cell_space = len(max(node_keys, key=lambda key : len(key))) + 1

        # Initialize matrix 
        matrix = [[" " * cell_space for x in range(len(node_keys) * scale)] for y in range(len(node_keys) * scale)]

        for idx, node_key in enumerate(node_keys):
            matrix[idx * scale][idx * scale] = node_key.rjust(cell_space)
        
        for origin, destinations in self.__edges.items():
            for destination in destinations:
                origin_index = node_keys.index(origin) * scale
                destination_index = node_keys.index(destination) * scale
                for i in range(abs(origin_index - destination_index)):
                    y_move = (i + 1) * (1 if origin_index < destination_index else -1)
                    if matrix[origin_index + y_move][origin_index] == " " * cell_space:
                        if i == abs(origin_index - destination_index) - 1:
                            matrix[origin_index + y_move][origin_index] = (">" if origin_index < destination_index else "<").center(cell_space)
                        else:
                            matrix[origin_index + y_move][origin_index] = "|".center(cell_space)
        
        matrix = list(map(lambda row : list("".join(row)), matrix))

        for line in matrix:
            fill_char = " "
            idx = 0
            while idx < len(line) and line[idx] in ['<', '|', '>', ' ']:
                if line[idx] == ">":
                    fill_char = ">"
                if line[idx] == " ":
                    line[idx] = fill_char
                idx += 1
            fill_char = " "
            idx = len(line) - 1
            while idx >= 0 and line[idx] in ['<', '|', '>', ' ']:
                if line[idx] == "<":
                    fill_char = "<"
                if line[idx] == " ":
                    line[idx] = fill_char
                idx -= 1

        return "\n".join(map(lambda row : "".join(row), matrix))

    def __str__(self):
        # 2 os Header + separator
        output_rows = max(len(self.__nodes), len(self.__edges), len(self.__reversed_edges)) + 2
        # First column Nodes, Second column Edges, Third column reversed_edges
        node_edges_data = [["" for x in range(3)] for y in range(output_rows)]
        node_edges_data[0][0] = "Nodes"
        node_edges_data[0][1] = "Edges"
        node_edges_data[0][2] = "Reverse Edges"

        for idx, key in enumerate(self.__nodes.keys()):
            node_edges_data[idx + 2][0] = key
        for idx, key in enumerate(self.__edges.keys()):
            node_edges_data[idx + 2][1] = "{}: {}".format(key, ", ".join(self.__edges[key]))
        for idx, key in enumerate(self.__reversed_edges.keys()):
            node_edges_data[idx + 2][2] = "{}: {}".format(key, ", ".join(self.__reversed_edges[key]))

        for x in range(3):
            max_length = 0
            for y in range(output_rows):
                if len(node_edges_data[y][x]) > max_length:
                    max_length = len(node_edges_data[y][x])
            for y in range(output_rows):
                node_edges_data[y][x] = node_edges_data[y][x].ljust(max_length)
            node_edges_data[1][x] = "-" * max_length

        result = [
            "",
            "Nodes: {}".format(len(self.__nodes)),
            "Edges: {}".format(sum(map(lambda key : len(self.__edges[key]), self.__edges)))
        ]
        result.append("")
        result = result + list(map(lambda line : "| {} |".format(" | ".join(line)), node_edges_data))
        result.append("")
        result.append(self.draw())

        return "\n".join(result)