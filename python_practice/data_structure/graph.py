"""
    Graph ненаправленный, ребра без весов
        insert - добавить узел и связи с другими узлами по ссылкам,
        lookup - найти узел по значению и вернуть ссылку на него,
        delete - удалить узел по ссылке и связи с другими узлами).
"""

# from graph_linked_list import GraphLinkedList
class LinkedListNode:
    def __init__(self, data=None, right=None, left=None):
        self.data = data
        self.right = right
        self.left = left


class GraphLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.index_node = None

    def prepend(self, data):
        node = LinkedListNode(data, self.head, None)
        if self.head is not None:
            self.head.left = node
        else:
            self.tail = node
        self.head = node

    def append(self, data):
        node = LinkedListNode(data, None, self.tail)
        if self.head is None:
            self.head = node
            self.tail = node
            return

        self.tail.right = node
        self.tail = node

    def print(self):
        if self.head is None:
            print('Empty')
            return

        itr = self.head
        llst = ''
        while itr:
            llst += str(itr.data) + "-->"
            itr = itr.right

        return llst

    def get_length(self):
        count = 0
        itr = self.head
        while itr:
            count += 1
            itr = itr.right
        return count

    def replace(self, index, data):
        if index < 0 or index >= self.get_length():
            raise Exception('Invalid index')

        if index == 0:
            self.head.data = data
            return

        if index == self.get_length() - 1:
            self.tail.data = data
            return

        count = 0
        itr = self.head
        while itr:
            if count == index:
                itr.data = data
                return
            itr = itr.right
            count += 1

    def delete_from_head(self, index):
        if index < 0 or index >= self.get_length():
            raise Exception('Invalid index')

        if index == 0:
            self.head = self.head.right
            return

        if index == self.get_length() - 1:
            self.tail = self.tail.left
            return

        count = 0
        itr = self.head
        while itr:
            if count == index:
                itr.left.right = itr.right
                itr.right.left = itr.left
                return
            itr = itr.right
            count += 1

    def delete_by_name(self, name):
        if self.head.data == name:
            self.head = self.head.right
            return

        if self.tail.data == name:
            self.tail = self.tail.left
            return

        count = 0
        itr = self.head
        while itr:
            if itr.data == name:
                itr.left.right = itr.right
                itr.right.left = itr.left
                return
            itr = itr.right
            count += 1

    def delete_from_tail(self, index):
        if index < 0 or index >= self.get_length():
            raise Exception('Invalid index')

        if index == 0:
            self.head = self.head.right
            return

        if index == self.get_length():
            self.tail = self.tail.left
            return

        count = 0
        itr = self.tail
        while itr:
            if count == index:
                itr.left.right = itr.right
                itr.right.left = itr.left
            itr = itr.left
            count += 1

    def insert(self, index, data):
        if index < 0 or index >= self.get_length():
            raise Exception('Invalid index')

        if index == 0:
            self.prepend(data)
            return

        if index == self.get_length():
            self.append(data)
            return

        count = 0
        itr = self.head
        while itr:
            if count == index:
                node = LinkedListNode(data, itr, itr.left)
                itr.left.right = node
                itr.left = node
                break
            itr = itr.right
            count += 1

    def __getitem__(self, index):
        if index > self.get_length()-1:
            raise IndexError
        self.index_node = self.head
        for idx in range(index):
            self.index_node = self.index_node.right
        return self.index_node.data

    # def __str__(self):
    #     if self.head is None:
    #         return '[]'

    #     itr = self.head
    #     llst = ''
    #     while itr:
    #         llst += str(itr.data) + "-->"
    #         itr = itr.right
    #     return llst

    # def __repr__(self):
    #     return self.__str__()

class GraphNode:
    """
    Contains graph node that represents GraphLinkedList
    First item in list is a name of the node, all other are edges
    """

    def __init__(self, data=" "):
        self.node = GraphLinkedList()

        for edge in data:
            self.node.append(edge)

    def __str__(self):
        arr = ''
        for element in self.node:
            arr += f'{element.__str__()}, '
        return arr

    def get_length(self):
        return self.node.get_length()

    def __getitem__(self, index):
        return self.node.__getitem__(index)


class Graph:
    """ Graph class"""

    def __init__(self):
        self.graph = GraphLinkedList()
        self.graph.append(GraphNode().node)
        self.index_node = None

    def insert(self, data):
        """ Inserts node"""
        node = GraphNode(data).node
        self.graph.append(node)
        self.add_links()

    def lookup(self, name):
        """ Searches for a node and returns link"""
        for node in self.graph:
            if node[0] == name:
                return node
        return False

    def find_link(self, key):
        """Searches for the link"""
        for node in self.graph:
            if key == node.head.data:
                return node
        return key

    def add_links(self):
        """Replaces edges with links"""
        for itr, node in enumerate(self.graph):
            if node.get_length() >= 2:
                for idx, edge in enumerate(node):
                    if idx != 0:
                        self.graph[itr].replace(idx, self.find_link(edge))

    def delete(self, name):
        """Deletes node"""
        link = self.lookup(name)
        if link:
            for node in self.graph:
                node.delete_by_name(link)
            self.graph.delete_by_name(link)

    def __getitem__(self, index):
        if index > self.graph.get_length()-1:
            raise IndexError
        self.index_node = self.graph.head
        for idx in range(index):
            self.index_node = self.index_node.right
        return self.index_node.data

    def __str__(self):
        arr = ''
        for element in self.graph:
            arr += f'\n {element}'
        return arr


gr = Graph()
gr.insert(["A", "B", "C"])
gr.insert(["B", "A", "C"])
gr.insert(["C", "A", "B"])
gr.insert(["D", "A", "B"])

print(gr)
print(f'\nlink {gr.lookup("A")}')
gr.delete("A")
print(f'link {gr.lookup("A")}')
print(gr[2].print())

print(gr[1][1].print())
print(f'link {gr.lookup("B")}')
gr.delete("B")
print(f'link {gr.lookup("B")}')