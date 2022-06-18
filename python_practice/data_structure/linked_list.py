"""
    LinkedList 
        prepend - добавить в начало списка элемент, append - добавить в конец списка, 
        lookup - найти индекс элемента по значению (первого попавшегося), 
        insert - вставить элемент на конкретный индекс со сдвигом элементов направо, 
        delete - удалить элемент по индекс
"""

from random import randrange


class Node:
    def __init__(self, data=None, right=None):
        self.data = data
        self.right = right
        self.index_node = None


class LinkedList:
    def __init__(self):
        self.head = None

    def prepend(self, data):
        node = Node(data, self.head)
        self.head = node

    def append(self, data):
        if self.head is None:
            self.head = Node(data, None)
            return

        itr = self.head
        while itr.right:
            itr = itr.right

        itr.right = Node(data, None)

    def lookup(self, data):
        itr = self.head
        count = 0
        while itr:
            if itr.data == data:
                return count
            itr = itr.right
            count += 1


    def print(self):
        if self.head is None:
            print('Empty')
            return

        itr = self.head
        llst = ''
        while itr:
            llst += str(itr.data) + "-->"
            itr = itr.right
        print(llst)

    def get_length(self):
        count = 0
        itr = self.head
        while itr:
            count += 1
            itr = itr.right
        return count

    def delete(self, index):
        if index < 0 or index >= self.get_length():
            raise Exception('Invalid index')

        if index == 0:
            self.head = self.head.right
            return

        count = 0
        itr = self.head
        while itr:
            if count == index - 1:
                itr.right = itr.right.right
            itr = itr.right
            count += 1

    def insert(self, index, data):
        if index < 0 or index >= self.get_length():
            raise Exception('Invalid index')

        if index == 0:
            self.prepend(data)
            return
        count = 0
        itr = self.head
        while itr:
            if count == index - 1:
                node = Node(data, itr.right)
                itr.right = node
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

# linked_list = LinkedList()
# for i in range(10):
#     rand = randrange(10)
#     linked_list.append(rand)
#
# linked_list.insert(2, (2, 2))
# linked_list.print()
# print(linked_list.get_length())
# linked_list.delete(2)
# linked_list.print()
