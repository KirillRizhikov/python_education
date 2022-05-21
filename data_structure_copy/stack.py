"""
    Stack 
        push - добавить элемент в стек, 
        pop - изъять последний элемент, 
        peek - получить значение крайнего элемента стека
"""

# from doubly_linked_list import DoublyLinkedList
class LinkedListNode:
    def __init__(self, data=None, right=None, left=None):
        self.data = data
        self.right = right
        self.left = left
        self.index_node = None


class DoublyLinkedList:
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
        print("forward", llst)

        itr = self.tail
        llst = ''
        while itr:
            llst += str(itr.data) + "<--"
            itr = itr.left
        print("backward", llst)

    def get_length(self):
        count = 0
        itr = self.head
        while itr:
            count += 1
            itr = itr.right
        return count

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

    def __str__(self):
        if self.head is None:
            return '[]'

        itr = self.head
        llst = ''
        while itr:
            llst += str(itr.data) + "-->"
            itr = itr.right
        return llst

    def __repr__(self):
        return self.__str__()

class Stack:
    def __init__(self):
        self.buffer = DoublyLinkedList()
        self.index_node = None

    def push(self, val):
        self.buffer.prepend(val)

    def pop(self):
        pop = self.buffer.head.data
        self.buffer.delete_from_head(0)
        return pop

    def peek(self):
        return self.buffer.head.data

    def is_empty(self):
        return self.buffer.get_length()== 0

    def size(self):
        return self.buffer.get_length()

    def __getitem__(self, index):
        if index == 0:
            self.index_node = self.buffer.head
        if self.index_node is None:
            raise IndexError
        index_value = self.index_node.data
        self.index_node = self.index_node.right
        index += 1
        return index_value


q = Stack()
for char in "Kharkiv":
    q.push(char)
for i in q:
    print(i)
