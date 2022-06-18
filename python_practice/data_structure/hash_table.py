""" 
    HashTable 
        insert - добавить элемент с ключом (индекс = хеш_функция(ключ)), 
        lookup - получить значение по ключу, delete - удалить значение по ключу),
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

class HashTable:
    def __init__(self):
        self.MAX = 30
        self.arr = DoublyLinkedList()
        for i in range(30):
            self.arr.append(DoublyLinkedList())

    def get_hash(self, key):
        h = 0
        for char in key:
            h += ord(char)
        return h % self.MAX

    def __setitem__(self, key, value):
        h = self.get_hash(key)
        found = False
        for idx, element in enumerate(self.arr[h]):
            if len(element) == 2 and element[0] == key:
                self.arr[h].delete_from_head(idx)
                self.arr[h].append((key, value))
                found = True
        if found == False:
            self.arr[h].append((key, value))

    def __getitem__(self, key):
        h = self.get_hash(key)
        for element in self.arr[h]:
            if element[0] == key:
                return element[1]

    def __delitem__(self, key):
        h = self.get_hash(key)
        for idx, element in enumerate(self.arr[h]):
            if element[0] == key:
                self.arr[h].delete_from_head(idx)

    def __str__(self):
        arr = ''
        for element in self.arr:
            arr += f'\n {element.__str__()}'
        return arr


t = HashTable()

for indx, number in enumerate(range(200)):
    t[str(indx)] = number

print(t.get_hash("key"))
t["key"] = 1130
t['yek'] = 200
print(t)

print(t["key"])
del t['key']
print(t["key"])
