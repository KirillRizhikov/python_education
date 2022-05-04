import pytest
from ..linked_list import *

class TestLinkedList():
    def setup(self):
        self.list = LinkedList()

    def test_LinkedList_prepend(self):
        for i in range(10):
            self.list.prepend(i)
        assert self.list.head.data == 9

    def test_LinkedList_append(self):
        for i in range(10):
            self.list.append(i)
        assert self.list[9] == 9

    def test_LinkedList_lookup(self):
        for i in range(10):
            self.list.append(i)
        assert self.list.lookup(8) == 8

    def test_LinkedList_delete(self):
        for i in range(10):
            self.list.append(i)
        self.list.delete(8)
        assert self.list[8] != 8

