"""
Binary Search Tree
        insert - добавить элемент,
        lookup - найти элемент по значению и вернуть ссылку на него (узел),
        delete - удалить элемент по значению),
"""
import pytest
from ..binary_tree import *


class TestBinaryTree:
    def setup(self):
        self.tree = BinaryTree()
        for i in range(10):
            rand = randrange(10)
            self.tree.isert(rand)

    def test_TestBinaryTree_insert(self):
        self.tree.isert(9)
        assert self.tree.lookup(9).data == 9

    def test_TestBinaryTree_lookup(self):
        for i in range(10):
            rand = randrange(10)
            self.tree.isert(rand)
            assert self.tree.lookup(rand).data == rand

    def test_TestBinaryTree_delete(self):
        for i in range(10):
            rand = randrange(10)
            self.tree.isert(rand)
            self.tree.delete(rand)
            assert self.tree.lookup(rand) == False
