"""
    Graph ненаправленный, ребра без весов
        insert - добавить узел и связи с другими узлами по ссылкам,
        lookup - найти узел по значению и вернуть ссылку на него,
        delete - удалить узел по ссылке и связи с другими узлами).
"""

import pytest
from ..graph import *

class TestGraph:
    def setup(self):
        pass


    def test_TestGraph_insert(self):
        self.gr = Graph()
        self.gr.insert(["A", "B", "C"])
        self.gr.insert(["B", "A", "C"])
        self.gr.insert(["C", "A", "B"])
        self.gr.insert(["D", "A", "B"])
        assert self.gr[1][0] == "A"

    def test_TestGraph_lookup(self):
        self.gr = Graph()
        self.gr.insert(["A", "B", "C"])
        self.gr.insert(["B", "A", "C"])
        self.gr.insert(["C", "A", "B"])
        self.gr.insert(["D", "A", "B"])
        assert self.gr.lookup("C") == self.gr[3]

    def test_TestGraph_delete(self):
        self.gr = Graph()
        self.gr.insert(["A", "B", "C"])
        self.gr.insert(["B", "A", "C"])
        self.gr.insert(["C", "A", "B"])
        self.gr.insert(["D", "A", "B"])
        self.gr.delete("A")
        assert self.gr.lookup("A") == False
        self.gr.delete("B")
        assert self.gr.lookup("B") == False