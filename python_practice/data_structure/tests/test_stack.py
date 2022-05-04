"""
    Stack
        push - добавить элемент в стек,
        pop - изъять последний элемент,
        peek - получить значение крайнего элемента стека
"""
import pytest
from ..stack import *


class TestStack:
    def setup(self):
        self.st = Stack()
        for char in "Kharkiv":
            self.st.push(char)

    def test_TestStack_push(self):
        self.st.push("!")
        assert self.st[0] == "!"

    def test_TestStack_pop(self):
        assert self.st.pop() == "v"

    def test_TestStack_peek(self):
        assert self.st.peek() == "v"
