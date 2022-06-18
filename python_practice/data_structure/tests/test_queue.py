"""
    Queue
        enqueue - добавить элемент в конец очереди,
        dequeue - изъять элемент из начала очереди,
        peek - получить значение элемента в начале очереди
"""
import pytest
from ..queue import *

class TestQueue():
    def setup(self):
        self.qe = Queue()
        for char in "Kharkiv":
            self.qe.enqueue(char)

    def test_TestQueue_enqueue(self):
        self.qe.enqueue("!")
        assert self.qe[7] == "!"

    def test_TestQueue_dequeue(self):
        self.qe.dequeue()
        assert self.qe.dequeue() == "h"

    def test_TestQueue_peek(self):
        assert self.qe.peek() == "K"