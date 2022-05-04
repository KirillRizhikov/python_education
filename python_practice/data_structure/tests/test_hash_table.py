"""
    HashTable
        insert - добавить элемент с ключом (индекс = хеш_функция(ключ)),
        lookup - получить значение по ключу, delete - удалить значение по ключу),
"""
import pytest
from ..hash_table import *


class TestHashTable:
    def setup(self):
        self.ht = HashTable()

    def test_TestStack_insert_lookup(self):
        self.ht["key"] = 1130
        self.ht['yek'] = 200

        assert self.ht['key'] == 1130

