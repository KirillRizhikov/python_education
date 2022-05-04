"""
Реализовать на Python:
- Binary search
- Quick sort (Iterative)
- Recursive factorial implementation
Покрытие реализаций тестами - обязательно.
"""
from random import randrange
import sys
import math
import pytest

sys.path.insert(0, '/home/kirill/PycharmProjects/python_education/python_practice/')

from ..algorithms import *


def rand_arr(length, ranrange):
    """"Generates random array"""
    rand_arr = []
    for i in range(length):
        rand = randrange(ranrange)
        if rand not in rand_arr:
            rand_arr.append(rand)
    return rand_arr


def binary_test_arr():
    """Prepares input data for binary search function"""
    test_arr = []
    for i in range(100):
        arr = rand_arr(100, 10000)
        arr.sort()
        rand_num = arr[randrange(len(arr))]
        rand_idx = arr.index(rand_num)
        test_arr.append((arr, rand_num, rand_idx))
    return test_arr


def sort_test_arr():
    """Prepares input data for the sort function"""
    test_arr = []
    for i in range(100):
        arr = rand_arr(100, 10000)
        sorted_arr = arr
        sorted_arr.sort()
        test_arr.append((arr, sorted_arr))
    return test_arr


@pytest.mark.parametrize('arr, num, idx', binary_test_arr())
def test_binary_search(arr, num, idx):
    """Binary search test"""
    assert binary_search(arr, num) == idx


@pytest.mark.parametrize('arr, sorted_arr', sort_test_arr())
def test_selection_sort(arr, sorted_arr):
    """Binary search test"""
    quick_sort(arr)
    assert arr == sorted_arr


def test_factorial():
    """Factorial test"""
    for i in range(20):
        rand = randrange(100)
        assert factorial(rand) == math.factorial(rand)

        with pytest.raises(ValueError):
            factorial(-1)
