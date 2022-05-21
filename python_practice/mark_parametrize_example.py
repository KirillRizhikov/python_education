# import math func
def func():
    pass


import pytest

@pytest.mark.parametrize('num1','num2', 'result',
                         [
                             (7,3,10),
                             (10.5, 25.5, 36)
                         ]
                         )
def test_add(num1, num2, result):
    assert math_func.add(num1, num2) == result



