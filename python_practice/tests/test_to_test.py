# import unittest
import pytest
# import pytest_freezegun
import sys
import datetime

sys.path.insert(1, '/home/kirill/PycharmProjects/python_education/python_practice/')
from ..to_test import *


def test_even_odd():
    """Even-odd test"""
    assert even_odd(2) == 'even'
    assert even_odd(1) == 'odd'


def test_sum_all():
    """Summation test"""
    assert sum_all(4, 5, 6) == 15
    assert sum_all(-5, 20, 40, 60) == 115


@pytest.fixture
def current_hour():
    """Time fixture"""
    return datetime.now()


@pytest.mark.freeze_time
def test_time_of_day(current_hour, freezer):
    """Test time of the day"""
    freezer.move_to('2022-04-27 13:47:43.880088')
    assert time_of_day() == "afternoon"
    freezer.move_to('2022-04-27 9:47:43.880088')
    assert time_of_day() == "morning"
    freezer.move_to('2022-04-27 23:47:43.880088')
    assert time_of_day() == "night"
    freezer.move_to('2022-04-27 3:47:43.880088')
    assert time_of_day() == "night"


class TestProduct():
    """Class Product test"""
    def setup(self):
        self.product = Product("Car", 1000, 3)

    def test_Product_subtract_quantity(self):
        """ Test subsrtact quantity"""
        self.product.subtract_quantity()
        assert self.product.quantity == 2

    def test_Product_add_quantity(self):
        """Test product add quantity"""
        self.product.add_quantity()
        assert self.product.quantity == 4

    def test_Product_change_price(self):
        """Test product change price"""
        self.product.change_price(2000)
        assert self.product.price == 2000


class TestShop:
    """Test Shop class"""

    def setup(self):
        self.shop = Shop()
        self.shop.add_product(Product('bike', 10, 3))
        self.shop.add_product(Product('plane', 20, 20))

    def test_TestShop_add_product(self):
        """Test add product"""
        self.shop.add_product(Product('car', 20))
        assert self.shop._get_product_index('car') == 2

    def test_TestShop_get_product_index(self):
        """Test get product index"""
        assert self.shop._get_product_index('plane') == 1
        assert self.shop._get_product_index('bike') == 0

    def test_TestShop_sell_product(self):
        """Test sell product"""
        assert self.shop.sell_product('bike', 2) == 20
        assert self.shop.sell_product('plane', 3) == 60

        with pytest.raises(ValueError):
            self.shop.sell_product('bike', 200)
