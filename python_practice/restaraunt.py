"""1. Изучить UML диаграмму классов - https://medium.com/@smagid_allThings/uml-class-diagrams-tutorial-step-by-step-520fd83b300b, https://medium.com/@smagid_allThings/uml-class-diagram-example-fab6197200e6
2. Создать UML диаграмму классов для ресторана с возможностью доставки еды
- подумать об основных классах, в основном их около 10 для данного задания, например, Customer, Order, Waiter etc.
- подумать о свойствах этих классов и действиях, которые они могут осуществлять
- отобразить это на диаграмме
- использовать принципы ООП на диаграмме(Инкапсуляция и Наследование)
- использовать модификаторы доступа и статические методы и поля
- добавить типы данных
3. Реализовать все классы в коде
Рекомендую использовать https://app.diagrams.net/ для построения UML диаграммы(там есть набор фигур для создания UML диаграмм)"""

class User():
    """Class User"""
    def __init__(self, userid, password):
        self.userid = userid
        self.password = password


class Customer(User):
    """Customer class"""
    def __init__(self, userid, password, adress):
        super().__init__(userid, password)
        self.adress = adress
        self.carts = {}
        print("user created")

    def create_cart(self, cartid):
        """Creates shopping cart"""
        self.carts[cartid] = ShoppingCart(cartid)

    def place_order(self, orderid, cartid, adress):
        """Creates order"""
        database.orders[orderid] = Order(orderid, cartid, adress)

    def add_cart_dish(self, cartid, dish, quantity):
        """Adds dish to the cart"""
        self.carts[cartid].add_cart_dish(dish, quantity)

    def del_cart_dish(self, cartid, dish):
        """Removes dish from the cart"""
        self.carts[cartid].del_cart_dish(dish)

    def get_cart(self, cartid):
        """Returns the cart"""
        print(f"Nere is your cart: \n {self.carts[cartid].cart}")
        return self.carts[cartid].cart


class Manager(User):
    """Manager class"""

    def approve_order(self, orderid):
        """Approves order"""
        database.orders[orderid].order_status = "Approved"

    def add_dish(self, menu, dish, price):
        """Adds dish to the menu"""
        menu.add_dish(dish, price)
        print("Manager adds dish to the menu")

    def del_dish(self, menu, dish):
        """Removes dish from the menu"""
        menu.del_dish(dish)
        print("Manager deletes dish from the menu")

    def send_to_kitchen(self, orderid):
        """Sends order to the kitchen"""
        kitchen.add_order(orderid)

    def creat_ship_info(self, orderid, shipping_cost):
        """Creates shipping information"""
        database.orders[orderid].creat_ship_info(shipping_cost)


class ShoppingCart():
    """Shopping Cart class"""
    def __init__(self, cartid):
        self.cartid = cartid
        self.cart = {}
        print('shopping cart created')

    def add_cart_dish(self, dish, quantity):
        """Adds dish to the cart"""
        print(f"{dish} has been added to cart")
        self.cart[dish] = quantity

    def del_cart_dish(self, dish):
        """Removes dish from the cart"""
        print(f"{dish} has been deleted from the cart")
        del self.cart[dish]

    def get_cart(self):
        """Returns Cart"""
        print(f"Nere is your cart: \n {self.cart}")
        return self.cart


class Menu():
    """Menu class"""
    def __init__(self):
        self.menu = {'chicken': 10, 'pizza': 20}

    def get_menu(self):
        """Returns menu"""
        print(f"Nere is menu: \n {self.menu}")
        return self.menu

    def add_dish(self, dish, price):
        """Adds dish"""
        print(f"{dish} added to the menu")
        self.menu[dish] = price

    def del_dish(self, dish):
        """Removes dish"""
        print(f"{dish} deletedfrom the menu")
        del self.menu[dish]


class Order():
    """Order class"""
    def __init__(self, orderid, cartid, adress):
        self.orderid = orderid
        self.cart = customer.carts[cartid].cart
        self.order_status = 'for approval'
        self.adress = adress
        print(f'creating an order {orderid}')

    def get_order(self):
        """Returns order"""
        print(
            f'Order number: {self.orderid}\nitems: {self.cart}\nstatus: {self.order_status}')

    def creat_ship_info(self, shipping_cost):
        """Creates shipping information"""
        self.shippinginfo = ShippingInfo(
            self.orderid, self.adress, shipping_cost)

    def get_shipping_info(self):
        """Returns shipping information"""
        print(
            f'Shipping information: \n{self.shippinginfo.shipping_information}')


class Delivery():
    """Delivery class"""
    def __init__(self, orderid, deliverer_id):

        self.orderid = orderid
        self.deliverer_id = deliverer_id
        self.status = 'not sent'
        self.shippinginformation = database.orders[orderid].shippinginfo.shipping_information

    def deliver(self):
        """Changes status to delivered"""
        self.status = 'Delivered'

    def get_status(self):
        """Returns current status"""
        print(f'Order ID: {self.orderid}\n Deliverer ID: {self.deliverer_id} \n  Shipping information: \n {self.shippinginformation} \n Status: {self.status}')


class ShippingInfo():
    """Shipping information class"""
    def __init__(self, orderid, adress, shipping_cost):
        self.orderid = orderid
        self.shipping_cost = shipping_cost
        self.adress = adress
        self.shipping_information = {
            "Order ID": self.orderid,
            "shipping adress": self.adress,
            "shipping_cost": self.shipping_cost}


class Kitchen():
    """Kitchen class"""
    def __init__(self, shift, chefs_number):
        self.chefs_number = chefs_number
        self.kitchen_orders = []

    def add_order(self, orderid):
        """Adds order"""
        print(f'kitchen gets order {orderid}')
        self.kitchen_orders.append(orderid)

    def order_ready(self, orderid):
        """Changes status to Ready"""
        self.kitchen_orders.remove(orderid)
        database.orders[orderid].order_status = "Ready"

    def get_utilization(self):
        """Returns utilization"""
        return self.chefs_number / len(self.kitchen_orders)


class Database:
    """Database class"""
    def __init__(self):
        self.orders = {}
        self.delivery = {}
        print('Database created')

    def get_orders(self):
        """Returns orders"""
        print(f'Orders: {self.orders}')

    def get_delivery(self):
        """Returns deliveries"""
        print(f'Delivery: {self.delivery}')


database = Database()

menu = Menu()
manager = Manager(222, 321)
manager.add_dish(menu, "pasta", 30)
manager.add_dish(menu, "beer", 10)
menu.get_menu()

customer = Customer(111, 123, "adress")
customer.create_cart(1)
customer.add_cart_dish(1, "pasta", 1)
customer.get_cart(1)
customer.place_order(1, 1, "Saltovka")

print(database.orders[1].get_order())

manager.approve_order(1)
print(database.orders[1].get_order())
kitchen = Kitchen(12, 10)
manager.send_to_kitchen(1)
print(kitchen.kitchen_orders)
kitchen.order_ready(1)
print(database.orders[1].get_order())
manager.creat_ship_info(1, 125)
database.orders[1].get_shipping_info()
delivery = Delivery(1, 123)
delivery.deliver()
delivery.get_status()
