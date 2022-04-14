'''Задание:
- попрактиковаться в создании классов с нуля и наследовании от других классов
- создать класс Transport, подумать об атрибутах и методах, дополнить класс ими
- подумать и реализовать классы-наследники класса Transport (минимум 4),
переопределить методы и атрибуты для каждого класса
- реализовать множественное наследование (поработать с MRO), создать еще
один класс-предок, например, Engine
- использовать абстрактные классы в своей иерархии, переопределить или реализовать
5 магических методов
(любые кроме __str__, __repr__, ___new__, __init__) - чем экзотичнее тем лучше
 (https://rszalski.github.io/magicmethods/#operators),
- использовать декораторы @staticmethod, @classmethod, @property'''

from abc import ABC, abstractclassmethod


class Transport(ABC):
    """Transport class"""

    def __init__(self, build_date, factory_number, mass, max_speed, average_speed):
        self.build_date = build_date
        self.factory_number = factory_number
        self.mass = mass
        self.max_speed = max_speed
        self.average_speed = average_speed
        self.distance = 0

    def get_trip_time(self, distance):
        '''Calculates trip time'''
        self.tr_time = distance / self.average_speed
        return f'Trip time is: {self.tr_time} h'

    def add_distance(self, distance):
        """Adds distance to total"""
        self.distance += distance
        self.move()
        print(f"{distance}km added \n Total distance: {self.distance}")

    def get_info(self):
        """Gets information about current transport"""
        return {
            "Factory number: ": self.factory_number,
            "Build date: ": self.build_date,
            "Mass: ": self.mass,
            "Max speed: ": self.max_speed
        }

    @staticmethod
    def trip_time_calc(speed, distance):
        """Calculates trip time"""
        tr_time = distance / speed
        return f'Trip time is: {tr_time} h'

    @abstractclassmethod
    def move(self):
        """Blueprint for a move function"""
        pass


class Engine():
    """Engine class"""

    def __init__(self, volume, power, consumption):
        self.__volume = volume
        self.power = power
        self.consumption = consumption

    def __eq__(self, other):
        return self.volume == other.volume and self.power == other.power

    def __gt__(self, other):
        return self.volume > other.volume and self.power > other.power

    def __add__(self, other):
        return Engine(self.volume + other.volume,
                      self.power + other.power,
                      self.consumption + other.consumption)

    def fuel_for_trip(self, distance):
        """Calculates fuel needed tor a trip"""
        return distance * self.consumption

    @property
    def volume(self):
        """Set volume"""
        return self.__volume

    @volume.setter
    def set_volume(self, value):
        if value < 0:
            raise ValueError('Volume cannot be negative.')
        self.__volume = value


class Car(Transport, Engine):
    """Car class"""

    def __init__(self, build_date, factory_number, mass, max_speed,
                 average_speed, volume, power, consumption, company):
        Transport.__init__(self, build_date, factory_number,
                           mass, max_speed, average_speed)
        Engine.__init__(self, volume, power, consumption)

        self.company = company

    def __str__(self):
        return self.company

    @classmethod
    def default_car(cls):
        """Returnes default instance of a Car"""
        return cls("00/00/00", 0000, 00, 00, 00, 00, 00, 00, "Tavria")

    def move(self):
        print('The car is moving')

    def get_info(self):
        return {
            "Factory number: ": self.factory_number,
            "Build date: ": self.build_date,
            "Mass: ": self.mass,
            "Max speed: ": self.max_speed,
            "Engine volume: ": self.volume,
            "Consumption: ": self.consumption,
            "Company: ": self.company
        }


class Bicycle(Transport):
    """Bycucle class"""
    def move(self):
        print('The bicylle is moving')


class Plane(Transport):
    """Plane class"""
    def move(self):
        print('The plain is flying')


class Ship(Transport):
    """Ship class"""
    def move(self):
        print('The ship is sailing')

"""Test"""
car = Car("10/20/20", 123456, 2, 200, 80, 1.5, 150, 10, "Tavria")
car.move()
print(car.get_info())
car.add_distance(20)
car1 = Car.default_car()
print(car1.get_info())

car1.set_volume = 10
print(car1)
print(Car.trip_time_calc(10, 10))