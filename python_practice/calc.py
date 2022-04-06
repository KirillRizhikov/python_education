"""" Simple calculator program """


class Calculator:
    """" This class does the calculation """

    def __init__(self, x_arg, y_arg):
        """" __init__ function define arguments """
        self.x_arg = x_arg
        self.y_arg = y_arg

    def add(self):
        """"This function conducts addition """
        return self.x_arg + self.y_arg

    def subtract(self):
        """"This function conducts subtraction """
        return self.x_arg - self.y_arg

    def multiply(self):
        """"This function conducts multiplication """
        return self.x_arg * self.y_arg

    def divide(self):
        """"This function conducts division """
        if self.y_arg == 0:
            div = "Error division by zero"
        else:
            div = self.x_arg / self.y_arg
        return div


def check_number():
    """"This function processes input"""
    while True:
        try:
            arg = float(input())
            break
        except ValueError:
            print("Please enter a number")
    return arg


while True:
    # take first argument from user
    print("Please enter first number")
    arg1 = check_number()

    # take operation
    act = input('Enter operation + - * /')
    # check if input is a operator
    while act not in ('+', '-', '*', '/'):
        act = input("Please enter an operator")

    # take second argument from user
    print("Please enter second number")
    arg2 = check_number()

    calc = Calculator(arg1, arg2)

    if act == '+':
        print(calc.multiply())
    elif act == '-':
        print(calc.subtract())
    elif act == '*':
        print(calc.multiply())
    elif act == '/':
        print(calc.divide())

    # check if user wants another calculation
    next_calc = input('One more calc? (yes/no): ')
    if next_calc == "no":
        break
